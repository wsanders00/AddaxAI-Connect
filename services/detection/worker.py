"""
Detection Worker

Consumes images from the ingestion queue, runs object detection, and produces crops.
"""
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

from shared.bulk_jobs import is_bulk_image_cancelled
from shared.logger import get_logger, set_image_id
from shared.pipeline_recovery import (
    claim_image_stage,
    fail_pipeline_stage,
    maintain_image_lease,
    reconcile_stale_images,
)
from shared.queue import (
    RedisQueue,
    QUEUE_IMAGE_INGESTED,
    QUEUE_IMAGE_INGESTED_BULK,
    QUEUE_DETECTION_COMPLETE,
    QUEUE_DETECTION_COMPLETE_BULK,
    HEARTBEAT_KEY_DETECTION,
    DEVICE_KEY_DETECTION,
)
from config import get_settings
from model_loader import load_model
import torch
from shared.device import select_device
from detector import run_detection
from storage_operations import download_image_from_minio
from db_operations import update_image_status, insert_detections

# Enable PIL to load truncated images from camera traps
from PIL import ImageFile
ImageFile.LOAD_TRUNCATED_IMAGES = True

logger = get_logger("detection")
settings = get_settings()


def process_image(message: dict, detector) -> None:
    """
    Process image through detection pipeline.

    Args:
        message: Queue message with image metadata
        detector: Loaded MegaDetector model

    Raises:
        Exception: If processing fails (crashes worker)
    """
    image_uuid = message.get("image_uuid")
    storage_path = message.get("storage_path")
    camera_id = message.get("camera_id")
    origin = message.get("origin", "live")
    # Route the next stage to the matching priority queue. Bulk-origin
    # detections continue down the bulk pipeline so live cameras keep
    # priority on the classifier.
    downstream_queue_name = (
        QUEUE_DETECTION_COMPLETE_BULK if origin == "bulk" else QUEUE_DETECTION_COMPLETE
    )

    if not image_uuid or not storage_path:
        raise ValueError(f"Invalid message format: {message}")

    # Set correlation ID for logging
    set_image_id(image_uuid)

    # Cooperative stop: skip images of a cancelled bulk job before any download
    # or detection. Only bulk-origin images can belong to a job, so the live
    # pipeline never pays for this check.
    if origin == "bulk" and is_bulk_image_cancelled(image_uuid):
        logger.info("Skipping cancelled bulk image", image_uuid=image_uuid)
        return

    # A repeated queue delivery after the detection commit only has to
    # reconstruct the downstream message. Do not run inference twice.
    from shared.database import get_db_session
    from shared.models import Image, Detection as DetectionModel
    persisted_detection_ids = []
    with get_db_session() as db:
        image = db.query(Image).filter(Image.uuid == image_uuid).first()
        if not image:
            raise ValueError(f"Image not found: {image_uuid}")
        persisted_detection_ids = [
            d.id for d in db.query(DetectionModel).filter(DetectionModel.image_id == image.id).all()
        ]
        if image.status in ("detected", "classifying", "classified"):
            queue = RedisQueue(downstream_queue_name)
            queue.publish({"image_uuid": image_uuid, "num_detections": len(persisted_detection_ids),
                           "detection_ids": persisted_detection_ids, "origin": origin})
            return

    claim_id = claim_image_stage(image_uuid, "pending", "processing")
    if not claim_id:
        logger.info("Detection delivery already claimed or terminal", image_uuid=image_uuid)
        return

    logger.info(
        "Processing image",
        image_uuid=image_uuid,
        storage_path=storage_path,
        camera_id=camera_id,
        origin=origin,
    )

    temp_files = []

    try:
        # Claim is durable before any expensive work; refresh while inference
        # runs so a second worker cannot mistake a healthy lease as abandoned.
        lease = maintain_image_lease(image_uuid, "processing", claim_id)
        lease.__enter__()

        # A stale lease may have been recovered after detections committed but
        # before the status transition. Reuse those rows instead of inferring
        # and inserting a second set.
        if persisted_detection_ids:
            update_image_status(image_uuid, "detected", claim_id)
            RedisQueue(downstream_queue_name).publish({
                "image_uuid": image_uuid,
                "num_detections": len(persisted_detection_ids),
                "detection_ids": persisted_detection_ids,
                "origin": origin,
            })
            return

        # Step 2: Download image from MinIO
        image_path = download_image_from_minio(storage_path)
        temp_files.append(image_path)

        # Step 3: Run detection
        detections = run_detection(detector, image_path)

        logger.info(
            "Detections found",
            image_uuid=image_uuid,
            num_detections=len(detections)
        )

        # If no detections, update status and publish message
        if len(detections) == 0:
            logger.info("No detections found", image_uuid=image_uuid)
            update_image_status(image_uuid, "detected", claim_id)

            # Publish to next queue (classification will handle empty detections)
            queue = RedisQueue(downstream_queue_name)
            queue.publish({
                "image_uuid": image_uuid,
                "num_detections": 0,
                "detection_ids": [],
                "origin": origin,
            })

            logger.info("Image processing complete (no detections)", image_uuid=image_uuid)
            return

        # Step 4: Insert detections into database
        detection_ids = insert_detections(image_uuid, detections, claim_id)

        # Step 5: Update image status to detected
        update_image_status(image_uuid, "detected", claim_id)

        # Step 6: Publish to detection-complete queue
        queue = RedisQueue(downstream_queue_name)
        queue.publish({
            "image_uuid": image_uuid,
            "num_detections": len(detections),
            "detection_ids": detection_ids,
            "origin": origin,
        })

        logger.info(
            "Image processing complete",
            image_uuid=image_uuid,
            num_detections=len(detections),
            detection_ids=detection_ids
        )

    except Exception as e:
        # Update status to failed
        try:
            fail_pipeline_stage(image_uuid, "processing", "detection", claim_id, e)
        except Exception as db_error:
            logger.error("Failed to update status to failed", error=str(db_error))

        logger.error(
            "Image processing failed",
            image_uuid=image_uuid,
            error=str(e),
            exc_info=True
        )
        raise

    finally:
        if 'lease' in locals():
            lease.__exit__(None, None, None)
        # Cleanup temporary files
        for temp_file in temp_files:
            try:
                os.unlink(temp_file)
            except Exception:
                pass


def main():
    """Main entry point for detection worker"""
    logger.info("Detection worker starting", log_level=settings.log_level)

    # Decide the device before the model download, so a GPU server that
    # cannot see its card fails in a second instead of after 280 MB.
    device = select_device(settings.use_gpu, torch.cuda.is_available())
    logger.info("Loading MegaDetector model", device=device)
    detector = load_model(device)
    logger.info("Model loaded successfully")

    # Initialize queue consumer. Live wins over bulk via priority BRPOP.
    queue = RedisQueue(QUEUE_IMAGE_INGESTED)
    queue.record_device(DEVICE_KEY_DETECTION, device)
    priority_queues = [QUEUE_IMAGE_INGESTED, QUEUE_IMAGE_INGESTED_BULK]
    logger.info("Listening for messages", queues=priority_queues)
    queue.consume_forever_priority(
        priority_queues,
        lambda msg: process_image(msg, detector),
        heartbeat_key=HEARTBEAT_KEY_DETECTION,
        maintenance_callback=lambda: reconcile_stale_images("detection"),
    )


if __name__ == "__main__":
    main()
