"""
Database operations for SpeciesNet classification worker

Handles inserting classifications and updating image status.
"""
from sqlalchemy.orm import Session
from typing import List

from shared.database import get_db_session
from shared.models import Image, Detection, Classification as ClassificationModel, Camera, Project, TaxonomyMapping, ServerSettings
from shared.logger import get_logger
from shared.pipeline_recovery import set_pipeline_status
from classifier import Classification, DetectionInfo

logger = get_logger("classification-speciesnet.db_operations")


def get_taxonomy_mapping() -> dict[str, str]:
    """
    Fetch all taxonomy mapping rows and return as {latin: common} dict.

    Returns:
        Dict mapping lowercase latin names to common names.
        Empty dict if no mapping is configured.
    """
    try:
        with get_db_session() as db:
            rows = db.query(TaxonomyMapping).all()
            return {row.latin: row.common for row in rows}
    except Exception as e:
        logger.error("Failed to fetch taxonomy mapping", error=str(e), exc_info=True)
        raise


def get_geofencing_config() -> dict[str, str]:
    """
    Fetch country code and admin1 region from server settings.

    Returns:
        Dict with 'country_code' and 'admin1_region' keys.
        Empty dict if not configured.
    """
    try:
        with get_db_session() as db:
            settings = db.query(ServerSettings).first()
            if settings and settings.speciesnet_country_code:
                return {
                    "country_code": settings.speciesnet_country_code,
                    "admin1_region": settings.speciesnet_admin1_region or "",
                }
            return {}
    except Exception as e:
        logger.error("Failed to fetch geofencing config", error=str(e), exc_info=True)
        raise


def get_detections_for_image(image_uuid: str) -> tuple[int, int, int, List[DetectionInfo], List[str] | None]:
    """
    Fetch detection records and project configuration for an image.

    Args:
        image_uuid: UUID of image

    Returns:
        Tuple of (image_id, image_width, image_height, list of DetectionInfo objects, included_species)
        included_species is None if all species are allowed, or a list of species names to include

    Raises:
        Exception: If query fails
    """
    logger.info("Fetching detections for image", image_uuid=image_uuid)

    try:
        with get_db_session() as db:
            # Get image record
            image = db.query(Image).filter(Image.uuid == image_uuid).with_for_update().first()

            if not image:
                raise ValueError(f"Image not found: {image_uuid}")

            # Get project's included species via camera
            included_species = None  # None = all species allowed
            camera = db.query(Camera).filter(Camera.id == image.camera_id).first()
            if camera and camera.project_id:
                project = db.query(Project).filter(Project.id == camera.project_id).first()
                if project and project.included_species:
                    included_species = project.included_species

            # Get image dimensions from metadata
            image_metadata = image.image_metadata or {}
            image_width = image_metadata.get('width')
            image_height = image_metadata.get('height')

            if not image_width or not image_height:
                raise ValueError(f"Image dimensions not found in metadata: {image_uuid}")

            # Get all detections for this image
            detections = db.query(Detection).filter(Detection.image_id == image.id).all()

            detection_infos = []
            for det in detections:
                bbox_normalized = det.bbox.get('normalized', [])
                if not bbox_normalized:
                    logger.warning(
                        "Detection missing normalized bbox, skipping",
                        detection_id=det.id
                    )
                    continue

                detection_info = DetectionInfo(
                    detection_id=det.id,
                    category=det.category or "unknown",
                    confidence=det.confidence,
                    bbox_normalized=bbox_normalized,
                    image_width=image_width,
                    image_height=image_height
                )
                detection_infos.append(detection_info)

            logger.info(
                "Detections fetched",
                image_uuid=image_uuid,
                num_detections=len(detection_infos),
                included_species_count=len(included_species) if included_species else 0,
                filter_mode="included" if included_species else "all"
            )

            return (image.id, image_width, image_height, detection_infos, included_species)

    except Exception as e:
        logger.error(
            "Failed to fetch detections",
            image_uuid=image_uuid,
            error=str(e),
            exc_info=True
        )
        raise


def insert_classifications(
    classifications: List[Classification], image_uuid: str, claim_id: str
) -> List[int]:
    """
    Insert classification records into database.

    Populates raw_prediction and raw_confidence when available (SpeciesNet).

    Args:
        classifications: List of Classification objects

    Returns:
        List of classification IDs

    Raises:
        Exception: If insert fails
    """
    if not classifications:
        logger.info("No classifications to insert")
        return []

    logger.info(
        "Inserting classifications",
        num_classifications=len(classifications)
    )

    try:
        with get_db_session() as db:
            image = db.query(Image).filter(Image.uuid == image_uuid).with_for_update().first()
            if not image or image.pipeline_claim_id != claim_id or image.status != "classifying":
                raise RuntimeError("Classification claim was lost before database write")
            classification_ids = []
            detection_ids = {item.detection_id for item in classifications}
            existing = {
                row[0] for row in db.query(ClassificationModel.detection_id)
                .filter(ClassificationModel.detection_id.in_(detection_ids)).all()
            } if detection_ids else set()

            for classification in classifications:
                # Delivery may repeat after a prior DB commit. Keep the
                # durable classification and any manual verification intact.
                if classification.detection_id in existing:
                    continue
                classification_record = ClassificationModel(
                    detection_id=classification.detection_id,
                    species=classification.species,
                    confidence=classification.confidence
                )

                # Set raw fields when available (SpeciesNet provides these)
                if hasattr(classification, 'raw_prediction') and classification.raw_prediction is not None:
                    classification_record.raw_prediction = classification.raw_prediction
                if hasattr(classification, 'raw_confidence') and classification.raw_confidence is not None:
                    classification_record.raw_confidence = classification.raw_confidence

                db.add(classification_record)
                db.flush()  # Get ID without committing

                classification_ids.append(classification_record.id)
                existing.add(classification.detection_id)

            db.commit()

            logger.info(
                "Classifications inserted",
                classification_ids=classification_ids
            )

            return classification_ids

    except Exception as e:
        logger.error(
            "Classification insert failed",
            error=str(e),
            exc_info=True
        )
        raise


def update_image_status(image_uuid: str, status: str, claim_id: str) -> None:
    """
    Update image processing status.

    Args:
        image_uuid: UUID of image
        status: New status ('classified', 'failed')

    Raises:
        Exception: If update fails
    """
    logger.info("Updating image status", image_uuid=image_uuid, status=status)

    try:
        set_pipeline_status(image_uuid, status, claim_id=claim_id)
        logger.info("Image status updated", image_uuid=image_uuid, status=status)

    except Exception as e:
        logger.error(
            "Image status update failed",
            image_uuid=image_uuid,
            status=status,
            error=str(e),
            exc_info=True
        )
        raise
