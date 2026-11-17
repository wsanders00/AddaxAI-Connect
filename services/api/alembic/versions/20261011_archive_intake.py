"""Add immutable archive intake target and idempotency data to bulk jobs.

Revision ID: 20261011_archive_intake
Revises: 20261010_pipeline_recovery
Create Date: 2026-10-10
"""
from alembic import op
import sqlalchemy as sa


revision = "20261011_archive_intake"
down_revision = "20261010_pipeline_recovery"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("bulk_upload_jobs", sa.Column("deployment_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "fk_bulk_upload_jobs_deployment_id_deployments", "bulk_upload_jobs", "deployments",
        ["deployment_id"], ["id"], ondelete="RESTRICT",
    )
    op.create_index("ix_bulk_upload_jobs_deployment_id", "bulk_upload_jobs", ["deployment_id"])
    op.add_column("bulk_upload_jobs", sa.Column("client_batch_id", sa.String(length=200), nullable=True))
    op.add_column("bulk_upload_jobs", sa.Column("request_fingerprint", sa.String(length=64), nullable=True))
    op.add_column("bulk_upload_jobs", sa.Column("archive_manifest", sa.JSON(), nullable=True))
    op.create_unique_constraint(
        "uq_bulk_job_project_client_batch", "bulk_upload_jobs", ["project_id", "client_batch_id"]
    )


def downgrade():
    op.drop_constraint("uq_bulk_job_project_client_batch", "bulk_upload_jobs", type_="unique")
    op.drop_column("bulk_upload_jobs", "archive_manifest")
    op.drop_column("bulk_upload_jobs", "request_fingerprint")
    op.drop_column("bulk_upload_jobs", "client_batch_id")
    op.drop_index("ix_bulk_upload_jobs_deployment_id", table_name="bulk_upload_jobs")
    op.drop_constraint("fk_bulk_upload_jobs_deployment_id_deployments", "bulk_upload_jobs", type_="foreignkey")
    op.drop_column("bulk_upload_jobs", "deployment_id")
