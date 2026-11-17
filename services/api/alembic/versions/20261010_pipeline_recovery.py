"""Add durable image pipeline lease and bounded retry metadata.

Revision ID: 20261010_pipeline_recovery
Revises: 20261009_camera_service_tasks
Create Date: 2026-10-10
"""
from alembic import op
import sqlalchemy as sa


revision = '20261010_pipeline_recovery'
down_revision = '20261009_camera_service_tasks'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'images',
        sa.Column('pipeline_updated_at', sa.DateTime(timezone=True),
                  server_default=sa.func.now(), nullable=False),
    )
    op.add_column(
        'images',
        sa.Column('pipeline_attempts', sa.Integer(), server_default='0', nullable=False),
    )
    op.add_column('images', sa.Column('pipeline_error', sa.Text(), nullable=True))
    op.add_column('images', sa.Column('pipeline_failed_stage', sa.String(length=20), nullable=True))
    op.add_column('images', sa.Column('pipeline_claim_id', sa.String(length=36), nullable=True))
    op.create_index('ix_images_pipeline_updated_at', 'images', ['pipeline_updated_at'])
    op.add_column(
        'bulk_upload_jobs',
        sa.Column('pipeline_updated_at', sa.DateTime(timezone=True),
                  server_default=sa.func.now(), nullable=False),
    )
    op.add_column('bulk_upload_jobs', sa.Column('pipeline_claim_id', sa.String(length=36), nullable=True))
    op.add_column(
        'bulk_upload_jobs',
        sa.Column('pipeline_attempts', sa.Integer(), server_default='0', nullable=False),
    )
    op.add_column('bulk_upload_jobs', sa.Column('pipeline_error', sa.Text(), nullable=True))
    op.add_column(
        'bulk_upload_jobs',
        sa.Column('staging_complete', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    )
    op.create_index(
        'ix_bulk_upload_jobs_pipeline_updated_at', 'bulk_upload_jobs', ['pipeline_updated_at']
    )


def downgrade() -> None:
    op.drop_index('ix_bulk_upload_jobs_pipeline_updated_at', table_name='bulk_upload_jobs')
    op.drop_column('bulk_upload_jobs', 'staging_complete')
    op.drop_column('bulk_upload_jobs', 'pipeline_error')
    op.drop_column('bulk_upload_jobs', 'pipeline_attempts')
    op.drop_column('bulk_upload_jobs', 'pipeline_claim_id')
    op.drop_column('bulk_upload_jobs', 'pipeline_updated_at')
    op.drop_index('ix_images_pipeline_updated_at', table_name='images')
    op.drop_column('images', 'pipeline_error')
    op.drop_column('images', 'pipeline_failed_stage')
    op.drop_column('images', 'pipeline_claim_id')
    op.drop_column('images', 'pipeline_attempts')
    op.drop_column('images', 'pipeline_updated_at')
