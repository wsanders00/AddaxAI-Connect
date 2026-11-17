"""Join the retained archive schema to the stock v0.16 migration path."""
from alembic import op

revision = '20261014_preserve_archive_merge'
down_revision = ('20261011_archive_intake', '20261013_camera_retired_at')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
