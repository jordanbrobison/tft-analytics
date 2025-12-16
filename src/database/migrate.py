import logging
import os
from pathlib import Path
from sqlalchemy import text

from src.database.connection import get_db_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_migration():
    """Run database migration to create initial schema."""

    logger.info("Starting database migration...")

    # Read schema SQL file
    schema_file = Path(__file__).parent / "schema.sql"

    if not schema_file.exists():
        logger.error(f"Schema file not found: {schema_file}")
        return False

    with open(schema_file, 'r') as f:
        schema_sql = f.read()

    # Execute schema SQL
    try:
        engine = get_db_engine()
        with engine.begin() as conn:
            # Split by statement and execute each
            statements = [s.strip() for s in schema_sql.split(';') if s.strip()]

            for i, statement in enumerate(statements, 1):
                logger.info(f"Executing statement {i}/{len(statements)}...")
                conn.execute(text(statement))

        logger.info("✅ Database migration completed successfully!")
        return True

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def verify_tables():
    """Verify that all tables were created."""

    expected_tables = [
        'raw_players',
        'raw_matches',
        'player_match_history',
        'data_collection_log'
    ]

    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name
            """))

            tables = [row[0] for row in result]

            logger.info(f"\nTables found in database:")
            for table in tables:
                status = "✅" if table in expected_tables else "⚠️"
                logger.info(f"  {status} {table}")

            missing = set(expected_tables) - set(tables)
            if missing:
                logger.warning(f"\nMissing tables: {missing}")
                return False

            logger.info(f"\nAll expected tables created successfully!")
            return True

    except Exception as e:
        logger.error(f"Verification failed: {e}")
        return False


if __name__ == "__main__":
    print("=" * 80)
    print("TFT Data Pipeline - Database Migration")
    print("=" * 80)

    # Run migration
    if run_migration():
        print()
        verify_tables()
    else:
        print("\nMigration failed. Check logs above.")
