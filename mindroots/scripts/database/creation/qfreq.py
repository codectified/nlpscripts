import os
import logging
import time
from datetime import datetime
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Configure logging
log_filename = f"qfreq_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables from .env
load_dotenv()

# Get Neo4j credentials
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASS")

# Validate credentials
if not all([uri, user, password]):
    logger.error("Missing Neo4j connection details. Set NEO4J_URI, NEO4J_USER, and NEO4J_PASS in your .env file.")
    raise ValueError("Missing Neo4j connection details.")

logger.info("Connecting to Neo4j...")
driver = GraphDatabase.driver(uri, auth=(user, password))

# Throttling constants
BATCH_SIZE = 50
THROTTLE_DELAY = 0.1  # seconds between batches

def update_root_frequency(tx, root_id, arabic):
    """Update quranic_freq property for a single root."""
    query = """
    MATCH (r:Root {root_id: $root_id})
    OPTIONAL MATCH (c:CorpusItem {corpus_id: 2, root: $arabic})
    WITH r, count(c) AS freq
    SET r.quranic_freq = freq
    RETURN r.arabic AS root, freq
    """
    result = tx.run(query, root_id=root_id, arabic=arabic)
    record = result.single()
    return record

def update_all_root_frequencies():
    """Fetch all roots and update their Quranic frequency counts."""
    logger.info("Starting Quranic frequency update for all roots...")

    try:
        with driver.session() as session:
            # Fetch all roots
            roots_result = session.run("MATCH (r:Root) RETURN r.root_id AS id, r.arabic AS arabic")
            roots = list(roots_result)

            if not roots:
                logger.warning("No roots found in database.")
                return

            logger.info(f"Found {len(roots)} roots to process.")

            updated_count = 0
            error_count = 0

            for i, row in enumerate(roots, start=1):
                try:
                    record = session.execute_write(update_root_frequency, row["id"], row["arabic"])

                    if record:
                        freq = record["freq"]
                        logger.debug(f"[{i}/{len(roots)}] Updated {record['root']} → {freq} occurrences")
                        updated_count += 1

                    # Log progress every BATCH_SIZE roots
                    if i % BATCH_SIZE == 0:
                        logger.info(f"Progress: {i}/{len(roots)} roots processed ({updated_count} updated, {error_count} errors)...")
                        time.sleep(THROTTLE_DELAY)

                except Exception as e:
                    error_count += 1
                    logger.error(f"Error updating root {row['id']} ({row['arabic']}): {str(e)}")

            logger.info(f"✅ Finished updating root frequencies. Updated: {updated_count}, Errors: {error_count}, Total: {len(roots)}")
            return updated_count, error_count

    except Exception as e:
        logger.error(f"Fatal error in update_all_root_frequencies: {str(e)}", exc_info=True)
        raise

def main():
    """Main entry point."""
    try:
        logger.info("=" * 60)
        logger.info("QURANIC FREQUENCY UPDATE SCRIPT STARTED")
        logger.info("=" * 60)

        updated, errors = update_all_root_frequencies()

        logger.info("=" * 60)
        logger.info("QURANIC FREQUENCY UPDATE SCRIPT COMPLETED")
        logger.info(f"Results: {updated} roots updated, {errors} errors")
        logger.info(f"Log saved to: {log_filename}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Script execution failed: {str(e)}", exc_info=True)
        raise
    finally:
        logger.info("Closing database connection...")
        driver.close()
        logger.info("Database connection closed.")

if __name__ == "__main__":
    main()