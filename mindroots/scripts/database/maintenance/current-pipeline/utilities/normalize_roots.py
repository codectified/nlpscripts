import time
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import logging

# --- Setup ---
load_dotenv()
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASS")
driver = GraphDatabase.driver(uri, auth=(user, password))

# --- Logging ---
logger = logging.getLogger("normalize_roots")
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler("../logs/normalize_roots.log", encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(ch)

# --- Main function ---
def normalize_roots():
    logger.info("🔵 Starting root normalization (adding plain_root property)...")
    
    try:
        with driver.session() as session:
            # First, let's count how many roots need normalization
            count_query = """
            MATCH (r:Root)
            WHERE r.n_root IS NOT NULL 
              AND r.n_root CONTAINS '-'
              AND r.plain_root IS NULL
            RETURN count(r) AS total
            """
            result = session.run(count_query)
            total_count = result.single()["total"]
            logger.info(f"📊 Found {total_count} Root nodes requiring normalization")
            
            if total_count == 0:
                logger.info("✅ No roots need normalization. Exiting.")
                return
            
            # Update roots by adding plain_root property
            update_query = """
            MATCH (r:Root)
            WHERE r.n_root IS NOT NULL 
              AND r.n_root CONTAINS '-'
              AND r.plain_root IS NULL
            SET r.plain_root = REPLACE(r.n_root, '-', '')
            RETURN count(r) AS updated
            """
            
            result = session.run(update_query)
            updated_count = result.single()["updated"]
            logger.info(f"✅ Updated {updated_count} Root nodes with plain_root property")
            
            # Verify the results
            verify_query = """
            MATCH (r:Root)
            WHERE r.plain_root IS NOT NULL
            RETURN count(r) AS total_with_plain_root
            """
            result = session.run(verify_query)
            final_count = result.single()["total_with_plain_root"]
            logger.info(f"📊 Total Root nodes now with plain_root: {final_count}")
            
            # Show some examples
            sample_query = """
            MATCH (r:Root)
            WHERE r.plain_root IS NOT NULL
            RETURN r.n_root AS original, r.plain_root AS normalized
            LIMIT 5
            """
            result = session.run(sample_query)
            logger.info("📝 Sample normalizations:")
            for record in result:
                logger.info(f"   '{record['original']}' → '{record['normalized']}'")
                
    except Exception as e:
        logger.error(f"❌ Error during root normalization: {e}")
        raise
    finally:
        driver.close()

    logger.info("🎉 Root normalization completed successfully!")

if __name__ == "__main__":
    normalize_roots()