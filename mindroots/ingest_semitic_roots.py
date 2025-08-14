#!/usr/bin/env python3
"""
Semitic Roots Integration Script - Phase 1: Root Nodes

This script integrates Semitic root data from sem_root.csv into the Neo4j database.

INTEGRATION LOGIC:
1. Load Arabic alphabet mapping from sem_lang.csv  
2. For each root in sem_root.csv:
   - Reconstruct Arabic root from radical IDs (e.g., 11,22,23 → س-ل-م)
   - Check if root exists in Neo4j (match on 'arabic' property)
   - If exists: Add 'sem_id' property with the sem_root.csv ID
   - If not exists: Create new Root node with all standard properties + sem_id

ROOT NODE PROPERTIES (based on existing schema):
- r1, r2, r3: Individual radical characters  
- arabic: Root with hyphens (e.g., "س-ل-م")
- n_root: Normalized root (same as arabic for now)
- english: Transliterated root (e.g., "s-l-m")
- node_type: "Root"
- root_type: "Triliteral" or "Quadriliteral" 
- Triliteral_ID: Auto-generated for triliterals
- root_id: Auto-generated unique ID
- sem_id: ID from sem_root.csv (NEW)
- concept: English concept from sem_root.csv (NEW)

Author: Claude Code
Date: 2025-08-13
"""

import csv
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class SemiticRootsIntegrator:
    def __init__(self):
        self.setup_logging()
        self.connect_to_neo4j()
        self.arabic_alphabet = {}
        self.stats = {
            'total_processed': 0,
            'existing_updated': 0,
            'new_created': 0,
            'errors': 0,
            'skipped': 0
        }
        
    def setup_logging(self):
        """Configure comprehensive logging."""
        log_filename = f"semitic_roots_integration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("Starting Semitic Roots Integration - Phase 1")
        
    def connect_to_neo4j(self):
        """Connect to Neo4j database."""
        try:
            uri = os.getenv('NEO4J_URI')
            user = os.getenv('NEO4J_USER') 
            password = os.getenv('NEO4J_PASS')
            
            if not all([uri, user, password]):
                raise ValueError("Missing Neo4j environment variables")
                
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
            
            # Test connection
            with self.driver.session() as session:
                result = session.run("RETURN 1 as test")
                result.single()
                
            self.logger.info("Successfully connected to Neo4j")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Neo4j: {e}")
            raise
            
    def load_arabic_alphabet(self) -> Dict[int, str]:
        """Load Arabic alphabet mapping from sem_lang.csv."""
        try:
            with open('sem_lang.csv', 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row['lang'] == 'Arabic':
                        arabic_chars = row['script'].split(',')
                        translit_chars = row['translit'].split(',')
                        
                        # Create mappings
                        self.arabic_alphabet = {i: char for i, char in enumerate(arabic_chars)}
                        self.transliteration = {i: trans for i, trans in enumerate(translit_chars)}
                        
                        self.logger.info(f"Loaded Arabic alphabet with {len(self.arabic_alphabet)} characters")
                        return self.arabic_alphabet
                        
            raise ValueError("Arabic language not found in sem_lang.csv")
            
        except Exception as e:
            self.logger.error(f"Failed to load Arabic alphabet: {e}")
            raise
            
    def reconstruct_root(self, rad1: int, rad2: int, rad3: int, rad4: Optional[str] = None) -> Dict[str, str]:
        """
        Reconstruct root from radical IDs.
        
        Returns:
            Dict with 'arabic', 'english', 'r1', 'r2', 'r3', 'r4', 'root_type'
        """
        try:
            # Get Arabic characters
            r1 = self.arabic_alphabet.get(rad1, f"?{rad1}")
            r2 = self.arabic_alphabet.get(rad2, f"?{rad2}")  
            r3 = self.arabic_alphabet.get(rad3, f"?{rad3}")
            
            # Get transliterations
            t1 = self.transliteration.get(rad1, f"?{rad1}")
            t2 = self.transliteration.get(rad2, f"?{rad2}")
            t3 = self.transliteration.get(rad3, f"?{rad3}")
            
            result = {
                'r1': r1,
                'r2': r2, 
                'r3': r3,
                'arabic': f"{r1}-{r2}-{r3}",
                'english': f"{t1}-{t2}-{t3}",
                'root_type': 'Triliteral'
            }
            
            # Handle quadriliteral roots
            if rad4 and rad4.strip():
                rad4_int = int(rad4)
                r4 = self.arabic_alphabet.get(rad4_int, f"?{rad4}")
                t4 = self.transliteration.get(rad4_int, f"?{rad4}")
                
                result.update({
                    'r4': r4,
                    'arabic': f"{r1}-{r2}-{r3}-{r4}",
                    'english': f"{t1}-{t2}-{t3}-{t4}",
                    'root_type': 'Quadriliteral'
                })
                
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to reconstruct root from {rad1},{rad2},{rad3},{rad4}: {e}")
            raise
            
    def check_root_exists(self, arabic_root: str) -> Optional[int]:
        """
        Check if root exists in Neo4j database.
        
        Returns:
            Root node ID if exists, None otherwise
        """
        try:
            with self.driver.session() as session:
                query = """
                MATCH (r:Root {arabic: $arabic_root})
                RETURN elementId(r) as root_id
                """
                result = session.run(query, arabic_root=arabic_root)
                record = result.single()
                
                if record:
                    return record['root_id']
                return None
                
        except Exception as e:
            self.logger.error(f"Failed to check if root exists: {arabic_root} - {e}")
            raise
            
    def get_next_root_id(self) -> int:
        """Get next available root_id."""
        try:
            with self.driver.session() as session:
                query = "MATCH (r:Root) RETURN COALESCE(MAX(r.root_id), 0) + 1 as next_id"
                result = session.run(query)
                return result.single()['next_id']
                
        except Exception as e:
            self.logger.error(f"Failed to get next root ID: {e}")
            raise
            
    def get_next_triliteral_id(self) -> int:
        """Get next available Triliteral_ID."""
        try:
            with self.driver.session() as session:
                query = """
                MATCH (r:Root {root_type: 'Triliteral'}) 
                RETURN COALESCE(MAX(r.Triliteral_ID), 0) + 1 as next_id
                """
                result = session.run(query)
                return result.single()['next_id']
                
        except Exception as e:
            self.logger.error(f"Failed to get next Triliteral ID: {e}")
            raise
            
    def update_existing_root(self, root_element_id: str, sem_id: int, concept: str):
        """Update existing root with sem_id and concept."""
        try:
            with self.driver.session() as session:
                query = """
                MATCH (r:Root) 
                WHERE elementId(r) = $root_element_id
                SET r.sem_id = $sem_id, r.concept = $concept
                RETURN r.arabic as arabic
                """
                result = session.run(query, 
                                   root_element_id=root_element_id, 
                                   sem_id=sem_id, 
                                   concept=concept)
                record = result.single()
                
                if record:
                    self.logger.info(f"Updated existing root: {record['arabic']} with sem_id: {sem_id}")
                    self.stats['existing_updated'] += 1
                else:
                    self.logger.warning(f"Failed to update root with element ID: {root_element_id}")
                    
        except Exception as e:
            self.logger.error(f"Failed to update existing root: {e}")
            raise
            
    def create_new_root(self, root_data: Dict, sem_id: int, concept: str):
        """Create new root node."""
        try:
            # Get next IDs
            root_id = self.get_next_root_id()
            triliteral_id = None
            if root_data['root_type'] == 'Triliteral':
                triliteral_id = self.get_next_triliteral_id()
                
            # Prepare properties
            properties = {
                'r1': root_data['r1'],
                'r2': root_data['r2'],
                'r3': root_data['r3'],
                'arabic': root_data['arabic'],
                'n_root': root_data['arabic'],  # Same as arabic for now
                'english': root_data['english'],
                'node_type': 'Root',
                'root_type': root_data['root_type'],
                'root_id': root_id,
                'sem_id': sem_id,
                'concept': concept
            }
            
            # Add r4 for quadriliterals
            if 'r4' in root_data:
                properties['r4'] = root_data['r4']
            
            # Add Triliteral_ID for triliterals
            if triliteral_id:
                properties['Triliteral_ID'] = triliteral_id
                
            with self.driver.session() as session:
                query = """
                CREATE (r:Root $properties)
                RETURN r.arabic as arabic
                """
                result = session.run(query, properties=properties)
                record = result.single()
                
                if record:
                    self.logger.info(f"Created new root: {record['arabic']} with sem_id: {sem_id}")
                    self.stats['new_created'] += 1
                else:
                    self.logger.warning(f"Failed to create root: {root_data['arabic']}")
                    
        except Exception as e:
            self.logger.error(f"Failed to create new root: {e}")
            raise
            
    def process_roots(self, limit: Optional[int] = None):
        """
        Main processing function to integrate roots from sem_root.csv.
        
        Args:
            limit: Optional limit for testing (process only first N roots)
        """
        try:
            self.logger.info("Starting root processing...")
            
            with open('sem_root.csv', 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for i, row in enumerate(reader):
                    if limit and i >= limit:
                        self.logger.info(f"Reached limit of {limit} roots")
                        break
                        
                    try:
                        # Parse row data
                        sem_id = int(row['id'])
                        rad1 = int(row['rad1'])
                        rad2 = int(row['rad2'])
                        rad3 = int(row['rad3'])
                        rad4 = row['rad4'] if row['rad4'].strip() else None
                        concept = row['concept']
                        
                        # Reconstruct root
                        root_data = self.reconstruct_root(rad1, rad2, rad3, rad4)
                        arabic_root = root_data['arabic']
                        
                        self.logger.info(f"Processing root {sem_id}: {arabic_root} (concept: {concept})")
                        
                        # Check if root exists
                        existing_root_id = self.check_root_exists(arabic_root)
                        
                        if existing_root_id:
                            # Update existing root
                            self.update_existing_root(existing_root_id, sem_id, concept)
                        else:
                            # Create new root
                            self.create_new_root(root_data, sem_id, concept)
                            
                        self.stats['total_processed'] += 1
                        
                        # Progress logging
                        if self.stats['total_processed'] % 50 == 0:
                            self.logger.info(f"Progress: {self.stats['total_processed']} roots processed")
                            
                    except Exception as e:
                        self.logger.error(f"Error processing row {i+1}: {e}")
                        self.stats['errors'] += 1
                        continue
                        
        except Exception as e:
            self.logger.error(f"Fatal error in process_roots: {e}")
            raise
            
    def print_statistics(self):
        """Print final statistics."""
        self.logger.info("=" * 60)
        self.logger.info("INTEGRATION COMPLETE - STATISTICS")
        self.logger.info("=" * 60)
        self.logger.info(f"Total roots processed: {self.stats['total_processed']}")
        self.logger.info(f"Existing roots updated: {self.stats['existing_updated']}")
        self.logger.info(f"New roots created: {self.stats['new_created']}")
        self.logger.info(f"Errors encountered: {self.stats['errors']}")
        self.logger.info(f"Success rate: {((self.stats['total_processed'] - self.stats['errors']) / max(self.stats['total_processed'], 1)) * 100:.1f}%")
        
    def close(self):
        """Clean up resources."""
        if hasattr(self, 'driver'):
            self.driver.close()
            self.logger.info("Database connection closed")


def main():
    """Main execution function."""
    integrator = None
    try:
        # Initialize integrator
        integrator = SemiticRootsIntegrator()
        
        # Load Arabic alphabet
        integrator.load_arabic_alphabet()
        
        # Process roots (use limit=10 for testing, remove for full processing)
        integrator.process_roots(limit=10)  # Remove limit parameter for full processing
        
        # Print statistics
        integrator.print_statistics()
        
        print("\n✅ Phase 1 Complete!")
        print("Next step: Phase 2 - Integrate sem_word.csv data")
        
    except Exception as e:
        print(f"❌ Integration failed: {e}")
        if integrator:
            integrator.logger.error(f"Integration failed: {e}")
        
    finally:
        if integrator:
            integrator.close()


if __name__ == "__main__":
    main()