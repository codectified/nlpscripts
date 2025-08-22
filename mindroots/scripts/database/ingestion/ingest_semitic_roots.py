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
import time
from datetime import datetime
from typing import Dict, Optional
import os
import sys
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Try to import rich for enhanced logging
try:
    from rich.console import Console
    from rich.logging import RichHandler
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn
    from rich.panel import Panel
    from rich.table import Table
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("Rich not available - falling back to standard logging")

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
            'skipped': 0,
            'already_processed': 0
        }
        
        # Rich console setup
        if RICH_AVAILABLE:
            self.console = Console()
            self.progress = None
        
        # Throttling settings for Neo4j Aura
        self.batch_size = 25  # Process in smaller batches
        self.delay_between_batches = 2.0  # 2 second delay between batches
        self.delay_between_operations = 0.1  # 100ms delay between individual operations
        
    def setup_logging(self):
        """Configure comprehensive dual logging with Rich support."""
        self.log_filename = f"semitic_roots_integration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        # Configure file logging (detailed)
        file_handler = logging.FileHandler(self.log_filename, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(funcName)-20s | %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        
        # Configure console logging
        if RICH_AVAILABLE:
            console_handler = RichHandler(
                console=Console(),
                show_time=True,
                show_path=False,
                rich_tracebacks=True
            )
            console_handler.setLevel(logging.INFO)
        else:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(console_formatter)
        
        # Setup logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Log startup
        self.logger.info("🚀 Starting Semitic Roots Integration - Phase 1")
        self.logger.info(f"📝 Detailed logs: {self.log_filename}")
        
        if RICH_AVAILABLE:
            self.console.print(Panel.fit(
                "[bold blue]Semitic Roots Integration - Phase 1[/bold blue]\n"
                "[green]✓[/green] Rich logging enabled\n"
                f"[yellow]📝[/yellow] Log file: {self.log_filename}",
                title="🚀 Integration Started"
            ))
        
    def connect_to_neo4j(self):
        """Connect to Neo4j database."""
        try:
            uri = os.getenv('NEO4J_URI')
            user = os.getenv('NEO4J_USER') 
            password = os.getenv('NEO4J_PASS')
            
            if not all([uri, user, password]):
                raise ValueError("Missing Neo4j environment variables")
                
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
            
            # Test connection and get database info
            with self.driver.session() as session:
                result = session.run("RETURN 1 as test")
                result.single()
                
                # Get existing roots count
                count_result = session.run("MATCH (r:Root) RETURN count(r) as count")
                existing_count = count_result.single()['count']
                
                # Check for already processed roots
                processed_result = session.run("MATCH (r:Root) WHERE r.sem_id IS NOT NULL RETURN count(r) as count")
                already_processed = processed_result.single()['count']
                
            self.logger.info(f"✅ Connected to Neo4j successfully")
            self.logger.info(f"📊 Database status: {existing_count} total roots, {already_processed} already have sem_id")
            
            if RICH_AVAILABLE:
                self.console.print(f"[green]✓[/green] Neo4j connected | [cyan]{existing_count}[/cyan] roots | [yellow]{already_processed}[/yellow] processed")
            
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
            
    def check_root_status(self, arabic_root: str, sem_id: int) -> Dict[str, any]:
        """
        Check root status in Neo4j database.
        
        Returns:
            Dict with 'exists', 'element_id', 'already_processed', 'current_sem_id'
        """
        try:
            with self.driver.session() as session:
                query = """
                MATCH (r:Root {arabic: $arabic_root})
                RETURN elementId(r) as element_id, r.sem_id as current_sem_id
                """
                result = session.run(query, arabic_root=arabic_root)
                record = result.single()
                
                if record:
                    current_sem_id = record['current_sem_id']
                    already_processed = current_sem_id is not None
                    
                    # Check if this exact sem_id already exists (duplicate prevention)
                    if already_processed and current_sem_id == sem_id:
                        self.logger.debug(f"Root {arabic_root} already processed with sem_id {sem_id}")
                        return {
                            'exists': True,
                            'element_id': record['element_id'],
                            'already_processed': True,
                            'current_sem_id': current_sem_id,
                            'is_duplicate': True
                        }
                    
                    return {
                        'exists': True,
                        'element_id': record['element_id'],
                        'already_processed': already_processed,
                        'current_sem_id': current_sem_id,
                        'is_duplicate': False
                    }
                else:
                    return {
                        'exists': False,
                        'element_id': None,
                        'already_processed': False,
                        'current_sem_id': None,
                        'is_duplicate': False
                    }
                    
        except Exception as e:
            self.logger.error(f"Failed to check root status: {arabic_root} - {e}")
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
            
    def update_existing_root(self, root_element_id: str, sem_id: int, concept: str, arabic_root: str, current_sem_id: any):
        """Update existing root with sem_id and concept."""
        try:
            with self.driver.session() as session:
                # Prepare update based on current state
                if current_sem_id is None:
                    # First time adding sem_id
                    query = """
                    MATCH (r:Root) 
                    WHERE elementId(r) = $root_element_id
                    SET r.sem_id = $sem_id, r.concept = $concept
                    RETURN r.arabic as arabic, r.root_id as root_id
                    """
                    action = "Added sem_id"
                else:
                    # Updating existing sem_id (overwrite scenario)
                    query = """
                    MATCH (r:Root) 
                    WHERE elementId(r) = $root_element_id
                    SET r.sem_id = $sem_id, r.concept = $concept
                    RETURN r.arabic as arabic, r.root_id as root_id
                    """
                    action = f"Updated sem_id from {current_sem_id}"
                
                result = session.run(query, 
                                   root_element_id=root_element_id, 
                                   sem_id=sem_id, 
                                   concept=concept)
                record = result.single()
                
                if record:
                    self.logger.info(f"✅ {action} → Root {arabic_root} | sem_id: {sem_id} | concept: '{concept}' | db_id: {record['root_id']}")
                    if RICH_AVAILABLE:
                        self.console.print(f"[green]✓[/green] Updated [cyan]{arabic_root}[/cyan] → sem_id: [yellow]{sem_id}[/yellow] | [dim]{concept}[/dim]")
                    self.stats['existing_updated'] += 1
                    # Throttling for Aura
                    time.sleep(self.delay_between_operations)
                else:
                    self.logger.warning(f"⚠️ Failed to update root with element ID: {root_element_id}")
                    
        except Exception as e:
            self.logger.error(f"❌ Failed to update existing root {arabic_root}: {e}")
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
                
            self.logger.debug(f"Creating new {root_data['root_type'].lower()} root: {root_data['arabic']} with properties: {properties}")
                
            with self.driver.session() as session:
                query = """
                CREATE (r:Root $properties)
                RETURN r.arabic as arabic, r.root_id as root_id
                """
                result = session.run(query, properties=properties)
                record = result.single()
                
                if record:
                    trilateral_info = f" | Triliteral_ID: {triliteral_id}" if triliteral_id else ""
                    self.logger.info(f"✨ Created NEW root → {record['arabic']} | sem_id: {sem_id} | concept: '{concept}' | db_id: {record['root_id']}{trilateral_info}")
                    if RICH_AVAILABLE:
                        self.console.print(f"[bold green]✨ NEW[/bold green] [cyan]{record['arabic']}[/cyan] → sem_id: [yellow]{sem_id}[/yellow] | [dim]{concept}[/dim]")
                    self.stats['new_created'] += 1
                    # Throttling for Aura
                    time.sleep(self.delay_between_operations)
                else:
                    self.logger.warning(f"⚠️ Failed to create root: {root_data['arabic']}")
                    
        except Exception as e:
            self.logger.error(f"❌ Failed to create new root {root_data['arabic']}: {e}")
            raise
            
    def process_roots(self, limit: Optional[int] = None):
        """
        Main processing function to integrate roots from sem_root.csv.
        
        Args:
            limit: Optional limit for testing (process only first N roots)
        """
        try:
            self.logger.info("📊 Starting root processing...")
            self.logger.info(f"⚙️ Throttling: batch_size={self.batch_size}, batch_delay={self.delay_between_batches}s, op_delay={self.delay_between_operations}s")
            
            # Count total roots for progress tracking
            with open('sem_root.csv', 'r', encoding='utf-8') as f:
                total_roots = sum(1 for _ in csv.DictReader(f)) - (1 if limit else 0)  # Subtract 1 for header
                if limit:
                    total_roots = min(total_roots, limit)
            
            self.logger.info(f"🎯 Target: {total_roots} roots to process")
            
            # Initialize progress bar if Rich is available
            if RICH_AVAILABLE:
                self.progress = Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    MofNCompleteColumn(),
                    console=self.console
                )
                progress_task = self.progress.add_task("Processing roots", total=total_roots)
                self.progress.start()
            
            with open('sem_root.csv', 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                batch_count = 0
                
                for i, row in enumerate(reader):
                    if limit and i >= limit:
                        self.logger.info(f"🏁 Reached limit of {limit} roots")
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
                        
                        self.logger.debug(f"Processing sem_id {sem_id}: {arabic_root} (concept: {concept})")
                        
                        # Check root status (existence, duplicate prevention)
                        root_status = self.check_root_status(arabic_root, sem_id)
                        
                        if root_status['is_duplicate']:
                            # Skip duplicate processing
                            self.logger.info(f"♾️ SKIP duplicate → {arabic_root} already has sem_id {sem_id}")
                            if RICH_AVAILABLE:
                                self.console.print(f"[yellow]♾️ SKIP[/yellow] [cyan]{arabic_root}[/cyan] already processed")
                            self.stats['already_processed'] += 1
                        elif root_status['exists']:
                            # Update existing root
                            self.update_existing_root(
                                root_status['element_id'], 
                                sem_id, 
                                concept, 
                                arabic_root, 
                                root_status['current_sem_id']
                            )
                        else:
                            # Create new root
                            self.create_new_root(root_data, sem_id, concept)
                            
                        self.stats['total_processed'] += 1
                        
                        # Update progress bar
                        if RICH_AVAILABLE and hasattr(self, 'progress'):
                            self.progress.update(progress_task, advance=1)
                        
                        # Batch throttling for Neo4j Aura
                        if self.stats['total_processed'] % self.batch_size == 0:
                            batch_count += 1
                            remaining = total_roots - self.stats['total_processed']
                            self.logger.info(f"🔄 Batch {batch_count} complete ({self.stats['total_processed']}/{total_roots}) | {remaining} remaining")
                            self.logger.info(f"⏳ Pausing {self.delay_between_batches}s for Neo4j Aura throttling...")
                            if RICH_AVAILABLE:
                                self.console.print(f"[blue]⏳[/blue] Batch pause ({self.delay_between_batches}s) | [green]{self.stats['existing_updated']} updated[/green] | [yellow]{self.stats['new_created']} created[/yellow]")
                            time.sleep(self.delay_between_batches)
                            
                    except Exception as e:
                        self.logger.error(f"❌ Error processing row {i+1} (sem_id {row.get('id', 'unknown')}): {e}")
                        self.stats['errors'] += 1
                        continue
                        
            # Stop progress bar
            if RICH_AVAILABLE and hasattr(self, 'progress'):
                self.progress.stop()
                        
        except Exception as e:
            self.logger.error(f"Fatal error in process_roots: {e}")
            raise
            
    def print_statistics(self):
        """Print comprehensive final statistics."""
        success_count = self.stats['existing_updated'] + self.stats['new_created']
        total_attempted = self.stats['total_processed']
        success_rate = (success_count / max(total_attempted, 1)) * 100
        
        self.logger.info("🏁 " + "=" * 58)
        self.logger.info("🏁 INTEGRATION COMPLETE - FINAL STATISTICS")
        self.logger.info("🏁 " + "=" * 58)
        self.logger.info(f"📊 Total roots processed: {total_attempted}")
        self.logger.info(f"✅ Existing roots updated: {self.stats['existing_updated']}")
        self.logger.info(f"✨ New roots created: {self.stats['new_created']}")
        self.logger.info(f"♾️ Already processed (skipped): {self.stats['already_processed']}")
        self.logger.info(f"❌ Errors encountered: {self.stats['errors']}")
        self.logger.info(f"🎆 Success rate: {success_rate:.1f}%")
        self.logger.info(f"📝 Detailed log: {self.log_filename}")
        
        if RICH_AVAILABLE:
            # Create rich statistics table
            table = Table(title="🏆 Integration Results")
            table.add_column("Metric", style="bold")
            table.add_column("Count", justify="right")
            table.add_column("Description", style="dim")
            
            table.add_row("📊 Total Processed", str(total_attempted), "Roots from sem_root.csv")
            table.add_row("✅ Updated", str(self.stats['existing_updated']), "Existing roots with new sem_id")
            table.add_row("✨ Created", str(self.stats['new_created']), "Brand new root nodes")
            table.add_row("♾️ Skipped", str(self.stats['already_processed']), "Already had correct sem_id")
            table.add_row("❌ Errors", str(self.stats['errors']), "Failed operations")
            table.add_row("🎆 Success Rate", f"{success_rate:.1f}%", "Successful operations")
            
            self.console.print(table)
            self.console.print(Panel.fit(
                f"[green]✓[/green] Integration completed successfully!\n"
                f"[yellow]📝[/yellow] Detailed logs: {self.log_filename}\n"
                f"[blue]🚀[/blue] Ready for Phase 2: Word integration",
                title="🏁 Phase 1 Complete"
            ))
        
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
        
        # Process all roots (limit removed for full processing)
        integrator.process_roots()
        
        # Print statistics
        integrator.print_statistics()
        
        if RICH_AVAILABLE:
            integrator.console.print("\n[bold green]✨ Phase 1 Integration Complete![/bold green]")
            integrator.console.print("[blue]🚀 Next:[/blue] Phase 2 - Integrate sem_word.csv data")
        else:
            print("\n✨ Phase 1 Complete!")
            print("🚀 Next step: Phase 2 - Integrate sem_word.csv data")
        
    except Exception as e:
        print(f"❌ Integration failed: {e}")
        if integrator:
            integrator.logger.error(f"Integration failed: {e}")
        
    finally:
        if integrator:
            integrator.close()


if __name__ == "__main__":
    main()