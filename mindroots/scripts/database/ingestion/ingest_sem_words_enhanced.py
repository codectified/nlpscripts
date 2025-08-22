#!/usr/bin/env python3
"""
Enhanced Semitic Words Integration Script - Phase 2: Word Nodes

This script integrates Semitic word data from sem_word.csv into the Neo4j database,
linking them to the root nodes created in Phase 1.

INTEGRATION LOGIC:
1. Load word data from sem_word.csv
2. Resolve sem_id (root references) using parent-child relationships
3. Check for existing words to prevent duplicates
4. Create Word nodes and link to Root nodes via sem_id
5. Track detailed statistics and provide rich visual feedback

WORD NODE PROPERTIES:
- word: The actual word text (Arabic, Hebrew, etc.)
- lang: Language ID (1=Arabic, 2=Hebrew, 3=Sabaic, etc.)  
- category: Word category (0=base/lemma, 1=derived)
- concept: Semantic concept
- meaning: English meaning/definition
- sem_word_id: Original ID from sem_word.csv (for tracking)

Author: Claude Code
Date: 2025-08-14
"""

import csv
import os
import time
import logging
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Rich imports with fallback
try:
    from rich.console import Console
    from rich.logging import RichHandler
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn, TimeElapsedColumn
    from rich.panel import Panel
    from rich.table import Table
    from rich.live import Live
    from rich.layout import Layout
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("⚠️  Rich not available - falling back to standard logging")

# Load environment variables
load_dotenv()

class SemiticWordsIntegrator:
    def __init__(self):
        self.setup_logging()
        self.connect_to_neo4j()
        self.stats = {
            'total_processed': 0,
            'words_created': 0,
            'duplicates_skipped': 0,
            'errors': 0,
            'resolve_failures': 0,
            'missing_roots': 0,
            'root_links_created': 0
        }
        
        # Processing settings
        self.batch_size = 150  # Conservative for Aura
        self.delay_between_batches = 1.5  # 1.5 second delay
        self.delay_between_operations = 0.05  # 50ms delay
        
        # Rich console setup
        if RICH_AVAILABLE:
            self.console = Console()
            self.progress = None
            
    def setup_logging(self):
        """Configure comprehensive dual logging with Rich support."""
        self.log_filename = f"sem_words_integration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        # Create logs directory if it doesn't exist
        os.makedirs("../../../logs", exist_ok=True)
        log_path = f"../../../logs/{self.log_filename}"
        
        # Configure file logging (detailed)
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(funcName)-25s | %(message)s'
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
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Log startup
        self.logger.info("🚀 Starting Semitic Words Integration - Phase 2")
        self.logger.info(f"📝 Detailed logs: {log_path}")
        
        if RICH_AVAILABLE:
            self.console.print(Panel.fit(
                "[bold blue]Semitic Words Integration - Phase 2[/bold blue]\\n"
                "[green]✓[/green] Rich logging enabled\\n"
                f"[yellow]📝[/yellow] Log file: {log_path}",
                title="🚀 Word Integration Started"
            ))
            
    def connect_to_neo4j(self):
        """Connect to Neo4j database and verify setup."""
        try:
            uri = os.getenv('NEO4J_URI')
            user = os.getenv('NEO4J_USER')
            password = os.getenv('NEO4J_PASS')
            
            if not all([uri, user, password]):
                raise ValueError("Missing Neo4j environment variables")
                
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
            
            # Test connection and get database status
            with self.driver.session() as session:
                result = session.run("RETURN 1 as test")
                result.single()
                
                # Get root nodes with sem_id count
                root_result = session.run("MATCH (r:Root) WHERE r.sem_id IS NOT NULL RETURN count(r) as count")
                roots_with_sem_id = root_result.single()['count']
                
                # Get existing word count
                word_result = session.run("MATCH (w:Word) RETURN count(w) as count")
                existing_words = word_result.single()['count']
                
                # Check for existing word relationships
                rel_result = session.run("MATCH ()-[r:BELONGS_TO_SEMITIC_ROOT]->() RETURN count(r) as count")
                existing_relationships = rel_result.single()['count']
                
            self.logger.info(f"✅ Connected to Neo4j successfully")
            self.logger.info(f"📊 Database status: {roots_with_sem_id} roots with sem_id, {existing_words} existing words, {existing_relationships} word-root links")
            
            if RICH_AVAILABLE:
                self.console.print(f"[green]✓[/green] Neo4j connected | [cyan]{roots_with_sem_id}[/cyan] roots ready | [yellow]{existing_words}[/yellow] existing words")
                
        except Exception as e:
            self.logger.error(f"Failed to connect to Neo4j: {e}")
            raise
            
    def load_sem_words(self, csv_path: str) -> Dict[int, Dict]:
        """Load and validate sem_word.csv data."""
        try:
            self.logger.info(f"📚 Loading word data from {csv_path}")
            
            rows = {}
            with open(csv_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                
                # Validate required columns
                required = {"id", "category", "lang", "root", "word", "concept", "meaning", "root_lang"}
                missing = required - set(reader.fieldnames or [])
                if missing:
                    raise ValueError(f"CSV missing columns: {missing}")
                
                for row in reader:
                    try:
                        row_id = int(row["id"])
                        rows[row_id] = row
                    except (KeyError, ValueError) as e:
                        self.logger.warning(f"⚠️ Skipping invalid row (bad id): {row} - {e}")
                        
            self.logger.info(f"✅ Loaded {len(rows)} word entries from {csv_path}")
            
            if RICH_AVAILABLE:
                self.console.print(f"[green]✓[/green] Loaded [cyan]{len(rows)}[/cyan] word entries")
                
            return rows
            
        except Exception as e:
            self.logger.error(f"❌ Failed to load word data: {e}")
            raise
            
    def resolve_sem_id(self, row: Dict, rows_by_id: Dict[int, Dict]) -> Optional[int]:
        """
        Resolve the sem_id (root reference) for a word entry.
        
        Logic:
        - If root_lang is empty: row['root'] is the direct sem_id
        - If root_lang is set: row['root'] is a parent row ID, follow chain to get sem_id
        """
        try:
            root_lang_val = (row.get("root_lang") or "").strip()
            
            if root_lang_val:  # Derived word - follow parent chain
                parent_row_id = int(row["root"])
                parent = rows_by_id.get(parent_row_id)
                
                if not parent:
                    self.logger.warning(f"⚠️ Parent row id {parent_row_id} not found for child id {row.get('id')}")
                    return None
                    
                # Parent's root is the sem_id
                sem_id = int(parent["root"])
                self.logger.debug(f"🔗 Resolved derived word {row.get('id')} → parent {parent_row_id} → sem_id {sem_id}")
                return sem_id
                
            else:  # Base/lemma word - direct sem_id
                sem_id = int(row["root"])
                self.logger.debug(f"📍 Direct sem_id for word {row.get('id')}: {sem_id}")
                return sem_id
                
        except (ValueError, TypeError) as e:
            self.logger.warning(f"⚠️ Failed to resolve sem_id for row {row.get('id')}: {e}")
            return None
            
    def check_word_duplicate(self, word: str, lang: int, sem_id: int, meaning: str) -> bool:
        """
        Check if word already exists to prevent duplicates.
        
        Match on: word + lang + sem_id (same word in same language for same root)
        """
        try:
            with self.driver.session() as session:
                query = """
                MATCH (w:Word {word: $word, lang: $lang})
                MATCH (w)-[:BELONGS_TO_SEMITIC_ROOT]->(r:Root {sem_id: $sem_id})
                RETURN count(w) as count
                """
                result = session.run(query, word=word, lang=lang, sem_id=sem_id)
                count = result.single()['count']
                
                is_duplicate = count > 0
                if is_duplicate:
                    self.logger.debug(f"🔍 Duplicate detected: '{word}' (lang: {lang}, sem_id: {sem_id})")
                
                return is_duplicate
                
        except Exception as e:
            self.logger.error(f"❌ Failed to check for duplicates: {e}")
            # On error, assume not duplicate to avoid blocking processing
            return False
            
    def create_word_and_link(self, word_data: Dict, sem_id: int, sem_word_id: int, create_missing_roots: bool = True) -> bool:
        """
        Create Word node and link to Root via sem_id.
        
        Returns True if successful, False if failed.
        """
        try:
            root_clause = "MERGE" if create_missing_roots else "MATCH"
            
            # Create word with link to root
            query = f"""
            {root_clause} (r:Root {{sem_id: $sem_id}})
            CREATE (w:Word {{
                word: $word,
                lang: toInteger($lang),
                category: toInteger($category),
                concept: $concept,
                meaning: $meaning,
                sem_word_id: $sem_word_id
            }})
            CREATE (w)-[:BELONGS_TO_SEMITIC_ROOT]->(r)
            RETURN w.word as created_word, r.arabic as root_arabic
            """
            
            with self.driver.session() as session:
                result = session.run(query, {
                    "sem_id": sem_id,
                    "word": word_data.get("word"),
                    "lang": word_data.get("lang"),
                    "category": word_data.get("category"),
                    "concept": word_data.get("concept"),
                    "meaning": word_data.get("meaning"),
                    "sem_word_id": sem_word_id
                })
                
                record = result.single()
                
                if record:
                    created_word = record['created_word']
                    root_arabic = record.get('root_arabic', 'unknown')
                    
                    # Determine language name
                    lang_names = {1: 'Arabic', 2: 'Hebrew', 3: 'Sabaic', 4: 'Ugaritic', 5: 'Aramaic'}
                    lang_name = lang_names.get(int(word_data.get("lang", 0)), f"Lang{word_data.get('lang')}")
                    
                    self.logger.info(f"✨ Created word: '{created_word}' ({lang_name}) → root {root_arabic} | sem_id: {sem_id}")
                    
                    if RICH_AVAILABLE:
                        self.console.print(f"[green]✨[/green] [cyan]{created_word}[/cyan] ([dim]{lang_name}[/dim]) → [yellow]{root_arabic}[/yellow]")
                    
                    self.stats['words_created'] += 1
                    self.stats['root_links_created'] += 1
                    
                    # Throttling
                    time.sleep(self.delay_between_operations)
                    return True
                else:
                    self.logger.warning(f"⚠️ Failed to create word for sem_word_id {sem_word_id}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"❌ Failed to create word for sem_word_id {sem_word_id}: {e}")
            return False
            
    def process_words(self, csv_path: str, batch_size: int = 150, create_missing_roots: bool = True, limit: Optional[int] = None):
        """
        Main processing function to integrate words from sem_word.csv.
        
        Args:
            csv_path: Path to sem_word.csv
            batch_size: Number of words to process per batch
            create_missing_roots: Create root nodes if they don't exist
            limit: Optional limit for testing
        """
        try:
            # Load word data
            rows_by_id = self.load_sem_words(csv_path)
            
            total_words = len(rows_by_id)
            if limit:
                total_words = min(total_words, limit)
                
            self.logger.info(f"🎯 Target: {total_words} words to process")
            self.logger.info(f"⚙️ Settings: batch_size={batch_size}, create_missing_roots={create_missing_roots}")
            
            # Initialize progress tracking
            if RICH_AVAILABLE:
                self.progress = Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    MofNCompleteColumn(),
                    TimeElapsedColumn(),
                    console=self.console
                )
                progress_task = self.progress.add_task("Processing words", total=total_words)
                self.progress.start()
                
            # Process words
            batch_count = 0
            
            for idx, (row_id, row) in enumerate(rows_by_id.items(), start=1):
                if limit and idx > limit:
                    self.logger.info(f"🏁 Reached limit of {limit} words")
                    break
                    
                try:
                    # Resolve sem_id
                    sem_id = self.resolve_sem_id(row, rows_by_id)
                    
                    if sem_id is None:
                        self.logger.warning(f"⚠️ Skipping word {row_id} - could not resolve sem_id")
                        self.stats['resolve_failures'] += 1
                        self.stats['total_processed'] += 1
                        continue
                        
                    # Check for duplicates
                    word_text = row.get("word", "").strip()
                    lang = int(row.get("lang", 0))
                    meaning = row.get("meaning", "")
                    
                    if not word_text:
                        self.logger.warning(f"⚠️ Skipping word {row_id} - empty word text")
                        self.stats['errors'] += 1
                        self.stats['total_processed'] += 1
                        continue
                        
                    if self.check_word_duplicate(word_text, lang, sem_id, meaning):
                        self.logger.info(f"⚪ SKIP duplicate: '{word_text}' already exists for sem_id {sem_id}")
                        if RICH_AVAILABLE:
                            self.console.print(f"[yellow]⚪ SKIP[/yellow] [dim]'{word_text}' (duplicate)[/dim]")
                        self.stats['duplicates_skipped'] += 1
                        self.stats['total_processed'] += 1
                        continue
                        
                    # Create word and link to root
                    success = self.create_word_and_link(row, sem_id, row_id, create_missing_roots)
                    
                    if not success:
                        self.stats['errors'] += 1
                        
                    self.stats['total_processed'] += 1
                    
                    # Update progress
                    if RICH_AVAILABLE and hasattr(self, 'progress'):
                        self.progress.update(progress_task, advance=1)
                        
                    # Batch throttling
                    if self.stats['total_processed'] % batch_size == 0:
                        batch_count += 1
                        remaining = total_words - self.stats['total_processed']
                        self.logger.info(f"🔄 Batch {batch_count} complete ({self.stats['total_processed']}/{total_words}) | {remaining} remaining")
                        
                        if RICH_AVAILABLE:
                            self.console.print(f"[blue]⏳[/blue] Batch pause ({self.delay_between_batches}s) | [green]{self.stats['words_created']} created[/green] | [yellow]{self.stats['duplicates_skipped']} skipped[/yellow]")
                            
                        time.sleep(self.delay_between_batches)
                        
                except Exception as e:
                    self.logger.error(f"❌ Error processing word {row_id}: {e}")
                    self.stats['errors'] += 1
                    self.stats['total_processed'] += 1
                    continue
                    
            # Stop progress bar
            if RICH_AVAILABLE and hasattr(self, 'progress'):
                self.progress.stop()
                
        except Exception as e:
            self.logger.error(f"❌ Fatal error in process_words: {e}")
            raise
            
    def print_statistics(self):
        """Print comprehensive final statistics."""
        success_count = self.stats['words_created']
        total_attempted = self.stats['total_processed']
        success_rate = (success_count / max(total_attempted, 1)) * 100
        
        self.logger.info("🏆 " + "=" * 58)
        self.logger.info("🏆 WORD INTEGRATION COMPLETE - FINAL STATISTICS")
        self.logger.info("🏆 " + "=" * 58)
        self.logger.info(f"📊 Total words processed: {total_attempted}")
        self.logger.info(f"✨ Words created: {self.stats['words_created']}")
        self.logger.info(f"🔗 Root links created: {self.stats['root_links_created']}")
        self.logger.info(f"⚪ Duplicates skipped: {self.stats['duplicates_skipped']}")
        self.logger.info(f"⚠️ Resolve failures: {self.stats['resolve_failures']}")
        self.logger.info(f"❌ Errors: {self.stats['errors']}")
        self.logger.info(f"🎯 Success rate: {success_rate:.1f}%")
        self.logger.info(f"📝 Detailed log: logs/{self.log_filename}")
        
        if RICH_AVAILABLE:
            # Create rich statistics table
            table = Table(title="🏆 Word Integration Results")
            table.add_column("Metric", style="bold")
            table.add_column("Count", justify="right")
            table.add_column("Description", style="dim")
            
            table.add_row("📊 Total Processed", str(total_attempted), "Words from sem_word.csv")
            table.add_row("✨ Words Created", str(self.stats['words_created']), "New Word nodes created")
            table.add_row("🔗 Links Created", str(self.stats['root_links_created']), "BELONGS_TO_SEMITIC_ROOT relationships")
            table.add_row("⚪ Duplicates Skipped", str(self.stats['duplicates_skipped']), "Already existing words")
            table.add_row("⚠️ Resolve Failures", str(self.stats['resolve_failures']), "Could not resolve sem_id")
            table.add_row("❌ Errors", str(self.stats['errors']), "Failed operations")
            table.add_row("🎯 Success Rate", f"{success_rate:.1f}%", "Successful creations")
            
            self.console.print(table)
            self.console.print(Panel.fit(
                f"[green]✓[/green] Word integration completed successfully!\\n"
                f"[yellow]📝[/yellow] Detailed logs: logs/{self.log_filename}\\n"
                f"[blue]🎉[/blue] Semitic database now includes multilingual words!",
                title="🏆 Phase 2 Complete"
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
        integrator = SemiticWordsIntegrator()
        
        # Process words (remove limit for full processing)
        integrator.process_words(
            csv_path="sem_word.csv",
            batch_size=150,
            create_missing_roots=True,
            limit=None  # Remove this line or set to None for full processing
        )
        
        # Print statistics
        integrator.print_statistics()
        
        if RICH_AVAILABLE:
            integrator.console.print("\\n[bold green]🎉 Phase 2 Word Integration Complete![/bold green]")
            integrator.console.print("[blue]🚀 Next:[/blue] Your Semitic database is now fully integrated!")
        else:
            print("\\n🎉 Phase 2 Complete!")
            print("🚀 Your Semitic database is now fully integrated!")
            
    except Exception as e:
        print(f"❌ Integration failed: {e}")
        if integrator:
            integrator.logger.error(f"Integration failed: {e}")
            
    finally:
        if integrator:
            integrator.close()


if __name__ == "__main__":
    main()