import csv
import pandas as pd
from collections import defaultdict
from camel_tools.morphology.database import MorphologyDB
from camel_tools.morphology.analyzer import Analyzer

# Load the built-in morphological database
db = MorphologyDB.builtin_db()

# Initialize the analyzer with the database
analyzer = Analyzer(db)

test_words = ['كتب', 'استخرج', 'مكتوب', 'مِفتاح']

for w in test_words:
    analyses = analyzer.analyze(w)
    print(f"Word: {w}")
    for a in analyses:
        print("  →", a.get('pattern', 'NA'))