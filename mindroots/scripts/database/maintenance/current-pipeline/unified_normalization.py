"""
Unified Arabic Text Normalization Module
=======================================

This module provides a single source of truth for Arabic text normalization across
the entire MindRoots pipeline, ensuring consistent text processing between:

1. **Hans Wehr Dictionary Ingestion** (addHansWehr.py)
   - Normalizes Hans Wehr dictionary entries before matching to Word nodes

2. **Word Node Normalization** (phase2-refactor/normalize_words.py)
   - Creates normalized properties on Lane's Lexicon Word nodes

3. **Corpus Item Lemma Normalization** (phase2-refactor/normalize_lemmas.py)
   - Normalizes Quranic corpus lemmas from Buckwalter transliteration

4. **Corpus Segment Rebuilding** (phase2-refactor/rebuild_segments.py)
   - Converts Buckwalter segments to Arabic and applies normalization layers

CORE NORMALIZATION RULES
========================

The normalization pipeline follows these rules in order:

1. **Unicode Normalization (NFKD)**
   - Decomposes composite characters (آ → ا + madda mark)
   - Essential for proper diacritic handling

2. **Diacritic Stripping**
   - Removes tashkeel: U+064B-U+064A (fatha, damma, kasra, etc.)
   - Removes extended marks: U+0670 (superscript alif)
   - Removes madda and hamza marks: U+0653-U+0655

3. **Alif Normalization** (automatic via NFKD + diacritic stripping)
   - أ (hamza above) → ا + hamza mark → ا (mark stripped)
   - إ (hamza below) → ا + hamza mark → ا (mark stripped)
   - آ (madda) → ا + madda mark → ا (mark stripped)
   - ٱ (wasla) → ا (explicit replacement, not decomposed by NFKD)

4. **Ya Normalization**
   - ى (alif maqsura) → ي (ya)

5. **Hamza Seat Normalization** (automatic via NFKD + diacritic stripping)
   - ؤ (waw hamza) → و + hamza mark → و (mark stripped)
   - ئ (ya hamza) → ي + hamza mark → ي (mark stripped)

6. **Feminine Marker Handling**
   - ة (ta marbuta) preserved in standard normalization
   - Conservative layer available for fallback matching (strips ة entirely)

BUCKWALTER CONVERSION
====================

Special preprocessing for Buckwalter transliteration symbols:
- A^ → آ (alif madda sequences)
- ^ → آ (stray madda symbols)
- # → ئ (hamza on ya seat)
- @ → removed entirely
- { → A (wasla alif → plain alif for camel-tools compatibility)

NORMALIZATION LAYERS
===================

The module provides multiple normalization layers for flexible matching:

1. **no_diacritics**: Strip diacritics only, preserve structure
   - Use for: Article-preserving matching (الكتاب remains الكتاب)

2. **normalized**: Full standard normalization
   - Use for: Standard corpus ↔ lexicon matching

3. **conservative**: Standard + feminine marker removal
   - Use for: Fallback matching when standard fails
   - Example: آزفة → ازف (handles orthographic variants)

USAGE EXAMPLES
=============

```python
from unified_normalization import normalize_arabic, create_normalization_layers

# Standard normalization
text = "الْكِتَابُ"
normalized = normalize_arabic(text)  # → "الكتاب"

# Multiple layers for flexible matching
layers = create_normalization_layers("آزفة")
# layers['no_diacritics'] → "آزفة"
# layers['normalized'] → "ازفة"
# layers['conservative'] → "ازف"

# Buckwalter conversion
from unified_normalization import buckwalter_to_arabic
arabic = buckwalter_to_arabic("A^zfp")  # → "آزفة"
```

CRITICAL IMPLEMENTATION NOTES
=============================

1. **NFKD First**: Always apply unicodedata.normalize('NFKD') before diacritic stripping
2. **Decomposition Dependency**: Alif and hamza seat normalization rely on NFKD decomposition
3. **Order Matters**: Strip diacritics BEFORE other transformations
4. **Wasla Exception**: ٱ (U+0671) must be explicitly replaced (not decomposed by NFKD)
5. **Camel-Tools Compatibility**: Preprocess special Buckwalter symbols before transliteration

This unified approach ensures all pipeline components use identical normalization logic,
eliminating inconsistencies that previously caused linking failures between corpus data
and lexical resources.
"""

import re
import unicodedata
from camel_tools.utils.charmap import CharMapper
from camel_tools.utils.transliterate import Transliterator

# Buckwalter to Arabic transliterator
bw2ar = Transliterator(CharMapper.builtin_mapper("bw2ar"))

def strip_diacritics(text):
    """
    Strip all diacritics including madda and hamza marks.

    Args:
        text (str): Arabic text with potential diacritics

    Returns:
        str: Text with all diacritics removed, None if input is None/empty
    """
    if not text:
        return None

    # Apply Unicode normalization first to decompose composite characters
    text = unicodedata.normalize('NFKD', text)

    # Strip diacritics including madda (U+0653) and hamza marks (U+0654-U+0655)
    diacritics_pattern = re.compile(r'[\u064B-\u0655\u0670]')
    text = diacritics_pattern.sub('', text)

    # Normalize waṣla alif to regular alif (not handled by NFKD)
    text = text.replace('ٱ', 'ا')  # U+0671 → U+0627

    return text

def normalize_arabic(text):
    """
    Apply standard Arabic normalization pipeline.

    Normalization steps:
    1. Strip diacritics (including madda and hamza marks)
    2. Normalize alifs (أ, إ, آ, ٱ → ا)
    3. Normalize ya (ى → ي)
    4. Keep feminine marker (ة) as-is
    5. Hamza seats normalized automatically by NFKD + diacritic stripping

    Args:
        text (str): Arabic text to normalize

    Returns:
        str: Normalized Arabic text, None if input is None/empty
    """
    if not text:
        return None

    # Strip diacritics first (includes alif normalization)
    text = strip_diacritics(text)

    # Normalize ya alif maqsura
    text = text.replace('ى', 'ي')

    # Note: Alif normalization (أ, إ, آ → ا) is handled automatically:
    # - NFKD decomposes آ → ا + madda mark
    # - NFKD decomposes أإ → ا + hamza marks
    # - Diacritic stripping removes the marks
    # - ٱ (waṣla alif) is handled explicitly in strip_diacritics()

    # Note: Hamza seat normalization (ؤئ → وي) also handled automatically:
    # - NFKD decomposes ؤ → و + hamza mark, ئ → ي + hamza mark
    # - Diacritic stripping removes hamza marks

    return text

def buckwalter_to_arabic(bw_text):
    """
    Convert Buckwalter transliteration to Arabic with preprocessing for special symbols.

    Handles special Buckwalter symbols before camel-tools processing:
    - ^ → آ (madda alif, but also handles A^ → آ sequences)
    - # → ئ (hamza on ya)
    - @ → remove entirely
    - { → A (waṣla alif → plain alif for camel-tools)

    Args:
        bw_text (str): Buckwalter transliteration string

    Returns:
        str: Arabic text, None if input is None/empty
    """
    if not bw_text:
        return None

    # Preprocess special symbols that camel-tools doesn't handle correctly
    # Handle alif madda cases
    bw_text = re.sub(r"A\^", "آ", bw_text)  # A^ sequence → آ
    bw_text = bw_text.replace("^", "آ")     # remaining ^ → آ
    bw_text = bw_text.replace("#", "}")     # hamza on ya → ئ (using } for camel-tools)
    bw_text = bw_text.replace("@", "")      # remove @ entirely
    bw_text = bw_text.replace("{", "A")     # waṣla alif → plain alif for camel-tools

    # Use camel-tools for main transliteration
    result = bw2ar.transliterate(bw_text)

    # Debug warning for any unresolved ^ characters
    if "^" in result:
        print(f"WARNING: Unresolved ^ in Buckwalter '{bw_text}' → result: '{result}'")

    return result

def create_normalization_layers(arabic_text):
    """
    Create all normalization layers for a given Arabic text.

    Returns a dictionary with the following keys:
    - no_diacritics: Diacritics stripped only
    - normalized: Standard normalization (alifs, ya, hamza seats)
    - conservative: Same as normalized but with ة removed entirely (fallback matching)

    Args:
        arabic_text (str): Source Arabic text

    Returns:
        dict: Dictionary with normalization layers, values are None if input is None/empty
    """
    if not arabic_text:
        return {
            'no_diacritics': None,
            'normalized': None,
            'conservative': None
        }

    no_diacritics = strip_diacritics(arabic_text)
    normalized = normalize_arabic(arabic_text)

    # Conservative layer: remove feminine markers entirely for fallback matching
    conservative = normalized.replace('ة', '') if normalized else None

    return {
        'no_diacritics': no_diacritics,
        'normalized': normalized,
        'conservative': conservative
    }