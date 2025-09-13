"""
Unified normalization module for consistent text processing across all pipeline scripts.

This module provides a single source of truth for Arabic text normalization,
ensuring consistency between:
- Hans Wehr ingestion
- Word node normalization
- CorpusItem lemma normalization
- CorpusItem segment rebuilding

Core normalization rules:
1. Diacritic stripping (U+064B–U+0655, U+0670) with NFKD normalization
2. Alif normalization (أ, إ, آ, ٱ → ا)
3. Ya normalization (ى → ي)
4. Feminine marker handling (keep in main, strip in conservative fallback)
5. Hamza seat normalization (handled by NFKD + diacritic stripping)
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