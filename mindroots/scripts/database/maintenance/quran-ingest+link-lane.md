📜 Quran Corpus Ingestion & Linking Pipeline (Documented)

⸻

🔹 Phase 1 – Reset & Reindex the Corpus
	•	Problem: Legacy ingest created word boundary mismatches → extra/missing CorpusItem nodes (13 with no item_id).
	•	Fix:
	•	Deleted all CorpusItem nodes for corpus_id = 2.
	•	Reindex script built:
	•	Corpus → Surah → Ayah → CorpusItem hierarchy.
	•	Segment-level props (sX_form, sX_arabic, sX_lemma, sX_root, sX_tag, sX_features, etc.).
	•	form_concat for rendering full word.

⸻

🔹 Phase 2 – Normalize Segment Lemmas & Arabic Forms
	•	Problems hit:
	•	Buckwalter conversion left odd symbols:
	•	^ (long vowel marker, safe to strip).
	•	# (odd Buckwalter hamza/ya hybrid → mapped to ئ).
	•	sX_arabic_full sometimes contains spurious whitespaces → must be cleaned in a separate script.
	•	Fix:
	•	Created sX_lemma_norm (Buckwalter → Arabic, normalized).
	•	Ran cleanup to create sX_lemma_cleaned:
	•	Remove ^.
	•	Replace # → ئ.
	•	Next: additional cleanup pass on sX_arabic_full whitespace issues.

⸻

🔹 Phase 3 – Select Top-Level Lemma (lemma)
	•	Problem: Multiple segments → no consistent lemma.
	•	Fix logic:
	•	New top-level property: lemma.
	•	Script checks each segment s1–s7 in order:
	1.	Use sX_lemma_cleaned if present and that segment also has sX_root.
	2.	Else, fallback to sX_lemma_norm if that segment also has sX_root.
	•	Special case: 20:94:2 → dual roots → manually assign lemma from first root only (bny).

⸻

🔹 Phase 4 – Linking to Lane’s Lexicon
	•	Root matching:
	•	CorpusItem.root is no whitespace (e.g. بني).
	•	Root nodes store:
	•	arabic with hyphenated root (e.g. ب-ن-ي).
	•	r1, r2, r3 for individual radicals.
	•	Script must check both formats.
	•	Lemma matching:
	•	Use CorpusItem.lemma (from Phase 3).
	•	Compare against child Word nodes of the matched root:
	1.	Try w.arabic_no_diacritics (exact stripped match).
	2.	If no match, try w.arabic_normalized (hamza, alif, ya, ta marbuta normalization).
	•	If no match:
	•	Create a placeholder Word node under the root:
	•	generated = true.
	•	Carry over lemma as arabic.
	•	Link CorpusItem → Word.
	•	If no root found at all:
	•	Flag with ci.link_failed = true, ci.link_failed_reason = 'root_not_found'.

⸻

🔹 Phase 5 – Validation & QA
	•	Counts:
	•	CorpusItem count must equal TSV rows.
	•	All nodes should now have: root, lemma, form_concat.
	•	Linking coverage:
	•	Count: CorpusItems linked → Word.
	•	Count: CorpusItems flagged as failed.
	•	Manual review:
	•	Manually inspect failures (mostly orthographic variants or rare edge cases).

⸻

🔹 Phase 6 – Expansion
	•	Backfill Buckwalter transliteration onto all Word nodes in Lane’s lexicon.
	•	Handle particles layer separately (words without roots).
	•	Add QA checks for:
	•	Surah / Ayah completeness.
	•	Form concatenation rendering vs. Quran text.

⸻

✅ So the very next step once lemma_cleaned script finishes is:
	1.	Run the top-level lemma selection script (Phase 3).
	2.	Then we can rewrite the linking script (Phase 4) to match on root + lemma.

