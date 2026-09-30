# Query parameter handling

TRACE keeps the received query string as the lossless request representation
and exposes a decoded key/value map for analysis. The two fields serve
different purposes: use `url` when the original spelling or every repeated
value matters, and use `params` for convenient lookup of a single value by
key.

## Raw request URL

The collector records the URL path and query string separately. It does not
decode, reorder, or collapse query parameters. The flusher builds the event's
`url` column from that path and the original query string, preserving percent
escapes, pair order, blank values, repeated keys, and separators such as a
trailing `&`. A request with no query string has no `?` appended. When a POST
has no query string, the flusher instead synthesizes a readable URL from its
body parameters; that URL is sorted and is not a raw request target.

## `params` MAP semantics

The event's `params` field is stored as `MAP<STRING, STRING>`, which cannot
represent multiple values for one key. Parsing therefore applies these
deterministic rules:

- Percent escapes in keys and values are decoded. A literal `+` remains `+`;
  it is not treated as a space.
- A key without `=` and a key with an empty value (for example, `flag` and
  `empty=`) map to the empty string. Empty pairs between separators are
  ignored.
- If a key occurs more than once, the last occurrence wins in the map. This
  rule also applies when differently encoded keys decode to the same key.
- Unrecognized parameter names are kept; there is no allowlist.

The raw `url` retains all original occurrences and encodings even when the
map collapses duplicates. Consumers that need every occurrence must inspect
the query string in `url`, rather than relying on `params`.

The parser-to-Parquet behavior is pinned by
`flusher/src/raw_log_parser.rs::test_query_params_e2e_pixel_round_trip_preserves_raw_url`.
The collector's real-router request-to-JSONL behavior is pinned by
`collector/src/main.rs::contract_get_pixel_preserves_complex_query_verbatim`.
