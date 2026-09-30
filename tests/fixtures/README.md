Five complete signal update sequences copied from inbox/done in filename order, retaining only public schema fields. No credentials or HTTP headers are copied.

dexscreener_recorded.json is a field-allowlisted recorded SAPIJIJU response (a later snapshot, not a historical valuation). dexscreener_offline.json contains SYNTHETIC $100K responses for the five tokens. Offline replay deliberately returns no mcap for each token’s first update, then these controlled responses. This demonstrates recovery from missing enrichment; it does not estimate historical acceptance. Tokens without a fixture stay mcap_unknown.

`filter_v4_golden.json` freezes 1,882 pre-addon verdict/reason pairs from
`inbox/done`, both with original mcap and with mcap fixed at $200,000.
`filter_v4_inputs.jsonl` contains those public signal rows in manifest order,
copied byte-for-byte (excluding line separators) and verified against every
manifest SHA-256. The dedicated compatibility test uses these portable inputs,
not a live inbox or the current `_baseline` implementation as its oracle.
Expected answers are unchanged. Only documented mcap/chain reason aliases are
mapped; flags are additive. This is a frozen corpus, not all future inbox rows.
