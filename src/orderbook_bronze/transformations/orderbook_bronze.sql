-- The whole pipeline: Auto Loader appends every snapshot file, from every exchange, to one bronze table as it lands.
-- Each line is kept whole as `raw` (VARIANT, every level) and the top 5 levels a side are parsed into arrays of structs.
CREATE OR REFRESH STREAMING TABLE orderbook_bronze
COMMENT 'One-minute order book snapshots: the top 5 levels parsed, the full book kept as raw VARIANT'
AS SELECT raw:exchange::string AS exchange, raw:ticker::string AS ticker, raw:ts::timestamp AS ts,
  transform(slice(raw:bids::array<array<double>>, 1, 5), l -> named_struct('price', l[0], 'size', l[1])) AS bids,
  transform(slice(raw:asks::array<array<double>>, 1, 5), l -> named_struct('price', l[0], 'size', l[1])) AS asks,
  raw, _metadata.file_path AS source_file, current_timestamp() AS ingested_at
FROM STREAM read_files('${landing_path}', format => 'json', singleVariantColumn => 'raw');
