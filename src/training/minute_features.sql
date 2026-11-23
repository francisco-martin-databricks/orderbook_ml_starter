-- The model's inputs, computed from bronze when the view is read (nothing runs to keep it up to date).
-- Each driver uses only the snapshot at ts. The label is the mid price's move over the next 5 minutes.
CREATE OR REPLACE VIEW minute_features AS
WITH book AS (
  SELECT exchange, ticker, ts, (bids[0].price + asks[0].price) / 2 AS mid,
         (asks[0].price - bids[0].price) / ((bids[0].price + asks[0].price) / 2) * 1e4 AS spread_bps,
         aggregate(bids, 0D, (acc, l) -> acc + l.size) AS bid_depth_5,
         aggregate(asks, 0D, (acc, l) -> acc + l.size) AS ask_depth_5
  FROM orderbook_bronze)
SELECT exchange, ticker, ts, spread_bps,
       (bid_depth_5 - ask_depth_5) / (bid_depth_5 + ask_depth_5) AS order_imbalance,
       CASE WHEN LEAD(ts, 5) OVER w = ts + INTERVAL 5 MINUTES
            THEN (LEAD(mid, 5) OVER w / mid - 1) * 1e4 END AS next_5min_return_bps
FROM book
WINDOW w AS (PARTITION BY exchange, ticker ORDER BY ts)
