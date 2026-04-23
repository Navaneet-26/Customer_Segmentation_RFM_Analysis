-- NOTE:
-- This project uses anonymized (dummy) project IDs and dataset names.
-- Original Power BI report and BigQuery connections are not shared 
-- to protect credentials and cloud resources.
-- This script is intended to demonstrate the RFM analysis logic only. 

-- Step 1 : Append All Monthly Sales Data 

CREATE OR REPLACE TABLE `dummy-XXXX.sales.sales_2025` AS 
SELECT * FROM `dummy-XXXX.sales.202501`
UNION ALL SELECT * FROM `dummy-XXXX.sales.202502`
UNION ALL SELECT * FROM `dummy-XXXX.sales.202503`
UNION ALL SELECT * FROM `dummy-XXXX.sales.202504`
UNION ALL SELECT * FROM `dummy-XXXX.sales.202505`
UNION ALL SELECT * FROM `dummy-XXXX.sales.202506`
UNION ALL SELECT * FROM `dummy-XXXX.sales.202507`
UNION ALL SELECT * FROM `dummy-XXXX.sales.202508`
UNION ALL SELECT * FROM `dummy-XXXX.sales.202509`
UNION ALL SELECT * FROM `dummy-XXXX.sales.202510`
UNION ALL SELECT * FROM `dummy-XXXX.sales.202511`
UNION ALL SELECT * FROM `dummy-XXXX.sales.202512`;

-- Step 2 : Calculate recency, frequency, monetary, r, f, m ranks
-- Combines views with CTEs

CREATE OR REPLACE VIEW `dummy-XXXX.sales.rfm_metrics` AS

WITH 
current_date AS( 
  SELECT DATE('2026-01-01') AS analyst_date -- todays' date
),

rfm AS (
  SELECT CustomerID,
  MAX(OrderDate) AS last_order_date,
  date_diff((SELECT analyst_date FROM current_date ), MAX(OrderDate), DAY) as recency,
  COUNT(*) AS frequency,
  SUM(OrderValue)*100 AS monetary
  FROM `dummy-XXXX.sales.sales_2025` 
  GROUP BY CustomerID
)

SELECT 
  rfm.*,
  ROW_NUMBER() OVER(ORDER BY rfm.recency DESC) AS r_rank,
  ROW_NUMBER() OVER(ORDER BY rfm.frequency) AS f_rank,
  ROW_NUMBER() OVER(ORDER BY rfm.monetary) AS m_rank
FROM rfm;

-- Step 3 : Assing deciles (10 - best, 1- worst)

CREATE OR REPLACE VIEW `dummy-XXXX.sales.rfm_scores` AS 
SELECT 
  *,
  NTILE(10) OVER (ORDER BY r_rank) AS r_score,
  NTILE(10) OVER (ORDER BY f_rank) AS f_score,
  NTILE(10) OVER (ORDER BY m_rank) AS m_score
FROM `dummy-XXXX.sales.rfm_metrics`;

-- Step 4 : Total Score

CREATE OR REPLACE VIEW `dummy-XXXX.sales.rfm_total_scores`
AS
SELECT 
  CustomerID,
  recency,
  frequency,
  monetary,
  r_score,
  f_score,
  m_score,
  (r_score + f_score + m_score) AS rfm_total_score
FROM `dummy-XXXX.sales.rfm_scores`
order by rfm_total_score DESC;

-- Step 5 BI ready rfm segments table

CREATE OR REPLACE TABLE `dummy-XXXX.sales.rfm_segments_final`
AS
SELECT
  *,
CASE 
  -- Best Customers
  WHEN r_score >= 9 AND f_score >= 9 AND m_score >= 9 THEN 'Champions'

  -- Very Valuable & Loyal
  WHEN r_score >= 8 AND f_score >= 8 AND m_score >= 7 THEN 'Loyal Customers'

  -- High Spenders (even if not frequent)
  WHEN m_score >= 9 AND r_score >= 6 THEN 'Big Spenders'

  -- Recent but low frequency (new users)
  WHEN r_score >= 8 AND f_score <= 4 THEN 'New Customers'

  -- Growing customers (potential to become loyal)
  WHEN r_score >= 7 AND f_score BETWEEN 5 AND 7 THEN 'Potential Loyalists'

  -- Moderate activity
  WHEN r_score >= 5 AND f_score >= 5 THEN 'Active Customers'

  -- Losing engagement
  WHEN r_score <= 4 AND f_score >= 7 THEN 'At Risk'

  -- Previously valuable but now inactive
  WHEN r_score <= 3 AND m_score >= 7 THEN 'Hibernating High Value'

  -- Almost lost
  WHEN r_score <= 3 AND f_score <= 3 THEN 'Lost Customers'

  -- Everything else
  ELSE 'Low Value Customers'
END 
  AS rfm_segment
FROM `dummy-XXXX.sales.rfm_total_scores`
ORDER BY rfm_total_score DESC


























