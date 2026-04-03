-- =============================================================================
--  LAYER 2: BUSINESS LOGIC VIEW
--  File: sql/02_create_rfm_view.sql
--
--  This VIEW applies customer segmentation logic on top of raw RFM scores.
--  Power BI connects DIRECTLY to this view — it is the clean, business-ready
--  data source for all dashboards.
--
--  Segmentation Logic (based on R_Score and F_Score combinations):
--  ┌──────────────────┬──────────────────────────────────────────────────────┐
--  │ Segment          │ Business Meaning                                     │
--  ├──────────────────┼──────────────────────────────────────────────────────┤
--  │ Champions        │ Bought recently, buy often, spend the most           │
--  │ Loyal Customers  │ Buy regularly, responsive to promotions              │
--  │ Potential Loyals │ Recent customers with average frequency              │
--  │ Recent Customers │ Bought most recently, but not often                  │
--  │ Promising        │ Recent shoppers, haven't spent much yet              │
--  │ Need Attention   │ Above-average R,F,M but declining                    │
--  │ About to Sleep   │ Below average recency & frequency                    │
--  │ At Risk          │ Spent big, purchased often, but long time ago        │
--  │ Cannot Lose Them │ Made largest purchases, haven't returned in a while  │
--  │ Hibernating      │ Low recency, frequency, and monetary scores          │
--  │ Lost             │ Lowest scores across all metrics                     │
--  └──────────────────┴──────────────────────────────────────────────────────┘
-- =============================================================================

USE rfm_analytics;

DROP VIEW IF EXISTS vw_customer_segments;

CREATE VIEW vw_customer_segments AS
SELECT
    CustomerID,
    Recency,
    Frequency,
    Monetary,
    R_Score,
    F_Score,
    M_Score,
    RFM_Score,
    RFM_Segment,

    -- ── SEGMENT LABEL ──────────────────────────────────────────────────────
    CASE
        WHEN R_Score = 5 AND F_Score >= 4                      THEN 'Champions'
        WHEN R_Score >= 4 AND F_Score >= 3                     THEN 'Loyal Customers'
        WHEN R_Score >= 3 AND F_Score >= 3 AND M_Score >= 3    THEN 'Potential Loyalists'
        WHEN R_Score = 5 AND F_Score <= 2                      THEN 'Recent Customers'
        WHEN R_Score >= 4 AND F_Score <= 2                     THEN 'Promising'
        WHEN R_Score = 3 AND F_Score >= 3                      THEN 'Need Attention'
        WHEN R_Score <= 3 AND F_Score <= 3 AND M_Score >= 3    THEN 'About to Sleep'
        WHEN R_Score <= 2 AND F_Score >= 3                     THEN 'At Risk'
        WHEN R_Score <= 2 AND F_Score >= 4 AND M_Score >= 4    THEN 'Cannot Lose Them'
        WHEN R_Score = 2 AND F_Score <= 2                      THEN 'Hibernating'
        WHEN R_Score = 1 AND F_Score <= 2                      THEN 'Lost'
        ELSE 'Others'
    END AS Segment,

    -- ── SEGMENT PRIORITY (for sorting in Power BI) ─────────────────────────
    CASE
        WHEN R_Score = 5 AND F_Score >= 4                      THEN 1
        WHEN R_Score >= 4 AND F_Score >= 3                     THEN 2
        WHEN R_Score >= 3 AND F_Score >= 3 AND M_Score >= 3    THEN 3
        WHEN R_Score = 5 AND F_Score <= 2                      THEN 4
        WHEN R_Score >= 4 AND F_Score <= 2                     THEN 5
        WHEN R_Score = 3 AND F_Score >= 3                      THEN 6
        WHEN R_Score <= 3 AND F_Score <= 3 AND M_Score >= 3    THEN 7
        WHEN R_Score <= 2 AND F_Score >= 3                     THEN 8
        WHEN R_Score <= 2 AND F_Score >= 4 AND M_Score >= 4    THEN 9
        WHEN R_Score = 2 AND F_Score <= 2                      THEN 10
        WHEN R_Score = 1 AND F_Score <= 2                      THEN 11
        ELSE 12
    END AS SegmentPriority,

    -- ── BUSINESS ACTION FLAG ────────────────────────────────────────────────
    CASE
        WHEN R_Score <= 2 AND F_Score >= 3                     THEN 'Immediate Re-engagement Campaign'
        WHEN R_Score <= 2 AND F_Score >= 4 AND M_Score >= 4    THEN 'VIP Win-Back Offer'
        WHEN R_Score = 5 AND F_Score >= 4                      THEN 'Reward & Upsell'
        WHEN R_Score >= 4 AND F_Score >= 3                     THEN 'Loyalty Program'
        WHEN R_Score <= 3 AND F_Score <= 3 AND M_Score >= 3    THEN 'Email Re-activation'
        ELSE 'Standard Newsletter'
    END AS RecommendedAction,

    -- ── REVENUE TIER (for scatter plot color coding in Power BI) ───────────
    CASE
        WHEN Monetary >= 5000  THEN 'High Value'
        WHEN Monetary >= 1000  THEN 'Mid Value'
        ELSE 'Low Value'
    END AS RevenueTier

FROM rfm_scores;


-- =============================================================================
--  VALIDATION QUERIES — Run these after creating the view to verify results.
-- =============================================================================

-- 1. Segment distribution (Fixed)
SELECT
    Segment,
    COUNT(*)                                          AS CustomerCount,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS Percentage,
    ROUND(AVG(Monetary), 2)                           AS Avg_Monetary,
    ROUND(AVG(Recency), 0)                            AS Avg_Recency,
    ROUND(AVG(Frequency), 1)                          AS Avg_Frequency
FROM vw_customer_segments
GROUP BY Segment, SegmentPriority
ORDER BY SegmentPriority;


-- 2. Top 10 highest-value customers at risk
SELECT
    CustomerID,
    Segment,
    Recency,
    Frequency,
    CONCAT('£', FORMAT(Monetary, 2)) AS Revenue,
    RecommendedAction
FROM vw_customer_segments
WHERE Segment IN ('At Risk', 'Cannot Lose Them')
ORDER BY Monetary DESC
LIMIT 10;


-- 3. KPI Summary (feeds Power BI KPI cards)
SELECT
    COUNT(DISTINCT CustomerID)          AS Total_Customers,
    CONCAT('£', FORMAT(SUM(Monetary), 2)) AS Total_Revenue,
    ROUND(AVG(Frequency), 1)            AS Avg_Orders_Per_Customer,
    ROUND(AVG(Monetary), 2)             AS Avg_Customer_Revenue
FROM vw_customer_segments;