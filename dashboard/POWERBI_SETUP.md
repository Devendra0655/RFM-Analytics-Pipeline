# Power BI Dashboard Setup Guide
# File: dashboard/POWERBI_SETUP.md
# Layer 3: Business Intelligence Configuration

==============================================================================
 STEP 1: CONNECT TO MYSQL
==============================================================================

1. Open Power BI Desktop
2. Click: Home → Get Data → MySQL database
   (Install MySQL connector if prompted: https://dev.mysql.com/downloads/connector/net/)
3. Server: localhost
   Database: rfm_analytics
4. Enter your MySQL credentials
5. In the Navigator, select: vw_customer_segments
6. Click: Load (NOT Transform — the view is already clean)

==============================================================================
 STEP 2: VERIFY DATA TYPES IN POWER BI
==============================================================================

In the Data pane, confirm these field types:
  CustomerID    → Whole Number
  Recency       → Whole Number
  Frequency     → Whole Number
  Monetary      → Decimal Number
  R_Score       → Whole Number
  F_Score       → Whole Number
  M_Score       → Whole Number
  RFM_Score     → Whole Number
  Segment       → Text
  RevenueTier   → Text

==============================================================================
 STEP 3: CREATE DAX MEASURES
==============================================================================

Go to: Modeling → New Measure
Create each of the following:

--- KPI: Total Revenue ---
Total Revenue = 
SUMX(vw_customer_segments, vw_customer_segments[Monetary])

--- KPI: Total Customers ---
Total Customers = 
DISTINCTCOUNT(vw_customer_segments[CustomerID])

--- KPI: Average Order Value ---
Avg Customer Revenue = 
AVERAGE(vw_customer_segments[Monetary])

--- KPI: At-Risk Revenue ---
At Risk Revenue = 
CALCULATE(
    SUM(vw_customer_segments[Monetary]),
    vw_customer_segments[Segment] IN {"At Risk", "Cannot Lose Them", "Hibernating"}
)

--- % of Customers At Risk ---
% At Risk = 
DIVIDE(
    CALCULATE(COUNTROWS(vw_customer_segments), 
              vw_customer_segments[Segment] IN {"At Risk", "Cannot Lose Them", "Hibernating"}),
    COUNTROWS(vw_customer_segments),
    0
)

--- Segment Revenue Share ---
Segment Revenue % = 
DIVIDE(
    SUMX(vw_customer_segments, vw_customer_segments[Monetary]),
    CALCULATE(SUMX(ALL(vw_customer_segments), vw_customer_segments[Monetary])),
    0
)

==============================================================================
 STEP 4: BUILD THE VISUALS
==============================================================================

VISUAL 1: KPI CARDS (top of dashboard)
  Card 1 → Field: [Total Revenue]    | Format: Currency £, 0 decimal places
  Card 2 → Field: [Total Customers]  | Format: Whole number
  Card 3 → Field: [Avg Customer Revenue] | Format: Currency £
  Card 4 → Field: [At Risk Revenue]  | Format: Currency £ (color red)

----

VISUAL 2: DONUT CHART (segment breakdown)
  Legend:      Segment
  Values:      CustomerID (Count)
  
  Color Mapping (Format → Colors):
    Champions        → #2ECC71  (green)
    Loyal Customers  → #27AE60  (dark green)
    At Risk          → #E74C3C  (red)
    Cannot Lose Them → #C0392B  (dark red)
    Hibernating      → #95A5A6  (grey)
    Lost             → #7F8C8D  (dark grey)

----

VISUAL 3: SCATTER PLOT (Recency vs Monetary — the KEY visual)
  X-axis:      Recency    (title: "Days Since Last Purchase")
  Y-axis:      Monetary   (title: "Total Revenue £")
  Legend:      Segment    (to color-code by segment)
  Size:        Frequency  (larger bubble = more orders)
  
  Add analytics:
    - Constant line X-axis at 90 days (label: "3-Month Mark")
    - Constant line Y-axis at 1000 (label: "£1K Revenue")
  
  Quadrant interpretation (add as text boxes):
    Top-right  = Cannot Lose Them (HIGH value, HIGH recency days = danger!)
    Top-left   = Champions (HIGH value, LOW recency days = recently active)
    Bottom-right = At Risk (low value, not seen recently)
    Bottom-left  = Lost / Hibernating

----

VISUAL 4: CLUSTERED BAR CHART (Revenue by Segment)
  Y-axis: Segment
  X-axis: [Total Revenue] (sum of Monetary)
  Sort:   Descending by revenue
  Color:  Apply conditional color (green → red scale by revenue)

----

VISUAL 5: TABLE (At-Risk Customer Action List)
  Filter:    Segment IN (At Risk, Cannot Lose Them)
  Columns:   CustomerID, Recency, Frequency, Monetary, RFM_Score, RecommendedAction
  Sort:      Monetary descending
  Format:    Monetary as £ currency
  Add:       Conditional formatting on Monetary (data bars)

==============================================================================
 STEP 5: DASHBOARD FORMATTING TIPS
==============================================================================

Page Size: 1280 × 720 (Widescreen)

Color Theme (Dark Professional):
  Background:  #1E2A38
  Card bg:     #263545
  Text:        #FFFFFF
  Accent:      #00C4B4 (teal — matches your MediLocate palette!)

Font: Segoe UI (Power BI default, clean and readable)

Slicer to add:
  - Segment (multi-select dropdown)
  - RevenueTier (High / Mid / Low Value)

Title suggestions:
  Main:    "Customer Retention Intelligence Dashboard"
  Subtitle: "RFM Segmentation Analysis | Online Retail 2010–2011"

==============================================================================
 STEP 6: PUBLISH (Optional — for sharing)
==============================================================================

1. Create free Power BI account at app.powerbi.com
2. Home → Publish → My Workspace
3. Take a screenshot of the published dashboard for your GitHub README

==============================================================================
 IMPORTANT NOTE FOR GITHUB
==============================================================================

The .pbix file contains your MySQL connection string.
Before committing:
  - Go to: File → Options → Data Load → Clear Permissions
  - OR: Export a screenshot/PDF instead of the raw .pbix
  - Add to .gitignore: *.pbix (and share via Google Drive link in README)