# Power BI Connection Guide for Chh-OLA Trip Analysis

## 📊 Connecting Power BI Desktop to Your Data

### Prerequisites
- Power BI Desktop installed (Download from [Microsoft Power BI](https://powerbi.microsoft.com))
- Completed ETL pipeline execution
- Query result CSV files in `/output/query_results/`

---

## Option 1: Import CSV Query Results (Recommended for Beginners)

### Step 1: Copy CSV Files to Local Machine
```bash
# From project directory
docker cp chh-ola-spark:/app/output/query_results ./query_results_local
```

### Step 2: Open Power BI Desktop
1. Click **Get Data** → **Text/CSV**
2. Navigate to the `query_results_local` folder
3. Select all CSV files you want to import
4. Click **Load**

### Step 3: Import Multiple Query Results
Import these key files:
- `city_revenue_analysis.csv` - City-wise metrics
- `peak_hours_analysis.csv` - Time-based patterns
- `payment_type_analysis.csv` - Payment preferences
- `distance_category_analysis.csv` - Trip distance insights
- `monthly_trends.csv` - Temporal trends

### Step 4: Data Modeling
1. Go to **Model View**
2. If relationships are needed, create them based on common fields
3. Most query results are already aggregated, so minimal modeling needed

---

## Option 2: Direct Parquet File Import (Advanced)

### Step 1: Copy Parquet Files
```bash
docker cp chh-ola-spark:/app/output/parquet/chh_ola_trips_processed ./parquet_local
```

### Step 2: Import in Power BI
1. Click **Get Data** → **More** → **File** → **Folder**
2. Select your `parquet_local` folder
3. Click **Combine & Transform Data**
4. Power BI will automatically detect Parquet format
5. Click **Load**

### Benefits of Parquet:
- Faster loading times
- Compressed file size
- All raw data available for custom analysis
- Preserves data types

---

## 📈 Creating Dashboards

### Dashboard 1: Revenue Overview

**Visualizations to Create:**

1. **City Revenue Card**
   - Visual: Stacked Bar Chart
   - Axis: `city`
   - Values: `total_revenue`
   - Sort by: `total_revenue` descending

2. **Total Revenue KPI**
   - Visual: Card
   - Fields: Sum of `total_revenue`
   - Format: Currency (₹)

3. **Trip Count by City**
   - Visual: Pie Chart
   - Legend: `city`
   - Values: `trip_count`

4. **Monthly Revenue Trend**
   - Visual: Line Chart
   - Axis: `month`
   - Values: `monthly_revenue`
   - Add trend line

### Dashboard 2: Operational Insights

1. **Peak Hours Heatmap**
   - Visual: Matrix or Heatmap
   - Rows: `hour`
   - Columns: `time_of_day`
   - Values: `trip_count`
   - Conditional formatting: Color scale

2. **Distance Category Distribution**
   - Visual: Donut Chart
   - Legend: `distance_category`
   - Values: `trip_count`

3. **Average Fare per KM by City**
   - Visual: Clustered Column Chart
   - Axis: `city`
   - Values: `avg_fare_per_km`

4. **Payment Type Breakdown**
   - Visual: Funnel Chart
   - Category: `payment_type`
   - Values: `percentage`

### Dashboard 3: Driver & Customer Analytics

1. **Top Drivers Performance**
   - Visual: Table
   - Columns: `driver_id`, `total_trips`, `total_revenue`, `avg_rating`
   - Sort by: `total_revenue`

2. **Vendor Comparison**
   - Visual: Clustered Bar Chart
   - Axis: `vendor_name`
   - Values: `total_revenue`, `avg_rating`

3. **Time of Day Analysis**
   - Visual: Stacked Column Chart
   - Axis: `time_of_day`
   - Values: `trip_count`, `total_revenue`

---

## 🎨 Design Best Practices

### Color Scheme
Use consistent colors across dashboards:
- **Primary**: #1F77B4 (Blue) - Revenue metrics
- **Secondary**: #FF7F0E (Orange) - Trip counts
- **Accent**: #2CA02C (Green) - Positive indicators
- **Warning**: #D62728 (Red) - Low performance

### Layout Tips
1. Place KPIs (Cards) at the top
2. Use consistent visual sizes
3. Add filters on the right sidebar
4. Group related visuals together
5. Use white space effectively

### Filters to Add
- **Date Range Slicer**: Filter by `pickup_time`
- **City Filter**: Multi-select dropdown
- **Payment Type Filter**: Checkbox list
- **Distance Category**: Radio buttons

---

## 🔄 Refresh Data

### Manual Refresh
1. Click **Home** → **Refresh**
2. Power BI reloads all data sources

### Scheduled Refresh (Power BI Service)
1. Publish to Power BI Service
2. Go to Dataset Settings
3. Configure Scheduled Refresh
4. Set frequency (daily/weekly)

---

## 📋 Sample Dashboard Measures (DAX)

Create these calculated measures for advanced analytics:

```dax
// Total Revenue
Total Revenue = SUM('city_revenue_analysis'[total_revenue])

// Average Fare
Average Fare = AVERAGE('city_revenue_analysis'[avg_revenue_per_trip])

// Trip Count
Total Trips = SUM('city_revenue_analysis'[trip_count])

// Revenue per Trip
Revenue per Trip = DIVIDE([Total Revenue], [Total Trips], 0)

// Peak Hour Indicator
Peak Hour = IF('peak_hours_analysis'[trip_count] > 500, "Peak", "Off-Peak")

// Distance Category Ranking
Distance Rank = RANKX(ALL('distance_category_analysis'), [trip_count],, DESC)
```

---

## 🎯 Key Insights to Highlight

Your dashboard should answer:

1. **Which city generates the most revenue?**
   - Use city revenue bar chart

2. **When is the peak demand time?**
   - Use hourly trip count line chart

3. **What's the preferred payment method?**
   - Use payment type pie chart

4. **How do trip distances vary?**
   - Use distance category distribution

5. **Which drivers perform best?**
   - Use top drivers table

6. **How does revenue trend monthly?**
   - Use monthly trend line chart

---

## 🐛 Troubleshooting

### Issue: CSV files not loading properly
**Solution**: Ensure CSV files have headers and use UTF-8 encoding

### Issue: Parquet files not recognized
**Solution**: Update Power BI Desktop to latest version (Nov 2023+)

### Issue: Date fields showing as text
**Solution**: Transform data type to Date in Power Query Editor

### Issue: Relationships not working
**Solution**: Most query results are pre-aggregated and don't need relationships

---

## 📸 Export and Share

1. **Export as PDF**
   - File → Export to PDF
   - Select pages to include

2. **Publish to Power BI Service**
   - Click **Publish** button
   - Sign in with Microsoft account
   - Share link with team

3. **Export Visualizations**
   - Click **...** on any visual
   - Export data → CSV or Excel

---

## 📚 Additional Resources

- [Power BI Documentation](https://docs.microsoft.com/power-bi/)
- [DAX Reference](https://dax.guide/)
- [Power BI Community](https://community.powerbi.com/)
- [Video Tutorials](https://www.youtube.com/powerbi)

---

## ✅ Dashboard Checklist

Before submitting your project, ensure:
- [ ] All CSV files imported successfully
- [ ] At least 3 dashboards created
- [ ] KPIs clearly visible at the top
- [ ] Filters are functional
- [ ] Color scheme is consistent
- [ ] Charts have proper titles and labels
- [ ] Data refreshes without errors
- [ ] Insights are clearly highlighted
- [ ] Dashboard is exported as PDF
- [ ] Screenshots included in report

---

**🎓 For Your Viva:**

Be prepared to explain:
1. Why you chose specific visualizations
2. How the data flows from CSV to Power BI
3. What insights the dashboards reveal
4. How Parquet format benefits the analysis
5. How you handled data quality issues

**Pro Tip**: Create a 1-page "Executive Summary" dashboard with only the top 4-5 KPIs for maximum impact!
