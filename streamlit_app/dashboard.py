import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="Chh-OLA Big Data Analytics",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main {
        padding-top: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
    }
    h1 {
        color: #1F77B4;
        text-align: center;
    }
    </style>
    """, unsafe_allow_html=True)

# ============================================================================
# LOAD DATA
# ============================================================================

@st.cache_data
def load_data():
    """Load all CSV files from query results"""
    data_path = os.path.join(os.path.dirname(__file__), 'data')
    
    try:
        city_data = pd.read_csv(f'{data_path}/city_revenue_analysis.csv')
        hours_data = pd.read_csv(f'{data_path}/peak_hours_analysis.csv')
        payment_data = pd.read_csv(f'{data_path}/payment_type_analysis.csv')
        distance_data = pd.read_csv(f'{data_path}/distance_category_analysis.csv')
        drivers_data = pd.read_csv(f'{data_path}/top_drivers.csv')
        monthly_data = pd.read_csv(f'{data_path}/monthly_trends.csv')
        time_data = pd.read_csv(f'{data_path}/time_of_day_analysis.csv')
        rate_data = pd.read_csv(f'{data_path}/rate_code_analysis.csv')
        vendor_data = pd.read_csv(f'{data_path}/vendor_comparison.csv')
        fare_data = pd.read_csv(f'{data_path}/fare_per_km_analysis.csv')
        summary = pd.read_csv(f'{data_path}/business_summary.csv')
        
        return {
            'city': city_data,
            'hours': hours_data,
            'payment': payment_data,
            'distance': distance_data,
            'drivers': drivers_data,
            'monthly': monthly_data,
            'time': time_data,
            'rate': rate_data,
            'vendor': vendor_data,
            'fare': fare_data,
            'summary': summary
        }
    except FileNotFoundError as e:
        st.error(f"❌ Data file not found: {e}")
        st.info("📁 Make sure CSV files are in `streamlit_app/data/` folder")
        return None

# Load data
data = load_data()

if data is None:
    st.stop()

# ============================================================================
# HEADER
# ============================================================================

st.title("🚕 Chh-OLA Cabs Big Data Analytics")
st.markdown("### Interactive Dashboard | PySpark + Parquet + Streamlit")
st.markdown("---")

# ============================================================================
# SIDEBAR - FILTERS
# ============================================================================

with st.sidebar:
    st.header("📊 Dashboard Controls")
    st.markdown("---")
    
    view_type = st.radio(
        "Select View:",
        ["📈 Dashboard", "📋 Raw Data", "ℹ️ About"]
    )
    
    st.markdown("---")
    st.subheader("Big Data Concepts")
    st.markdown("""
    ✅ Distributed Processing  
    ✅ Lazy Evaluation  
    ✅ Partitioning  
    ✅ Caching  
    ✅ Columnar Storage (Parquet)  
    ✅ Spark SQL  
    ✅ Data Locality  
    ✅ Docker Containerization  
    """)
    
    st.markdown("---")
    st.markdown("**Built with:**")
    st.markdown("• Apache Spark 3.5.0")
    st.markdown("• Python 3.10")
    st.markdown("• Streamlit")
    st.markdown("• Plotly")

# ============================================================================
# PAGE 1: DASHBOARD
# ============================================================================

if view_type == "📈 Dashboard":
    
    # ========== KPI CARDS ==========
    st.subheader("📊 Executive Summary")
    
    col1, col2, col3, col4 = st.columns(4)
    
    summary = data['summary']
    
    # FIXED: Remove .iloc[0], use float() conversion
    with col1:
        st.metric(
            label="Total Revenue (₹)",
            value=f"₹{float(summary['total_revenue'].values[0]):,.0f}",
            delta=None
        )

    with col2:
        st.metric(
            label="Total Trips",
            value=f"{int(float(summary['total_trips'].values[0])):,}",
            delta=None
        )

    with col3:
        st.metric(
            label="Avg Fare (₹)",
            value=f"₹{float(summary['avg_fare'].values[0]):.2f}",
            delta=None
        )

    with col4:
        st.metric(
            label="Avg Rating",
            value=f"{float(summary['avg_rating'].values[0]):.1f} ⭐",
            delta=None
        )

    
    st.markdown("---")
    
    # ========== TABS FOR DIFFERENT DASHBOARDS ==========
    tab1, tab2, tab3 = st.tabs([
        "💰 Revenue Overview",
        "🚗 Operational Insights",
        "👨‍✈️ Driver Performance"
    ])
    
    # ========== TAB 1: REVENUE OVERVIEW ==========
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("💰 City-wise Revenue")
            fig = px.bar(
                data['city'],
                x='city',
                y='total_revenue',
                color='total_revenue',
                color_continuous_scale='Blues',
                labels={'total_revenue': 'Revenue (₹)', 'city': 'City'},
                title="Revenue by City",
                text='total_revenue'
            )
            fig.update_traces(texttemplate='₹%{text:,.0f}', textposition='outside')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("🚕 Trip Count Distribution")
            fig = px.pie(
                data['city'],
                values='trip_count',
                names='city',
                title="Trips by City",
                hole=0.3
            )
            st.plotly_chart(fig, use_container_width=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📅 Monthly Revenue Trend")
            fig = px.line(
                data['monthly'],
                x='month',
                y='monthly_revenue',
                markers=True,
                title="Revenue Over Time",
                labels={'monthly_revenue': 'Revenue (₹)', 'month': 'Month'}
            )
            fig.update_traces(line_color='#1F77B4', marker_size=8)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("💳 Payment Type Distribution")
            fig = px.pie(
                data['payment'],
                values='percentage',
                names='payment_type',
                title="Payment Methods Used",
                hole=0.3
            )
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        st.subheader("📋 Detailed City Analysis")
        st.dataframe(
            data['city'].sort_values('total_revenue', ascending=False),
            use_container_width=True,
            hide_index=True
        )
    
    # ========== TAB 2: OPERATIONAL INSIGHTS ==========
    with tab2:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("⏰ Peak Demand Hours")
            fig = px.bar(
                data['hours'],
                x='hour',
                y='trip_count',
                color='trip_count',
                color_continuous_scale='Oranges',
                title="Trips by Hour (24H)",
                labels={'hour': 'Hour of Day', 'trip_count': 'Trips'},
                text='trip_count'
            )
            fig.update_traces(textposition='outside')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("🌅 Time of Day Analysis")
            time_order = ['Morning', 'Afternoon', 'Evening', 'Night']
            time_sorted = data['time'].sort_values('time_of_day',
                                                    key=lambda x: x.map({v: i for i, v in enumerate(time_order)}))
            fig = px.bar(
                time_sorted,
                x='time_of_day',
                y='trip_count',
                color='trip_count',
                title="Trips by Time Period",
                labels={'time_of_day': 'Time Period', 'trip_count': 'Trips'},
                category_orders={'time_of_day': time_order},
                text='trip_count'
            )
            fig.update_traces(textposition='outside')
            st.plotly_chart(fig, use_container_width=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📏 Trip Distance Distribution")
            fig = px.pie(
                data['distance'],
                values='trip_count',
                names='distance_category',
                title="Trip Distance Types",
                hole=0.3
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("💰 Fare by Distance Category")
            fig = px.bar(
                data['distance'],
                x='distance_category',
                y='avg_fare',
                color='avg_fare',
                color_continuous_scale='Viridis',
                title="Average Fare by Distance",
                labels={'distance_category': 'Distance Category', 'avg_fare': 'Avg Fare (₹)'},
                text='avg_fare'
            )
            fig.update_traces(texttemplate='₹%{text:.0f}', textposition='outside')
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        st.subheader("📊 Distance Category Details")
        st.dataframe(data['distance'], use_container_width=True, hide_index=True)
    
    # ========== TAB 3: DRIVER PERFORMANCE ==========
    with tab3:
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.subheader("👨‍✈️ Top 15 Drivers by Revenue")
            fig = px.bar(
                data['drivers'].head(15),
                x='total_revenue',
                y='driver_id',
                orientation='h',
                color='total_revenue',
                color_continuous_scale='Greens',
                title="Top Performing Drivers",
                labels={'driver_id': 'Driver ID', 'total_revenue': 'Revenue (₹)'},
                text='total_revenue'
            )
            fig.update_traces(texttemplate='₹%{text:,.0f}', textposition='outside')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("📊 Driver Metrics")
            st.metric("Avg Driver Rating", f"{float(data['drivers']['avg_rating'].mean()):.1f}/5.0")
            st.metric("Drivers Analyzed", f"{len(data['drivers'])}")
            st.metric("Avg Revenue/Driver", f"₹{float(data['drivers']['total_revenue'].mean()):,.0f}")
        
        st.markdown("---")
        st.subheader("🏢 Vendor Performance Comparison")
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                data['vendor'],
                x='vendor_name',
                y='total_revenue',
                color='total_revenue',
                title="Vendor Revenue Comparison",
                labels={'vendor_name': 'Vendor', 'total_revenue': 'Revenue (₹)'},
                text='total_revenue'
            )
            fig.update_traces(texttemplate='₹%{text:,.0f}', textposition='outside')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(
                data['vendor'],
                x='vendor_name',
                y='avg_rating',
                color='avg_rating',
                title="Vendor Ratings",
                labels={'vendor_name': 'Vendor', 'avg_rating': 'Avg Rating'},
                text='avg_rating',
                color_continuous_scale='RdYlGn'
            )
            fig.update_traces(texttemplate='%{text:.2f}', textposition='outside')
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        st.subheader("📋 Top 20 Drivers Detailed Table")
        st.dataframe(
            data['drivers'].head(20)[['driver_id', 'total_trips', 'total_revenue', 'avg_rating']],
            use_container_width=True,
            hide_index=True
        )

# ============================================================================
# PAGE 2: RAW DATA
# ============================================================================

elif view_type == "📋 Raw Data":
    st.header("📋 Raw Data Explorer")
    
    data_source = st.selectbox(
        "Select Dataset:",
        [
            "City Revenue",
            "Peak Hours",
            "Payment Types",
            "Distance Categories",
            "Top Drivers",
            "Monthly Trends",
            "Time of Day",
            "Rate Codes",
            "Vendor Comparison",
            "Fare per KM",
            "Business Summary"
        ]
    )
    
    data_map = {
        "City Revenue": data['city'],
        "Peak Hours": data['hours'],
        "Payment Types": data['payment'],
        "Distance Categories": data['distance'],
        "Top Drivers": data['drivers'],
        "Monthly Trends": data['monthly'],
        "Time of Day": data['time'],
        "Rate Codes": data['rate'],
        "Vendor Comparison": data['vendor'],
        "Fare per KM": data['fare'],
        "Business Summary": data['summary']
    }
    
    df = data_map[data_source]
    
    st.markdown(f"### {data_source} ({len(df)} records)")
    st.dataframe(df, use_container_width=True, height=500)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"{data_source.lower().replace(' ', '_')}.csv",
            mime="text/csv"
        )
    
    with col2:
        st.info(f"📊 Shape: {df.shape[0]} rows × {df.shape[1]} columns")
    
    with col3:
        st.success(f"✅ Data loaded successfully")

# ============================================================================
# PAGE 3: ABOUT
# ============================================================================

else:  # About page
    st.header("ℹ️ About This Project")
    
    st.markdown("""
    ## Chh-OLA Cabs Big Data Analysis
    
    ### Project Overview
    This is an **end-semester Big Data Analysis project** demonstrating:
    - **Data Processing**: Apache Spark (PySpark)
    - **Storage**: Parquet Columnar Format
    - **Visualization**: Streamlit Web Dashboard
    - **Deployment**: Streamlit Cloud
    
    ### Dataset
    - **Records**: 10,000 trip records
    - **Cities**: 8 major Indian cities
    - **Attributes**: 20 original + 8 derived features
    - **Date Range**: Jan-Dec 2024
    
    ### Big Data Concepts Demonstrated
    1. **Distributed Processing** - Parallel execution across cores
    2. **Lazy Evaluation** - DAG optimization before execution
    3. **Partitioning** - Data distribution by city (8 partitions)
    4. **Caching** - In-memory storage for iterative queries
    5. **Columnar Storage** - Parquet format (4x compression)
    6. **Spark SQL** - 10 analytical queries
    7. **Data Locality** - Partition pruning for optimization
    8. **Containerization** - Docker deployment
    
    ### Key Metrics
    """)
    
    col1, col2 = st.columns(2)
    
    summary = data['summary']
    
    # FIXED: Convert to float/int before formatting
    with col1:
        st.markdown(f"""
        - **Total Revenue**: ₹{float(summary['total_revenue'].values[0]):,.0f}
        - **Total Trips**: {int(float(summary['total_trips'].values[0])):,}
        - **Avg Fare**: ₹{float(summary['avg_fare'].values[0]):.2f}
        - **Avg Distance**: {float(summary['avg_distance'].values[0]):.2f} km
        """)

    with col2:
        st.markdown(f"""
        - **Avg Rating**: {float(summary['avg_rating'].values[0]):.1f}/5.0
        - **Active Drivers**: {int(float(summary['active_drivers'].values[0])):,}
        - **Active Riders**: {int(float(summary['active_riders'].values[0])):,}
        - **Avg Duration**: {float(summary['avg_duration'].values[0]):.1f} min
        """)

    
    st.markdown("---")
    st.markdown("""
    ### Technology Stack
    - **Backend**: Apache Spark 3.5.0 (PySpark)
    - **Storage**: Parquet (columnar, compressed)
    - **Container**: Docker
    - **Frontend**: Streamlit
    - **Visualization**: Plotly
    - **Language**: Python 3.10
    - **Deployment**: Streamlit Cloud
    
    ### Project Structure
    ```
    uber_trip_analysis/
    ├── docker/              # Docker configuration
    ├── scripts/             # PySpark ETL scripts
    ├── output_local/        # Generated CSV files
    ├── streamlit_app/       # This dashboard
    ├── docs/                # Documentation
    └── README.md            # Project guide
    ```
    
    ### How to Run Locally
    ```bash
    # Install dependencies
    pip install -r requirements.txt
    
    # Run dashboard
    streamlit run dashboard.py
    
    # Access at http://localhost:8501
    ```
    
    ### For More Information
    See the project README and documentation files.
    """)
    
    st.markdown("---")
    st.markdown("""
    **Project Submitted**: November 2025  
    **Course**: Big Data Analysis  
    **Submitted by**: [Your Name]
    """)

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown("""
    <div style='text-align: center'>
    <p>🚕 Chh-OLA Big Data Analytics | Built with ❤️ using Streamlit</p>
    <p><small>Apache Spark • PySpark • Parquet • Docker • Streamlit</small></p>
    </div>
    """, unsafe_allow_html=True)