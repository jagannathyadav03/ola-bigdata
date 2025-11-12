#!/usr/bin/env python3
"""
generate_data.py
================
Generates realistic Chh-OLA cab trip data for Big Data analysis.

This script creates 10,000 synthetic trip records with realistic patterns:
- Multiple Indian cities
- Various payment methods
- Time-based trip patterns
- Driver and rider IDs
- Fare calculations based on distance
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

def generate_chh_ola_data(num_records=10000, output_path='../data/chh_ola_trips.csv'):
    """
    Generate synthetic Chh-OLA cab trip data.
    
    Parameters:
    -----------
    num_records : int
        Number of trip records to generate
    output_path : str
        Path where CSV file will be saved
    """
    
    print(f"🚀 Starting data generation for {num_records} trip records...")
    
    # Set random seed for reproducibility
    np.random.seed(42)
    random.seed(42)
    
    # Define data parameters
    cities = ['Mumbai', 'Delhi', 'Bangalore', 'Hyderabad', 'Chennai', 
              'Kolkata', 'Pune', 'Ahmedabad']
    payment_types = ['Credit Card', 'Cash', 'UPI', 'Wallet', 'Free Ride']
    rate_codes = {
        1: 'Standard',
        2: 'Airport',
        3: 'Connaught Place',
        4: 'Noida',
        5: 'Negotiated Fare',
        6: 'Pooled Ride'
    }
    
    # Generate base data
    print("📝 Generating trip IDs and identifiers...")
    data = {
        'trip_id': [f'TRP{str(i).zfill(8)}' for i in range(1, num_records + 1)],
        'driver_id': [f'DRV{str(random.randint(1, 2000)).zfill(6)}' for _ in range(num_records)],
        'rider_id': [f'RDR{str(random.randint(1, 5000)).zfill(6)}' for _ in range(num_records)],
        'vendor_id': [random.choice([1, 2]) for _ in range(num_records)],
    }
    
    # Location and city data
    print("📍 Generating location and city data...")
    data['city'] = [random.choice(cities) for _ in range(num_records)]
    data['pickup_loc'] = [random.randint(1, 100) for _ in range(num_records)]
    data['drop_loc'] = [random.randint(1, 100) for _ in range(num_records)]
    
    # Trip characteristics
    print("🚗 Calculating trip characteristics...")
    # Distance follows exponential distribution (most trips are short)
    data['distance_km'] = np.round(np.random.exponential(8, num_records) + 1, 2)
    
    # Generate timestamps (one year of data)
    start_date = datetime(2024, 1, 1)
    data['pickup_time'] = [
        start_date + timedelta(
            days=random.randint(0, 365),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        ) for _ in range(num_records)
    ]
    
    # Passenger count (most trips have 1-2 passengers)
    data['num_passengers'] = [
        random.choices([1, 2, 3, 4, 5, 6], 
                      weights=[50, 25, 15, 7, 2, 1])[0] 
        for _ in range(num_records)
    ]
    
    # Rate code (most are standard)
    data['rate_code'] = [
        random.choices([1, 2, 3, 4, 5, 6], 
                      weights=[60, 15, 10, 8, 5, 2])[0] 
        for _ in range(num_records)
    ]
    
    # Payment method
    data['payment_type'] = [random.choice(payment_types) for _ in range(num_records)]
    
    # Calculate dependent fields
    print("💰 Calculating fares and timings...")
    
    # Drop time = pickup time + travel time (based on distance)
    data['drop_time'] = [
        data['pickup_time'][i] + timedelta(
            minutes=int(data['distance_km'][i] * 3 + random.randint(5, 15))
        ) 
        for i in range(num_records)
    ]
    
    # Fare calculation: Base fare (40) + per km rate (12) + surge/discount
    data['fare_amount'] = [
        round(40 + (data['distance_km'][i] * 12) + random.uniform(-20, 30), 2)
        for i in range(num_records)
    ]
    
    # Driver tip (70% of trips have tips)
    data['driver_tip'] = [
        round(random.uniform(0, data['fare_amount'][i] * 0.15), 2) 
        if random.random() > 0.3 else 0.0
        for i in range(num_records)
    ]
    
    # Toll amount (30% of trips have tolls)
    data['toll_amt'] = [
        round(random.uniform(10, 50), 2) 
        if random.random() > 0.7 else 0.0
        for i in range(num_records)
    ]
    
    # MTA tax (5% of fare)
    data['mta_tax'] = [
        round(data['fare_amount'][i] * 0.05, 2) 
        for i in range(num_records)
    ]
    
    # Total amount
    data['total_amount'] = [
        round(
            data['fare_amount'][i] + 
            data['driver_tip'][i] + 
            data['toll_amt'][i] + 
            data['mta_tax'][i], 
            2
        )
        for i in range(num_records)
    ]
    
    # Rating (90% of trips have ratings)
    data['rating'] = [
        round(random.uniform(3.0, 5.0), 1) 
        if random.random() > 0.1 else None
        for _ in range(num_records)
    ]
    
    # Stored flag (95% immediately stored)
    data['stored_flag'] = [
        random.choices(['Y', 'N'], weights=[95, 5])[0] 
        for _ in range(num_records)
    ]
    
    # Create DataFrame
    print("📊 Creating DataFrame...")
    df = pd.DataFrame(data)
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save to CSV
    print(f"💾 Saving to {output_path}...")
    df.to_csv(output_path, index=False)
    
    # Print summary statistics
    print("\n" + "="*60)
    print("✅ DATA GENERATION COMPLETE!")
    print("="*60)
    print(f"📈 Total records: {len(df):,}")
    print(f"📅 Date range: {df['pickup_time'].min()} to {df['pickup_time'].max()}")
    print(f"🏙️  Cities covered: {df['city'].nunique()}")
    print(f"👨‍✈️  Unique drivers: {df['driver_id'].nunique()}")
    print(f"👤 Unique riders: {df['rider_id'].nunique()}")
    print(f"\n💰 Revenue Statistics:")
    print(f"   Total revenue: ₹{df['total_amount'].sum():,.2f}")
    print(f"   Average fare: ₹{df['fare_amount'].mean():,.2f}")
    print(f"   Average trip distance: {df['distance_km'].mean():.2f} km")
    print(f"\n🏙️  City-wise Trip Distribution:")
    print(df['city'].value_counts().to_string())
    print("\n" + "="*60)
    
    return df

if __name__ == "__main__":
    # Generate the dataset
    df = generate_chh_ola_data(
        num_records=10000,
        output_path='../data/chh_ola_trips.csv'
    )
    
    print("\n🎉 Dataset ready for Big Data analysis!")
    print("👉 Next step: Run etl_process.py to convert to Parquet format")
