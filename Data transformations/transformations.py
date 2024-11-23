import pandas as pd
import numpy as np
import datetime


pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', None)  # Show all rows (caution: can slow output for large datasets)

path = r'path\to\your\csv\here'
global cust
cust = pd.read_csv(path)

path1 = r'path\to\your\csv\here'
global cust_supp
cust_supp = pd.read_csv(path1)

path2 = r'path\to\your\csv\here'
global marketing
marketing = pd.read_csv(path2)

path3 = r'path\to\your\csv\here'
global sales
sales = pd.read_csv(path3)



# ////////////////////  CUSTOMER DATA   \\\\\\\\\\\\\\\\\\\\\\\\

cust['date_of_birth'] = pd.to_datetime(cust['date_of_birth'], format='%d/%m/%Y', errors='coerce')
curr_date = pd.to_datetime(datetime.date.today())

# Calculate 'age' in years
cust['age'] = (curr_date - cust['date_of_birth']).dt.days // 365

# Calculate 'ltd' (Customer Lifetime Duration) in years
cust['created_at'] = pd.to_datetime(cust['created_at'], format='%d/%m/%Y', errors='coerce')
cust['ltd'] = (curr_date - cust['created_at']).dt.days // 365

# Calculate 'time_since_last_update' in years
cust['last_updated_at'] = pd.to_datetime(cust['last_updated_at'], format='%d/%m/%Y', errors='coerce')
cust['time_since_last_update'] = (curr_date - cust['last_updated_at']).dt.days // 365

# Create a 'full_name' column
cust['full_name'] = cust['first_name'] + " " + cust['last_name']

# Create 'full_address' column
cust['full_address'] = cust['address'] + ', ' + cust['city'] + ', ' + cust['state'] + ', ' + cust['country']

# ////////////////////////  SALES DATA  \\\\\\\\\\\\\\\\\\\\\\\\\\

# Total and average sales grouped by customer
sales_summary = sales.groupby('customer_id').agg(
    total_sales=('total_price', 'sum'),
    avg_sales=('total_price', 'mean')
).reset_index()

# Add these back to the customer dataset
cust = pd.merge(cust, sales_summary, on='customer_id', how='left')

# Calculate sales channel percentages for each customer
sales_count_for_each_channel = sales.groupby(['customer_id', 'sales_channel']).size().reset_index(name='channel_count')
total_sales_per_cust = sales.groupby('customer_id')['sales_id'].count().reset_index(name='total_sales')
sales_channel_percentage = pd.merge(sales_count_for_each_channel, total_sales_per_cust, on='customer_id')
sales_channel_percentage['channel_percentage'] = 100 * (sales_channel_percentage['channel_count'] / sales_channel_percentage['total_sales'])

# Add purchase frequency for each customer
sales_count = sales.groupby('customer_id')['sales_id'].count().reset_index(name='total_purchases')
cust = pd.merge(cust, sales_count, on='customer_id', how='left')
cust['purchase_frequency'] = cust['total_purchases'] / cust['ltd']

# Add product popularity
product_popularity = sales.groupby('product_name')['quantity'].sum().reset_index(name='total_quantity_sold')
sales = pd.merge(sales, product_popularity, on='product_name', how='left')

# CUSTOMER SUPPORT DATA
open_tickets = cust_supp[cust_supp['status'] == 'Open']
ticket_freq = open_tickets.groupby('customer_id').size().reset_index(name='open_tickets')
cust_supp = pd.merge(cust_supp, ticket_freq, on='customer_id', how='left')
cust_supp['open_tickets'] = cust_supp['open_tickets'].fillna(0).astype(int)

# ////////////////////////  MARKETING DATA  \\\\\\\\\\\\\\\\\\\\\\\\\\


# Add engagement rate for each row
marketing['engagement_rate'] = (marketing['clicks'] / marketing['impressions']) * 100

# Calculate conversion rates by campaign type
conversion_rate_by_campaign = marketing.groupby('campaign_type').apply(
    lambda x: 100 * x['conversion'].sum() / x['customer_id'].count()
).reset_index(name='conversion_rate')

# Add average engagement score for each campaign type
avg_eng_score = marketing.groupby('campaign_type')['engagement_score'].mean().reset_index(name='avg_engagement_score')
marketing = pd.merge(marketing, avg_eng_score, on='campaign_type', how='left')

# Cumulative interaction per customer and campaign type
cumul_interaction = marketing.groupby(['customer_id', 'campaign_type']).agg(
    total_clicks=('clicks', 'sum'),
    total_impressions=('impressions', 'sum')
).reset_index()
marketing = pd.merge(marketing, cumul_interaction, on=['customer_id', 'campaign_type'], how='left')
