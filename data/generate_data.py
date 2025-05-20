import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import date

fake = Faker()

# Helper function to generate random data

def generate_facebook_google_ads_data(num_rows, channel_name):
    data = {
        'ad_id': [fake.uuid4() for _ in range(num_rows)],
        'campaign_id': [random.randint(1, 100) for _ in range(num_rows)],
        'date': [fake.date_between(start_date=date(2024, 1, 1), end_date=date(2024, 4, 30)) for _ in range(num_rows)],
        'impressions': [random.randint(1000, 10000) for _ in range(num_rows)],
        'clicks': [random.randint(50, 200) for _ in range(num_rows)],
        'cost': [random.uniform(100, 2000) for _ in range(num_rows)],
        'channel': [channel_name for _ in range(num_rows)]
    }
    return pd.DataFrame(data)


def generate_email_campaigns_data(num_rows):
    data = {
        'email_id': [fake.uuid4() for _ in range(num_rows)],
        'campaign_id': [random.randint(1, 100) for _ in range(num_rows)],
        'send_date': [fake.date_between(start_date=date(2024, 1, 1), end_date=date(2024, 4, 30)) for _ in range(num_rows)],
        'opens': [random.randint(100, 500) for _ in range(num_rows)],
        'clicks': [random.randint(50, 200) for _ in range(num_rows)],
        'cost': [random.uniform(100, 2000) for _ in range(num_rows)],
        'channel': ['Email' for _ in range(num_rows)]
    }
    return pd.DataFrame(data)


def generate_customer_transactions_data(num_rows):
    data = {
        'transaction_id': [fake.uuid4() for _ in range(num_rows)],
        'customer_id': [random.randint(1, 1000) for _ in range(num_rows)],
        'campaign_id': [random.randint(1, 100) for _ in range(num_rows)],
        'purchase_date': [fake.date_between(start_date=date(2024, 1, 1), end_date=date(2024, 4, 30)) for _ in range(num_rows)],
        'amount': [random.uniform(10, 5000) for _ in range(num_rows)]
    }
    return pd.DataFrame(data)


def generate_website_visits_data(num_rows):
    data = {
        'session_id': [fake.uuid4() for _ in range(num_rows)],
        'customer_id': [random.randint(1, 1000) for _ in range(num_rows)],
        'page_views': [random.randint(1, 10) for _ in range(num_rows)],
        'session_duration': [random.randint(30, 300) for _ in range(num_rows)],
        'source': [random.choice(['Direct', 'Referral', 'Organic', 'Social']) for _ in range(num_rows)],
        'visit_date': [fake.date_between(start_date=date(2024, 1, 1), end_date=date(2024, 4, 30)) for _ in range(num_rows)]
    }
    return pd.DataFrame(data)


# Generate data
num_rows = random.randint(500, 1000)

facebook_ads_data = generate_facebook_google_ads_data(num_rows, 'Facebook')
google_ads_data = generate_facebook_google_ads_data(num_rows, 'Google')
email_campaigns_data = generate_email_campaigns_data(num_rows)
customer_transactions_data = generate_customer_transactions_data(num_rows)
website_visits_data = generate_website_visits_data(num_rows)

# Save to CSV
facebook_ads_data.to_csv('data/raw/facebook_ads.csv', index=False)
google_ads_data.to_csv('data/raw/google_ads.csv', index=False)
email_campaigns_data.to_csv('data/raw/email_campaigns.csv', index=False)
customer_transactions_data.to_csv('data/raw/customer_transactions.csv', index=False)
website_visits_data.to_csv('data/raw/website_visits.csv', index=False)

print("Data generation complete.") 