import pandas as pd
import numpy as np
from faker import Faker
from datetime import date
import secrets

fake = Faker()

# Helper function to generate random data

def generate_facebook_google_ads_data(num_rows, channel_name):
    data = {
        'ad_id': [fake.uuid4() for _ in range(num_rows)],
        'campaign_id': [secrets.SystemRandom().randint(1, 100) for _ in range(num_rows)],
        'date': [fake.date_between(start_date=date(2024, 1, 1), end_date=date(2024, 4, 30)) for _ in range(num_rows)],
        'impressions': [secrets.SystemRandom().randint(1000, 10000) for _ in range(num_rows)],
        'clicks': [secrets.SystemRandom().randint(50, 200) for _ in range(num_rows)],
        'cost': [secrets.SystemRandom().uniform(100, 2000) for _ in range(num_rows)],
        'channel': [channel_name for _ in range(num_rows)]
    }
    return pd.DataFrame(data)


def generate_email_campaigns_data(num_rows):
    data = {
        'email_id': [fake.uuid4() for _ in range(num_rows)],
        'campaign_id': [secrets.SystemRandom().randint(1, 100) for _ in range(num_rows)],
        'send_date': [fake.date_between(start_date=date(2024, 1, 1), end_date=date(2024, 4, 30)) for _ in range(num_rows)],
        'opens': [secrets.SystemRandom().randint(100, 500) for _ in range(num_rows)],
        'clicks': [secrets.SystemRandom().randint(50, 200) for _ in range(num_rows)],
        'cost': [secrets.SystemRandom().uniform(100, 2000) for _ in range(num_rows)],
        'channel': ['Email' for _ in range(num_rows)]
    }
    return pd.DataFrame(data)


def generate_customer_transactions_data(num_rows):
    data = {
        'transaction_id': [fake.uuid4() for _ in range(num_rows)],
        'customer_id': [secrets.SystemRandom().randint(1, 1000) for _ in range(num_rows)],
        'campaign_id': [secrets.SystemRandom().randint(1, 100) for _ in range(num_rows)],
        'purchase_date': [fake.date_between(start_date=date(2024, 1, 1), end_date=date(2024, 4, 30)) for _ in range(num_rows)],
        'amount': [secrets.SystemRandom().uniform(10, 5000) for _ in range(num_rows)]
    }
    return pd.DataFrame(data)


def generate_website_visits_data(num_rows):
    data = {
        'session_id': [fake.uuid4() for _ in range(num_rows)],
        'customer_id': [secrets.SystemRandom().randint(1, 1000) for _ in range(num_rows)],
        'page_views': [secrets.SystemRandom().randint(1, 10) for _ in range(num_rows)],
        'session_duration': [secrets.SystemRandom().randint(30, 300) for _ in range(num_rows)],
        'source': [secrets.choice(['Direct', 'Referral', 'Organic', 'Social']) for _ in range(num_rows)],
        'visit_date': [fake.date_between(start_date=date(2024, 1, 1), end_date=date(2024, 4, 30)) for _ in range(num_rows)]
    }
    return pd.DataFrame(data)


# Generate data
num_rows = secrets.SystemRandom().randint(500, 1000)

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
