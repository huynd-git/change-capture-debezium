import psycopg2
import faker
from datetime import datetime
import random

"""
VARIABLES
"""
fake = faker.Faker()

"""
FUNCTIONS
"""
def generate_transaction():
    """
    Generate fake transaction data
    Output:
        - data: transactions data generated in a python dictionary 
    """
    data = {}
    user = fake.simple_profile()

    data['transactionId'] = fake.uuid4()
    data['userId'] = user['username']
    data['timestamp'] = datetime.utcnow().timestamp()
    data['amount'] = round(random.uniform(10, 1000), 2)
    data['currency'] = random.choice(['USD', 'GBP'])
    data['city'] = fake.city()
    data['country'] = fake.country()
    data['merchantName'] = fake.company()
    data['paymentMethod'] = random.choice(['credit_card', 'debit_card', 'online_transfer'])
    data['ipAddress'] = fake.ipv4()
    data['voucherCode'] = random.choice(['', 'DISCOUNT10', ''])
    data['affiliateId'] = fake.uuid4()

    return data


def create_table(conn):
    """
    Create table in progesql database
    Input: 
        - conn: progresql connection 
    """
    cursor = conn.cursor()

    query = """CREATE TABLE IF NOT EXISTS transactions (
        transaction_id VARCHAR(255) PRIMARY KEY,
        user_id VARCHAR(255),
        timestamp TIMESTAMP,
        amount DECIMAL,
        currency VARCHAR(255),
        city VARCHAR(255),
        country VARCHAR(255),
        merchant_name VARCHAR(255),
        payment_method VARCHAR(255),
        ip_address VARCHAR(255),
        voucher_code VARCHAR(255),
        affiliate_id VARCHAR(255)
    )"""

    cursor.execute(query)
    
    cursor.close()
    conn.commit()


"""
MAIN
"""
if __name__ == "__main__":
    conn = psycopg2.connect(
        host='localhost',
        database='financial_db',
        user='postgres',
        password='postgres',
        port=5432
    )

    create_table(conn)
    data = generate_transaction()
    print(data)

    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO transactions
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (data['transactionId'], data['userId'], datetime.fromtimestamp(data['timestamp']), data['amount'],
        data['currency'], data['city'], data['country'], data['merchantName'],
        data['paymentMethod'], data['ipAddress'], data['voucherCode'], data['affiliateId']))
   

    cursor.close()
    conn.commit()
