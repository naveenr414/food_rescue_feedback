import os
import psycopg2
import pandas as pd
import argparse
import feedback.secret as secret

def load_data(query, conn):
    return pd.read_sql_query(query, conn)
def process_feedback_data(user, password, start_date='2022-05-01', end_date='2022-05-10'):
    conn = psycopg2.connect(
        dbname='fr_new',
        user=secret.database_username,
        password=secret.database_password,
        host='localhost',
        port=secret.database_port
    )



if __name__ == '__main__':
    start_date = '2024-05-01'
    end_date = '2024-05-02'
    parser = argparse.ArgumentParser()
    parser.add_argument('--user', type=str, help='Database user')
    parser.add_argument('--password', type=str, help='Database password')
    args = parser.parse_args()

    feedbacks = process_feedback_data(args.user, args.password, start_date, end_date)

    feedbacks.to_csv('feedbacks.csv', index=False)
    print(feedbacks.head())
