from fr_feedback import get_feedback_by_date, generate_prompts_and_analyze_feedback
from database import open_connection
import argparse
from psycopg2.extras import execute_values
import datetime
import os

current_time = datetime.datetime.now()

model_name = 'gpt-4o-mini'
parser = argparse.ArgumentParser()
parser.add_argument('--start_date',   help='num beneficiaries (arms)', type=str)
parser.add_argument('--end_date', help='volunteers per arm', type=str)
args = parser.parse_args()
start_date      = args.start_date
end_date = args.end_date

db_name = os.environ.get("POSTGRES_DB")
username = os.environ.get("POSTGRES_USER")
password = os.environ.get("POSTGRES_PASSWORD") 
ip_address = os.environ.get("DATABASE_HOST") 
port = os.environ.get("DATABASE_PORT")
openai_api_key = os.environ.get("OPENAI_API_KEY")

connection_dict = open_connection(db_name,username,password,ip_address,port)
connection = connection_dict['connection']
cursor = connection_dict['cursor']

feedbacks = get_feedback_by_date(connection,start_date,end_date)
if len(feedbacks) > 0:
    annotated_feedback = generate_prompts_and_analyze_feedback(feedbacks,model_name)

    columns = ['owner_id','positive_comment']

    values = [tuple(row) for row in annotated_feedback.to_numpy()]

    print(columns,values)
    all_vals = set()
    reduced_values = []
    for v in values:
        if v[0] not in all_vals:
            reduced_values.append(v)
            all_vals.add(v[0])
    values = reduced_values

    # columns = list(annotated_feedback.columns)+['created_at','updated_at']

    # values = [tuple(row)+(current_time,current_time) for row in annotated_feedback.to_numpy()]

    # insert_query = f"""
    #     INSERT INTO rescue_feedback ({', '.join(columns)}) 
    #     VALUES %s
    #     ON CONFLICT DO NOTHING
    # """

    insert_query = """INSERT INTO rescue_feedback (owner_id, positive_comment)
    VALUES %s
    ON CONFLICT (owner_id)
    DO UPDATE SET
    positive_comment = EXCLUDED.positive_comment;"""

    execute_values(cursor, insert_query, values)
connection.commit()
cursor.close()
connection.close()
