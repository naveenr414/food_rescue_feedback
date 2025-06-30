from database import open_connection
import datetime
import os
import json
import openai
from psycopg2.extras import execute_values
import time 
import sys

unix_time = time.time()
current_time = datetime.datetime.now()

model_name = 'gpt-4o-mini'

db_name = os.environ.get("POSTGRES_DB")
username = os.environ.get("POSTGRES_USER")
password = os.environ.get("POSTGRES_PASSWORD") 
ip_address = os.environ.get("DATABASE_HOST") 
port = os.environ.get("DATABASE_PORT")
openai_api_key = os.environ.get("OPENAI_API_KEY")

client = openai.OpenAI(api_key=openai_api_key)

connection_dict = open_connection(db_name,username,password,ip_address,port)
connection = connection_dict['connection']
cursor = connection_dict['cursor']

check_requests_within_hours = 28
sample_requests = []
all_batches = list(client.batches.list(limit=10))

nothing_found = True
for batch in all_batches:
    if batch.output_file_id and (time.time()-batch.created_at) < (check_requests_within_hours*3600):
        nothing_found = False 
        file_response = client.files.content(batch.output_file_id).text.split("\n")

        for line in file_response:
            try:
                sample_requests.append(json.loads(line))
            except:
                pass 

if nothing_found:
    print("Nothing found")
    sys.exit(1)

all_data = {}
current_time = datetime.datetime.now()
all_labels = ['recipient_problem', 'inadequate_food', 'donor_problem', 
    'direction_problem','earlier_pickup','system_problem',
    'update_contact','positive_comment']

for idx,i in enumerate(sample_requests): 
    try:
        custom_id = i['custom_id']
        which_task = "_".join(custom_id.split("_")[:-2])
        owner_id = custom_id.split("_")[-2]
        owner_type = custom_id.split("_")[-1]

        if owner_id not in all_data:
            all_data[owner_id] = {
                'owner_id': owner_id, 
                'owner_type': owner_type, 
                'created_at': current_time, 
                'updated_at': current_time
            }

            for j in all_labels:
                all_data[owner_id][j] = False 
        feedback_info = json.loads(i['response']['body']['choices'][0]['message']['content'])
        all_data[owner_id][which_task] = feedback_info[which_task]
    except:
        print("Error processing request {} of {}".format(idx+1,len(sample_requests)))

columns = ['owner_id', 'owner_type', 'created_at', 'updated_at', 'recipient_problem', 'inadequate_food', 'donor_problem', 'direction_problem', 'earlier_pickup', 'system_problem', 'update_contact', 'positive_comment']
values = [tuple([all_data[i][c] for c in columns]) for i in all_data]

insert_query = f"""
    INSERT INTO rescue_feedback ({', '.join(columns)}) 
    VALUES %s
    ON CONFLICT DO NOTHING
"""

execute_values(cursor, insert_query, values)
connection.commit()
cursor.close()
connection.close()

print("Wrote {} values".format(len(sample_requests)))