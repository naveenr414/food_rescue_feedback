from fr_feedback import get_feedback_by_date, generate_prompts_and_analyze_feedback
from database import open_connection
import argparse
import datetime
import os
import json
import openai

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

client = openai.OpenAI(api_key=openai_api_key)

connection_dict = open_connection(db_name,username,password,ip_address,port)
connection = connection_dict['connection']
cursor = connection_dict['cursor']

feedbacks = get_feedback_by_date(connection,start_date,end_date)
annotated_feedback = generate_prompts_and_analyze_feedback(feedbacks,model_name,batch=True)

processed_ids = set()

# Write the feedbacks to the appropriate file
w = open("{}/feedbacks.jsonl".format(os.path.dirname(__file__)),"w")
for feedback in annotated_feedback:
    if feedback['custom_id'] not in processed_ids:
        w.write(json.dumps(feedback))
        w.write("\n")
        processed_ids.add(feedback['custom_id'])
w.close()

if len(annotated_feedback) > 0:
    # Run the processing script
    batch_input_file_id = client.files.create(
        file=open("{}/feedbacks.jsonl".format(os.path.dirname(__file__)), "rb"),
        purpose="batch"
    ).id
    batch_info = client.batches.create(
        input_file_id=batch_input_file_id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={
            "description": "Food Rescue Eval."
        }
    )