from database import load_data
import openai 
import json
import os

openai_api_key = os.environ.get("OPENAI_API_KEY")

def get_feedback_by_date(conn,start_date,end_date):
    """Get all the feedback received between the start and end dates
    
    Arguments:
        cursor: Database PSQL cursor
        start_date: String, start date, of the form 2022-05-01
        end_date: String, end date, of the form 2022-05-10
    
    Returns: DataFrame, with organized info on all feedback"""
    
    feedbacks_query = f"""WITH all_ids as (
        SELECT 
            ds.donor_ids[c1.ord] as donor_id,
            c1.element AS donor_location_id,
            ds.recipient_ids[c2.ord2] as recipient_id,
            c2.element AS recipient_location_id,
            ds.delivery_id,
            ds.delivery_type,
            ds.volunteer_comment
        FROM 
            delivery_summaries ds,
            LATERAL unnest(ds.donor_location_ids) WITH ORDINALITY AS c1(element, ord),
            LATERAL unnest(ds.recipient_location_ids) WITH ORDINALITY AS c2(element, ord2)
        WHERE
            ds.published_at between '{start_date}' AND '{end_date}' and ds.volunteer_comment is not null
        )
        select d.id as donor_id, 
            dl.id as donor_location_id,
            d.name || ' - ' || dl.name as donor_name, 
            r.id as recipient_id,
            rl.id as recipient_location_id,	   
            r.name || ' - ' || rl.name as recipient_name,
            ai.delivery_id as id,
            ai.delivery_type as owner_type,
            ai.volunteer_comment
        from all_ids ai 
            inner join donors d on ai.donor_id=d.id 
            inner join donor_locations dl on ai.donor_location_id=dl.id
            inner join recipients r on ai.recipient_id=r.id
            inner join recipient_locations rl on ai.recipient_location_id=rl.id;
        """

    feedbacks = load_data(feedbacks_query, conn)
    feedbacks = feedbacks[feedbacks['volunteer_comment'] != '']

    return feedbacks

def get_batch_feedback(feedbacks, prompts, tasks, model_name):
    """Analyze the feedback using prompts to classify different properties
    
    Arguments:
        client: OpenAI client
        feedbacks: Dataframe of all the feedbacks
        prompts: Dictionary mapping prompt name to the prompt text
        tasks: List of prompts we're looking into
    
    Returns: DataFrame with annotated feedback"""

    all_data = []

    for i in range(len(feedbacks)):
        comment = (
            f'For this rescue, the donor is {feedbacks.loc[i, "donor_name"]};'
            f' the recipient is {feedbacks.loc[i, "recipient_name"]}.'
            f' Comment: {feedbacks.loc[i, "volunteer_comment"]}'
        )

        id = feedbacks.loc[i,"id"]
        owner_type = feedbacks.loc[i,'owner_type']

        for task in tasks:
            custom_id = "{}_{}_{}".format(task,id,owner_type)
            formatted_dict = {'custom_id': custom_id, 
            'method': 'POST', 
            'url': "/v1/chat/completions", 
            'body': {
                'model': model_name.replace("_self_reflection",""), 
                'messages': [{"role": "user", "content": prompts[task] + comment}], 
                'response_format':{"type": "json_object"},
            }}
            all_data.append(formatted_dict)
    return all_data 


def analyze_feedback(client, feedbacks, prompts, tasks, model_name):
    """Analyze the feedback using prompts to classify different properties
    
    Arguments:
        client: OpenAI client
        feedbacks: Dataframe of all the feedbacks
        prompts: Dictionary mapping prompt name to the prompt text
        tasks: List of prompts we're looking into
    
    Returns: DataFrame with annotated feedback"""
    for i in range(len(feedbacks)):
        comment = (
            f'For this rescue, the donor is {feedbacks.loc[i, "donor_name"]};'
            f' the recipient is {feedbacks.loc[i, "recipient_name"]}.'
            f' Comment: {feedbacks.loc[i, "volunteer_comment"]}'
        )

        print("On Rescue {} out of {}".format(i+1,len(feedbacks)))

        for task in tasks:
            # if task == 'donor_problem':
            try:
                response = client.chat.completions.create(
                    model=model_name.replace("_self_reflection",""),
                    messages=[{"role": "user", "content": prompts[task] + comment}],
                    response_format={"type": "json_object"},
                )
                output = response.choices[0].message.content
                feedback_info = json.loads(output)
                feedbacks.loc[i, f'{task}'] = feedback_info[task]
            except Exception as e:
                print(f"Error processing feedback {i} for task {task}: {e}")

    return feedbacks 

def generate_prompts_and_analyze_feedback(feedbacks,model_name,batch=False):
    """Use the OpenAI client to generate prompts
    
    Arguments:
        feedbacks: DataFrame of feedbacks
    
    Returns: DataFrame with annotated feedback"""

    if 'gpt' in model_name:
        client = openai.OpenAI(api_key=openai_api_key)
    else:
        raise Exception("Model {} not found".format(model_name))

    feedbacks = feedbacks[feedbacks['volunteer_comment'].notnull()]

    prompts = {}
    tasks = ['recipient_problem', 'inadequate_food', 'donor_problem', 
            'direction_problem','earlier_pickup','system_problem',
            'update_contact','positive_comment']
    for t in tasks:
         prompts[t] = open("{}/prompts/{}.txt".format(os.path.dirname(__file__), t)).read()


    if batch:
        annotated_feedback = get_batch_feedback(feedbacks, prompts, tasks,model_name)
    else:
        annotated_feedback = analyze_feedback(client, feedbacks, prompts, tasks,model_name)
        annotated_feedback = annotated_feedback.rename(columns={'rescue_id': 'old_rescue_id'})
        annotated_feedback = annotated_feedback.rename(columns={'id': 'owner_id'})[['owner_id']+tasks+['owner_type']]

    return annotated_feedback