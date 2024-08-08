from feedback.database import run_query, open_connection, close_connection
import feedback.secret as secret 
import random 
from feedback.database import load_data 
import pandas as pd 
import openai 
import json

def get_all_donors(cursor):
    """List all the donors
    
    Arguments: None
    
    Returns: List of Integers, all the donor_location_ids"""
    query = "SELECT DONOR_LOCATION_ID FROM DONATIONS"
    all_donors = run_query(cursor,query)
    all_donors = [i['donor_location_id'] for i in all_donors]

    return all_donors 

def get_all_donors_without_instructions(cursor):
    """List all the donors without instructions
    
    Arguments: None
    
    Returns: List of Integers, all the donor_location_ids"""
    query = ("SELECT donor_location_id"
            " FROM donations"
            " WHERE donor_location_id NOT IN (SELECT owner_id FROM special_instructions)")
    all_donors = run_query(cursor,query)
    all_donors = [i['donor_location_id'] for i in all_donors]

    return all_donors 


def get_all_donors_by_comments(cursor):
    """List all the donors sorted by the number of comments
    
    Arguments: cursor, method to access PSQL database
    
    Returns: List of Integers, all the donor_location_ids, sorted by 
        the number of comments"""
    query = ("SELECT d.donor_location_id, COUNT(r.donation_id) AS rescue_count"
            " FROM donations d"
            " JOIN rescues r ON d.id = r.donation_id"
            " WHERE r.volunteer_comment IS NOT NULL AND r.volunteer_comment <> ''"
            " GROUP BY d.donor_location_id"
            " ORDER BY rescue_count DESC;")
    all_donors = run_query(cursor,query)
    all_donors = [i['donor_location_id'] for i in all_donors]

    return all_donors 

def get_all_donors_by_comment_ratio(cursor):
    """List all the donors sorted by the number of comments vs. total resuces
    
    Arguments: cursor, method to access PSQL database
    
    Returns: List of Integers, all the donor_location_ids, sorted by 
        the number of comments"""

    query = ("SELECT d.donor_location_id, COUNT(r.donation_id) AS rescue_count"
            " FROM donations d"
            " JOIN rescues r ON d.id = r.donation_id"
            " WHERE r.volunteer_comment IS NOT NULL AND r.volunteer_comment <> ''"
            " GROUP BY d.donor_location_id"
            " ORDER BY rescue_count DESC;")
    all_donors = run_query(cursor,query)
    comments_by_donor = {}

    for i in all_donors:
        comments_by_donor[i['donor_location_id']] = i['rescue_count']

    query = ("SELECT d.donor_location_id, COUNT(r.donation_id) AS rescue_count"
            " FROM donations d"
            " JOIN rescues r ON d.id = r.donation_id"
            " GROUP BY d.donor_location_id"
            " ORDER BY rescue_count DESC;")
    all_donors = run_query(cursor,query)
    rescues_by_donor = {}

    for i in all_donors:
        rescues_by_donor[i['donor_location_id']] = i['rescue_count']

    all_donors = [(i,comments_by_donor[i]/rescues_by_donor[i],rescues_by_donor[i]) for i in comments_by_donor]
    all_donors = sorted(all_donors,key=lambda k: k[1],reverse=True)
    all_donors = sorted(all_donors,key=lambda k: k[2],reverse=True)

    return all_donors 



def get_donor_name(cursor,donor_location_id):
    """Get the name of a donor, given their location_id
    
    Arguments: donor_location_id, Integer
    
    Returns: String, name"""

    query = "SELECT name FROM DONOR_LOCATIONS WHERE ID={}".format(donor_location_id)
    donor_name = run_query(cursor,query)
    donor_name = [i['name'] for i in donor_name]

    if len(donor_name) > 0:
        donor_name = donor_name[0]
    else:
        donor_name = None

    query = ("SELECT D.name"
            " FROM DONORS D"
            " JOIN Donor_LOCATIONS DL ON D.ID = DL.Donor_ID"
            " WHERE DL.ID = {}".format(donor_location_id)) 
    real_donor_name = run_query(cursor,query)
    real_donor_name = [i['name'] for i in real_donor_name]
    if len(real_donor_name) > 0:
        real_donor_name = real_donor_name[0]
    else:
        real_donor_name = None

    return donor_name, real_donor_name

def get_recipient_name(cursor,recipient_location_id):
    """Get the name of a donor, given their location_id
    
    Arguments: donor_location_id, Integer
    
    Returns: String, name"""

    query = "SELECT name FROM RECIPIENT_LOCATIONS WHERE ID={}".format(recipient_location_id)
    recipient_name = run_query(cursor,query)
    recipient_name = [i['name'] for i in recipient_name]

    if len(recipient_name) > 0:
        recipient_name = recipient_name[0]
    else:
        recipient_name = None

    query = ("SELECT D.name"
            " FROM RECIPIENTS D"
            " JOIN Recipient_LOCATIONS DL ON D.ID = DL.RECIPIENT_ID"
            " WHERE DL.ID = {}".format(recipient_location_id)) 
    real_recipient_name = run_query(cursor,query)
    real_recipient_name = [i['name'] for i in real_recipient_name]
    if len(real_recipient_name) > 0:
        real_recipient_name = real_recipient_name[0]
    else:
        real_recipient_name = None

    return recipient_name, real_recipient_name


def get_instructions_by_id_donor(cursor,donor_location_id):
    """List all the instructions received for a donor_location
    
    Arguments: 
        donor_location_id: Integer, the donor location we're looking at
    
    Returns: List of Strings, all the feedback received"""

    query = ("SELECT text FROM"
            " SPECIAL_INSTRUCTIONS"
            " WHERE OWNER_ID={}"
            " AND OWNER_TYPE='DonorLocation' "
    )
    query = query.format(donor_location_id)
    all_instructions = run_query(cursor,query)
    all_instructions = [i['text'] for i in all_instructions]

    return all_instructions 

def get_instructions_by_id_recipient(cursor,recipient_location_id):
    """List all the instructions received for a donor_location
    
    Arguments: 
        donor_location_id: Integer, the donor location we're looking at
    
    Returns: List of Strings, all the feedback received"""

    query = ("SELECT text FROM"
            " SPECIAL_INSTRUCTIONS"
            " WHERE OWNER_ID={}"
            " AND OWNER_TYPE='RecipientLocation' "
    )
    query = query.format(recipient_location_id)
    all_instructions = run_query(cursor,query)
    all_instructions = [i['text'] for i in all_instructions]

    return all_instructions 


def get_feedback_by_id(cursor,donor_location_id):
    """List all the feedback received from a donor_location
    
    Arguments: 
        donor_location_id: Integer, the donor location we're looking at
    
    Returns: List of Strings, all the feedback received"""

    query = ("SELECT rescues.recipient_location_id, rescues.volunteer_comment FROM"
            " rescues JOIN donations ON donations.id = rescues.donation_id"
            " WHERE rescues.volunteer_comment IS NOT NULL "
            " AND rescues.volunteer_comment <> '' "
            " AND donations.donor_location_id = {}".format(donor_location_id))

    all_feedback = run_query(cursor,query)
    return all_feedback 

def get_feedback_by_date(conn,start_date,end_date):
    """Get all the feedback received between the start and end dates
    
    Arguments:
        cursor: Database PSQL cursor
        start_date: String, start date, of the form 2022-05-01
        end_date: String, end date, of the form 2022-05-10
    
    Returns: DataFrame, with organized info on all feedback"""
    
    feedbacks_query = f"SELECT * FROM rescues WHERE published_at BETWEEN '{start_date}' AND '{end_date}'"
    feedbacks = load_data(feedbacks_query, conn)
    feedbacks = feedbacks.dropna(subset=['volunteer_rating'])
    feedbacks = feedbacks[feedbacks['volunteer_comment'] != '']

    donations_query = "SELECT * FROM donations"
    donor_locations_query = "SELECT * FROM donor_locations"
    donors_query = "SELECT * FROM donors"
    recipient_locations_query = "SELECT * FROM recipient_locations"
    recipients_query = "SELECT * FROM recipients"

    donations = load_data(donations_query, conn)
    donor_locations = load_data(donor_locations_query, conn)
    donors = load_data(donors_query, conn)
    recipient_locations = load_data(recipient_locations_query, conn)
    recipients = load_data(recipients_query, conn)

    donations.rename(columns={'id': 'donation_id'}, inplace=True)
    feedbacks = feedbacks.merge(donations[['donation_id', 'donor_location_id']], on='donation_id', how='left')

    donor_locations.rename(columns={'id': 'donor_location_id', 'name': 'donor_location_name'}, inplace=True)
    feedbacks = feedbacks.merge(donor_locations[['donor_location_id', 'donor_id', 'donor_location_name']], on='donor_location_id', how='left')

    donors.rename(columns={'id': 'donor_id', 'name': 'donor_name'}, inplace=True)
    feedbacks = feedbacks.merge(donors[['donor_id', 'donor_name']], on='donor_id', how='left')

    recipient_locations.rename(columns={'id': 'recipient_location_id', 'name': 'recipient_location_name'}, inplace=True)
    feedbacks['recipient_location_id'] = feedbacks['recipient_location_id'].astype(int, errors='ignore')
    feedbacks = feedbacks.merge(recipient_locations[['recipient_location_id', 'recipient_location_name', 'recipient_id']], on='recipient_location_id', how='left')
    feedbacks = feedbacks.drop(columns=['recipient_name'])
    recipients.rename(columns={'id': 'recipient_id', 'name': 'recipient_name'}, inplace=True)
    feedbacks = feedbacks.merge(recipients[['recipient_id', 'recipient_name']], on='recipient_id', how='left')

    return feedbacks

def get_instruction_by_rescue(cursor,donor_location_id,recipient_location_id,volunteer_comment):
    """Get a rescue, instruction, and comment

    Arguments:
        cursor: Database Cursor for PSQL
        donor_location_id: Integer, donor location
        volunteer_comment: String, volunteer_comment for a particular rescue
    
    Returns: String, with donor-recipient-etc.
    """

    feedback = get_feedback_by_id(cursor,donor_location_id)
    feedback = [i for i in feedback if i['volunteer_comment'] == volunteer_comment]
    random_feedback = feedback[0]
    comment = volunteer_comment
    recipient_location_id = random_feedback['recipient_location_id']

    donor_instruction = '. '.join(get_instructions_by_id_donor(cursor,donor_location_id))
    recipient_instruction = '. '.join(get_instructions_by_id_recipient(cursor,recipient_location_id))
    donor_instruction = donor_instruction.replace("\r","").replace("\n"," ")
    recipient_instruction = recipient_instruction.replace("\r","").replace("\n","")

    donor_name = get_donor_name(cursor,donor_location_id)
    recipient_name = get_recipient_name(cursor,recipient_location_id)

    return "For this rescue, the donor is {}, and its location is {}; the recipient is {}, and its location is {}. Donor Instruction: {}. Recipient Instruction: {}. Comment: {}.".format(donor_name[1],
                    donor_name[0],
                    recipient_name[1],
                    recipient_name[0],
                    donor_instruction,
                    recipient_instruction,
                    comment)

def get_random_rescue(cursor,donor_location_id):
    """Get a random rescue, instruction, and comment

    Arguments:
        cursor: Database Cursor for PSQL
        donor_location_id: Integer, donor location
    
    Returns: String, with donor-recipient-etc.
    """

    feedback = get_feedback_by_id(cursor,donor_location_id)
    random_feedback = random.choice(feedback)
    comment = random_feedback['volunteer_comment']
    recipient_location_id = random_feedback['recipient_location_id']

    instruction = get_instructions_by_id_donor(cursor,donor_location_id)[0]

    donor_name = get_donor_name(cursor,donor_location_id)
    recipient_name = get_recipient_name(cursor,recipient_location_id)

    return "For this rescue, the donor is {}, and its location is {}; the recipient is {}, and its location is {}. Donor Instruction: {}. Recipient Instruction: {}. Comment: {}.".format(donor_name[1],donor_name[0],recipient_name[1],recipient_name[0],instruction,comment)

def improve_instructions(client,cursor,info_dataframe):
    """Suggest a list of improvements to instructions, given instructions to improve
    
    Arguments:
        client: OpenAI client
        info_dataframe: DataFrame with information on the instructions to update
    
    Returns: List of updated instructions"""

    new_instructions = []
    raw_prompt = open("../../data/prompts/information_specific_update.txt").read()
    
    for i in range(len(info_dataframe)):
        print("On {} out of {}".format(i+1,len(info_dataframe)))
        full_prompt = raw_prompt + "\n"+get_instruction_by_rescue(
            cursor,info_dataframe.iloc[i].donor_location_id,
            info_dataframe.iloc[i].recipient_location_id,info_dataframe.iloc[i].volunteer_comment)
        old_instruction_donor =  '. '.join(get_instructions_by_id_donor(cursor,info_dataframe.iloc[i].donor_location_id))
        old_instruction_recipient = '. '.join(get_instructions_by_id_recipient(cursor,info_dataframe.iloc[i].recipient_location_id))
        old_instruction_donor = old_instruction_donor.replace("\r","").replace("\n"," ")
        old_instruction_recipient = old_instruction_recipient.replace("\r","").replace("\n","")

        try:
            response = client.chat.completions.create(
                model="gpt-3.5-turbo-0125",
                messages=[{"role": "user", "content": full_prompt}],
                response_format={"type": "json_object"},
            )
            output = response.choices[0].message.content
            feedback_info = json.loads(output)
            new_instruction_donor = feedback_info['donor_info']
            new_instruction_recipient = feedback_info['recipient_info']
            donor_instruction_change = old_instruction_donor.strip(" .") != new_instruction_donor.strip(" .")
            recipient_instruction_change = old_instruction_recipient.strip(" .") != new_instruction_recipient.strip(" .")
            
            if donor_instruction_change or recipient_instruction_change:
                new_instructions.append({'donor_location_id': info_dataframe.iloc[i].donor_location_id, 
                                        'donor_id': info_dataframe.iloc[i].donor_id, 
                                        'donor_location_name': info_dataframe.iloc[i].donor_location_name, 
                                        'donor_name': info_dataframe.iloc[i].donor_name, 
                                        'recipient_location_id': info_dataframe.iloc[i].recipient_location_id, 
                                        'recipient_location_name': info_dataframe.iloc[i].recipient_location_name, 
                                        'recipient_name': info_dataframe.iloc[i].recipient_name, 
                                        'volunteer_comment': info_dataframe.iloc[i].volunteer_comment,
                                        'old_instruction_donor': old_instruction_donor, 
                                        'old_instruction_recipient': old_instruction_recipient, 
                                        'new_instruction_donor': new_instruction_donor, 
                                        'new_instruction_recipient': new_instruction_recipient, 
                                        'donor_instruction_change': donor_instruction_change,
                                        'recipient_instruction_change': recipient_instruction_change})
                print(feedback_info)
        except Exception as e:
            print(f"Error processing feedback {e}")

    return new_instructions

def analyze_feedback(client, feedbacks, prompts, tasks):
    """Analyze the feedback using prompts to classify different properties
    
    Arguments:
        client: OpenAI client
        feedbacks: Dataframe of all the feedbacks
        prompts: Dictionary mapping prompt name to the prompt text
        tasks: List of prompts we're looking into
    
    Returns: DataFrame with annotated feedback"""

    for i in range(len(feedbacks)):
        print("On feedback {} out of {}".format(i+1,len(feedbacks)))
        comment = (
            f'For this rescue, the donor is {feedbacks.loc[i, "donor_name"]};'
            f' the recipient is {feedbacks.loc[i, "recipient_name"]}.'
            f' Comment: {feedbacks.loc[i, "volunteer_comment"]}'
        )

        for task in tasks:
            try:
                response = client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[{"role": "user", "content": prompts[task] + comment}],
                    response_format={"type": "json_object"},
                )
                output = response.choices[0].message.content
                feedback_info = json.loads(output)
                feedbacks.loc[i, f'{task}'] = feedback_info[task]
            except Exception as e:
                print(f"Error processing feedback {i} for task {task}: {e}")

    return feedbacks 

def generate_prompts_and_analyze_feedback(feedbacks):
    """Use the OpenAI client to generate prompts
    
    Arguments:
        feedbacks: DataFrame of feedbacks
    
    Returns: DataFrame with annotated feedback"""

    client = openai.OpenAI(api_key=secret.openai_api_key)
    feedbacks = feedbacks[feedbacks['volunteer_comment'].notnull()]

    prompts = {}
    tasks = ['recipient_problem', 'inadequate_food', 'donor_problem', 
            'direction_problem','earlier_pickup','system_problem',
            'update_contact']
    for t in tasks:
        prompts[t] = open("../../data/prompts/hierarchical_prompts/{}.txt".format(t)).read()

    annotated_feedback = analyze_feedback(client, feedbacks, prompts, tasks)
    annotated_feedback = annotated_feedback.rename(columns={'id': 'rescue_id'})[['rescue_id']+tasks]

    return annotated_feedback