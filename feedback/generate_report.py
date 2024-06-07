import pandas as pd

def calculate_donor_statistics(feedbacks, labeled_feedbacks):
    """Compute the donor statistics from feedbacks + labeled feedbacks
    
    Arguments:
        feedbacks: dataframe of Feedbacks
        labeled_feedbacks: dataframe of Feedbacks with prompt classification
        
    Returns: Dataframe with Donor Statistics"""

    # Calculate the number of feedbacks per donor location
    donor_feedback_counts = feedbacks['donor_location_id'].value_counts().reset_index()
    donor_feedback_counts.columns = ['donor_location_id', 'feedback_count']

    # Calculate the average volunteer rating per donor location
    average_rating = feedbacks.groupby('donor_location_id')['volunteer_rating'].mean().reset_index()
    average_rating.columns = ['donor_location_id', 'average_volunteer_rating']

    # Get donor information
    donor_info = feedbacks.groupby('donor_location_id')[
        ['donor_id', 'donor_name', 'donor_location_name']].first().reset_index()

    # Merge dataframes to combine feedback counts, average ratings, and donor info
    donor_stats = donor_feedback_counts.merge(average_rating, on='donor_location_id', how='left')
    donor_stats = donor_stats.merge(donor_info, on='donor_location_id', how='left')

    # Initialize columns for donor contact problems, inadequate food, and volunteer comments
    donor_stats['donor_contact_problem'] = 0
    donor_stats['inadequate_food'] = 0
    donor_stats['volunteer_comments'] = [[] for _ in range(len(donor_stats))]

    # Update donor problems and comments based on labeled feedbacks
    for i in range(len(labeled_feedbacks)):
        problem_count = 0
        for problem in ['donor_contact_problem', 'inadequate_food']:
            if labeled_feedbacks.loc[i, problem] == 1:
                donor_location_id = labeled_feedbacks.loc[i, 'donor_location_id']
                donor_stats.loc[donor_stats['donor_location_id'] == donor_location_id, problem] += 1
                problem_count += 1

        if problem_count > 0:
            index = donor_stats[donor_stats['donor_location_id'] == donor_location_id].index[0]
            donor_stats.at[index, 'volunteer_comments'].append(labeled_feedbacks.loc[i, 'volunteer_comment'])

    donor_stats['feedback_count'] = donor_stats['volunteer_comments'].apply(len)

    # Calculate score based on average volunteer rating and donor problems
    donor_stats['score'] = (4 - donor_stats['average_volunteer_rating']) + donor_stats['inadequate_food'] + donor_stats[
        'donor_contact_problem']
    donor_stats = donor_stats.sort_values(by='score', ascending=False)
    return donor_stats

def calculate_recipient_statistics(feedbacks, labeled_feedbacks):
    """Compute the donor statistics from feedbacks + labeled feedbacks
    
    Arguments:
        feedbacks: dataframe of Feedbacks
        labeled_feedbacks: dataframe of Feedbacks with prompt classification
        
    Returns: Dataframe with Recipient Statistics"""


    # Calculate the number of feedbacks per recipient location
    recipient_feedback_counts = feedbacks['recipient_location_id'].value_counts().reset_index()
    recipient_feedback_counts.columns = ['recipient_location_id', 'feedback_count']

    # Calculate the average volunteer rating per recipient location
    average_rating = feedbacks.groupby('recipient_location_id')['volunteer_rating'].mean().reset_index()
    average_rating.columns = ['recipient_location_id', 'average_volunteer_rating']

    # Get recipient information
    recipient_info = feedbacks.groupby('recipient_location_id')[
        ['recipient_id', 'recipient_name', 'recipient_location_name']].first().reset_index()

    # Merge dataframes to combine feedback counts, average ratings, and recipient info
    recipient_stats = recipient_feedback_counts.merge(average_rating, on='recipient_location_id', how='left')
    recipient_stats = recipient_stats.merge(recipient_info, on='recipient_location_id', how='left')

    # Initialize columns for recipient problems and volunteer comments
    recipient_stats['recipient_problem'] = 0
    recipient_stats['volunteer_comments'] = [[] for _ in range(len(recipient_stats))]
    recipient_stats['feedback_count'] = len(recipient_stats)

    # Update recipient problems and comments based on labeled feedbacks
    for i in range(len(labeled_feedbacks)):
        if labeled_feedbacks.loc[i, 'recipient_problem'] == 1:
            recipient_location_id = labeled_feedbacks.loc[i, 'recipient_location_id']
            recipient_stats.loc[recipient_stats['recipient_location_id'] == recipient_location_id, 'recipient_problem'] += 1
            index = recipient_stats[recipient_stats['recipient_location_id'] == recipient_location_id].index[0]
            recipient_stats.at[index, 'volunteer_comments'].append(labeled_feedbacks.loc[i, 'volunteer_comment'])

    # Calculate score based on average volunteer rating and recipient problems
    recipient_stats['score'] = (4 - recipient_stats['average_volunteer_rating']) + recipient_stats['recipient_problem']
    recipient_stats = recipient_stats.sort_values(by='score', ascending=False)
    recipient_stats['feedback_count'] = recipient_stats['volunteer_comments'].apply(len)

    return recipient_stats

def info_update(labeled_feedbacks):
    """Compute the information from the labeled feedbacks, but cleaned up
    
    Arguments:
        labeled_feedbacks: dataframe of Feedbacks with prompt classification
        
    Returns: Dataframe with Info Update"""

    labeled_feedbacks = labeled_feedbacks[labeled_feedbacks['info_update'] == 1]
    report_info = labeled_feedbacks[['donor_location_id', 'donor_location_name', 'donor_id', 'donor_name', 'recipient_location_id', 'recipient_location_name', 'recipient_id', 'recipient_name', 'volunteer_rating', 'volunteer_comment']]
    return report_info