# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: patient
#     language: python
#     name: python3
# ---

# %load_ext autoreload
# %autoreload 2

import sys
sys.path.append('/usr0/home/naveenr/projects/food_rescue_feedback')

import numpy as np
import random 
import matplotlib.pyplot as plt
from feedback.fr_feedback import *
from feedback.generate_report import *
import feedback.secret as secret
from feedback.database import open_connection
import pandas as pd

start_date = "2024-04-26"
end_date = "2024-05-26"

# ## Donors

db_name = "fr_new" 
username = secret.database_username 
password = secret.database_password 
ip_address = "localhost"
port = secret.database_port

connection_dict = open_connection(db_name,username,password,ip_address,port)
connection = connection_dict['connection']
cursor = connection_dict['cursor']

feedbacks = get_feedback_by_date(connection,start_date,end_date)
feedbacks.to_csv('../../results/reports/feedbacks_{}_{}.csv'.format(start_date,end_date), index=False)

annotated_feedback = generate_prompts_and_analyze_feedback(feedbacks)
annotated_feedback.to_csv('../../results/reports/labeled_feedbacks_{}_{}.csv'.format(start_date,end_date), index=False)

# +
# feedbacks = pd.read_csv('../../results/reports/feedbacks_{}_{}.csv'.format(start_date,end_date))
# annotated_feedback = pd.read_csv('../../results/reports/labeled_feedbacks_{}_{}.csv'.format(start_date,end_date))

# +
# Calculate statistics and generate the report
donor_statistics = calculate_donor_statistics(feedbacks, annotated_feedback)
recipient_statistics = calculate_recipient_statistics(feedbacks, annotated_feedback)
report_info = info_update(annotated_feedback)

# Save the report to a CSV file
donor_statistics.to_csv('../../results/reports/report_donor_{}_{}.csv'.format(start_date,end_date), index=False)
recipient_statistics.to_csv('../../results/reports/report_recipient_{}_{}.csv'.format(start_date,end_date), index=False)
report_info.to_csv('../../results/reports/report_info_{}_{}.csv'.format(start_date,end_date), index=False)
# -


