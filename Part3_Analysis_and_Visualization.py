import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt 

# connect to MongoDB
client = MongoClient('localhost', 27017)

# specify database
db = client.health_insurance_marketplace_data

# specify collections
service_area = db.service_area
benefits_cost_sharing = db.benefits_cost_sharing
insurance = db.insurance

# extract data from collections and save to pandas dataframes
service_area_df = pd.DataFrame(list(service_area.find()))
benefits_df = pd.DataFrame(list(benefits_cost_sharing.find()))
insurance_df = pd.DataFrame(list(insurance.find()))


# a) Use “Service Area Dataset” from MongoDB. Find and plot the count of ServiceAreaName, SourceName , and BusinessYear across the country each state? 

def a():
    counts_by_state = service_area_df[['ServiceAreaName', 'SourceName', 'BusinessYear', 'StateCode']].groupby(by=['StateCode']).count()
    print(pd.DataFrame(counts_by_state))
    counts_by_state.plot.bar(title = 'a) State Counts for ServiceAreaName, SourceName, BusinessYear',
                         figsize = (15,10),
                         rot = 0)
    plt.show()


# b)​      ​Use “Service Area Dataset” from MongoDB. Find and plot the count of “​sources​” across the country. 

def b():
    source_name_count_national = service_area_df[['_id', 'SourceName']].groupby(by=['SourceName']).count()
    #display(source_name_count_national)
    source_name_count_national.plot.bar(title = "b) Source Count, National",
                                       figsize = (10,5),
                                       rot=0)
    plt.show()


# d)​      ​Use the “Benefit Cost Sharing” dataset from MongoDB. Find and plot the number of benefit plans in each state. 

def d():
    benefit_plan_count_by_state = benefits_df.groupby(by='StateCode')['PlanId'].nunique()
    #display(benefit_plan_count_by_state)
    benefit_plan_count_by_state.plot.barh(title = 'd) Number of Benefit Plans Per State', 
                                          figsize=(10,10))
    plt.show()

#  e)​      ​Use the “Insurance” dataset from MongoDB and find the number of mothers who smoke and also have children. 

def e():
    print('Mothers who smoke:', insurance_df[(insurance_df['sex'] == 'female') & (insurance_df['smoker'] == 'yes') & (insurance_df['children'] > 0)]['_id'].count())


#  f)​       ​Use the “Insurance” dataset from MongoDB. Find out which region has the highest rate of smokers. Plot the results for each region. 

def f():
    everyone = insurance_df.groupby('region').count()['_id']
    smokers = insurance_df[insurance_df.smoker == 'yes'].groupby('region').count()['_id']
    smoking_rates = smokers / everyone
    smoking_rates.plot.barh(title = 'f) Southeast Region has the highest Rate of Smoking')
    plt.show()
