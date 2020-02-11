import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 pyspark-shell'
import Part1_SparkSQL as a
import Part2_SparkStreaming as b
#import Part3_Analysis_and_Visualization as c
 
def main():
    print('Hello. Welcome to the system!')
    entry = None
    while entry != 'd':
        entry = input('\na) Read/Write credit card data from MariaDB to MongoDB\
                    \nb) Read/Write data from "Health Insurance Marketplace" to MongoDB\
                    \nc) Analyze/Visualize data (after loading)\
                    \nd) Log out\
                    \n\nPlease choose a, b, c, or d:\
                    \n>>>>> ')
        if entry == 'a':
            mdb_entry = input('\nMariaDB Tables:\
                           \n\ta) Table One: CDW_SAPP_BRANCH\
                           \n\tb) Table Two: CDW_SAPP_CREDITCARD\
                           \n\tc) Table Three: CDW_SAPP_CUSTOMER\
                           \n\nPlease select a table from the list\
                           \n>>>>> ')
            if mdb_entry == 'a':
                a.main('cdw_sapp_branch', a.branch_transformation_query, 'CDW_SAPP_D_BRANCH')
            elif mdb_entry == 'b':
                a.main('cdw_sapp_creditcard', a.cc_transformation_query, 'CDW_SAPP_D_CREDIT_CARD')
            elif mdb_entry == 'c':
                a.main('cdw_sapp_customer', a.customer_transformation_query, 'CDW_SAPP_D_CUSTOMER')       
        elif entry == 'b':
            himd_entry = input('\nHealth Insurance Marketplace Data Files:\
                           \n\ta) BenefitsCostSharing.txt\
                           \n\tb) Insurance.csv\
                           \n\tc) PlanAttributes.csv\
                           \n\td) Network.csv\
                           \n\te) ServiceArea.csv\
                           \n\nPlease select a file from the list\
                           \n>>>>> ')
            if himd_entry == 'a':
                b.main(b.url_1, b.topic_1, '\t', 'benefits_cost_sharing')
                b.main(b.url_2, b.topic_2, '\t', 'benefits_cost_sharing')
                b.main(b.url_3, b.topic_3, '\t', 'benefits_cost_sharing')
                b.main(b.url_4, b.topic_4, '\t', 'benefits_cost_sharing')
            elif himd_entry == 'b':
                b.main(b.url_5, b.topic_5, '\t', 'insurance')
            elif himd_entry == 'c':
                b.main(b.url_6, b.topic_6, '\t', 'plan_attributes')
            elif himd_entry == 'd':
                b.main(b.url_7, b.topic_7, ',', 'network')
            elif himd_entry == 'e':
                b.main(b.url_8, b.topic_8, ',', 'service_area')
            else:
                print('Invalid Option...')
        elif entry == 'c':
            viz_entry = input('\nAnalysis and Visualization Options:\
                           \n\ta) Plot state counts of ServiceAreaName, SourceName, and BusinessYear\
                           \n\tb) Plot the counts of sources across the country\
                           \n\tc) Do not choose\
                           \n\td) Plot the number of benefit plans in each state\
                           \n\te) Print the number of mothers who smoke\
                           \n\tf) Plot the rate of smoking for each region\
                           \n\nPlease select an option from the list\
                           \n>>>>> ')
            if viz_entry == 'a':
                c.a()
            elif viz_entry == 'b':
                c.b()
            elif viz_entry == 'd':
                c.d()
            elif viz_entry == 'e':
                c.e()
            elif viz_entry == 'f':
                c.f()
            else:
                print('Invalid Option...')                               
        elif entry != 'd':
            print('\nInvalid Option...')
    print('Closing Program. Goodbye.')
    
if __name__=='__main__':
    main()