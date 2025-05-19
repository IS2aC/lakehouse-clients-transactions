# This job is the first one, his role is to push transactional date performance if data exists ...
# Bucket 'dailytrans'  
"""
- firstly, check if the file csv already exits
- secondly, if schema is validated
- thirdly, if exists checks over data by comparing if  daily transaction dataframe is the same.
- fourthly, data quality checks over dataframe (checking over problem on dataframe.) --> using great_expectations

"""
if __name__ == "__main__":
    pass 

