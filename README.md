# WikiNER-BERT_privacy
Analysis of NERs in wikipedia text and BERT embedding aggregate NER leakage analysis. 

get_wikipedia_ners.py
------   You can find the code for performing reading Wikipedia XML from WWBP hdfs, Cleaning the XML dump to extract Text and getting
         NER along with frequency.
         
tweets_collection.py
------   In this script you will get a wrapper above mySQLdb to collect tweets associted named entities for twitterGender database.
         Creation and updation of tables is handled by the db_wrapper class. 
         
get_BERT.py
------  In this script you will get codes for extracting BERT embddings of the target tweets. BERT embedding aggregation is done. 
        Aggregation of level 11 layer is done and stored in a MySQL table.
        
logistic_regrssion.py
------  This file tarins a logistic regression classifier on all BERT embeddings. 1:5 ration of target and false labels. Conusion matrix,
        along with accuracy, fscore is stored in a new table.
        
Viz folder : 
        You will find interactive Bokeh vizualization in this folder.
        
Data : 
        Data needed for the above analysis is given in this folder.
