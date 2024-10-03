########### Packages
import re
import json
import os
import pandas as pd
import io
import names
import string

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pandas.io.json import json_normalize
import nltk
#nltk.download('stopwords')
#nltk.download('punkt') #Run if on new pc
#nltk.download('wordnet')
from nltk.tokenize import sent_tokenize, word_tokenize 
from datetime import datetime
from pandas.core.common import flatten
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from nltk.stem import WordNetLemmatizer
#import streamlit as st

custom_drops = ['brian', 'armando', 'katie', 'honglin']
###########Get directory structure
#path = os.chdir(os.path.dirname(sys.argv[0]))
TopDir = os.getcwd()+ "\\UHD-MSDA Slack export Dec 3 2017 - Nov 12 2019" #Change for new exports
ext = "-clean-predict" #New directory extension
SubDirs = os.listdir(TopDir + ext)

count = 0
msgs = 0
df = pd.DataFrame()
    
########### Iterate through files
for file in os.listdir(TopDir+ext):
    filename = os.fsdecode(file)
    with io.open(os.path.join(TopDir + ext, file), encoding="utf8", errors="ignore") as f:
        #print(file)
        #e = f.encode("utf-8", errors='ignore')
        df2 = pd.DataFrame.from_dict(json_normalize(json.load(f)), orient='columns')
        df2['Channel'] = filename.split(' ')[0]
        df = pd.concat([df, df2])#, sort = True) #Different versions between pc's
        #print(df2)

#print("Data Frame:")      
########### Build Data Frames 
df = df[['user','ts','text','reactions','subtype', 'Channel']]#Add channel #Selects the columns I want
df = df[~df['subtype'].isin(['channel_join', 'channel_topic', 'channel_purpose', 'reminder_add', 'file_comment', 'channel_archive', 'channel_name', 'pinned_item', 'thread_broadcast', 'tombstone'])]#Removes certain generated messages
#df = df[df['subtype'] == '']#Removes certain generated messages
df.index = range(len(df)) #Numbers Rows


########### Creates fake names 
name_list = {}
name_list = dict.fromkeys(df['user'].drop_duplicates())
for user in name_list:
    while True: #Keeps out duplicates
        name_list[user] = names.get_first_name()
        if (sum(value == name_list[user] for value in name_list.values()) == 1):
            break;
df['names'] = df['user'].map(name_list) 


########### Time Handling #Update to local time currently utc
#df['time'] = datetime.fromtimestamp(df[['ts']]) #Broken  to_datetime(1488286965000,unit='ms')

df['time'] = pd.to_datetime(df['ts'], unit='s')
#df['time'] = df['time'].dt.tz_localize('UTC').dt.tz_convert('US/Central')
#print(df['time'])
#df['time'] = df['time'].dt.remove_timezone()


########## #Sentiment Analyzer 
compound = []; pos = []; neu = []; neg = [];
analyzer = SentimentIntensityAnalyzer()
for i in range(0, len(df)):
    compound.append(analyzer.polarity_scores(df.loc[i,'text'])['compound'])
    pos.append(analyzer.polarity_scores(df.loc[i,'text'])['pos'])
    neu.append(analyzer.polarity_scores(df.loc[i,'text'])['neu'])
    neg.append(analyzer.polarity_scores(df.loc[i,'text'])['neg'])
    
df['pos'] = pos
df['neu'] = neu
df['neg'] = neg
df['comp'] = compound


########### Tokenize list of words 

# Scrub punctuation from this list #Still has ' 
# Add other columns to new df
dfwords = df
users = df.user.unique()
dfwords['text'] = dfwords['text'].apply(pd.Series) \
    .replace(to_replace = r'[.;:!\'?,\"()\[\]\_]+', value = ' ', regex = True)# \
 #   .replace(to_replace = r'[0-9]+', value = ' ', regex = True)
dfwords['text'] = dfwords['text'].str.lower()
dfwords['tokens'] = dfwords['text'].apply(word_tokenize) #test
dfwords = dfwords['tokens'].apply(pd.Series)  \
    .merge(dfwords, right_index = True, left_index = True) \
    .drop(["tokens"], axis = 1) \
    .melt(id_vars = ['ts','Channel'], value_name = "word") \
    .drop("variable", axis = 1) \
    .dropna()
dfwords
dfwords['word'] = dfwords['word'].replace(to_replace =r'(<br\s*/><br\s*/>)|(\-)|(\/)|(\`)|(\\)', value = '', regex = True)

#dfwords['word'] = dfwords['word'].replace(to_replace =r'[.;:!\'?,\"()\[\]]', value = ' ', regex = True)
dfwords['word'] = dfwords['word'].astype(str).replace(to_replace ='[^A-z]+', value = '', regex = True) 
#Remove stop words
stop = stopwords.words('english')
dfwords['word'] = dfwords['word'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop)]))
#Remove user names and web addresses
dfwords['word'] = dfwords['word'].apply(lambda x: ' '.join([word for word in x.split() if word not in (user)]))
dfwords['word'] = dfwords['word'].apply(lambda x: ' '.join([word for word in x.split() if word not in (custom_drops)]))

#Stem #Use this or lemmatizing, not both
#stemmer = PorterStemmer()
#dfwords['word'] = dfwords['word'].apply(stemmer.stem)
#Lemmatization
lemmatizer = WordNetLemmatizer()
dfwords['word'] = dfwords['word'].apply(lemmatizer.lemmatize)


dfwords = dfwords[dfwords['word'] != ''] #Drop empty
dfwords['length'] = dfwords['word'].str.len()
dfwords = dfwords[dfwords['length'] < 19]
#dfwords
dfwords = dfwords.merge(df, how='left')
dfwords = dfwords[dfwords['word'] != dfwords['names']] #Drop join errors

#ftokens = list(flatten(tokens))

print(dfwords)
##########Streamlit
#st.title("Sentiment analysis of Slack Conversations")
#st.write(dfwords)
#print(ftokens)   
   
########### Output
dfwords.to_excel("predict.xlsx") #66050 before
os.startfile("predict.xlsx")
df.to_excel("predicttest.xlsx")
os.startfile("predicttest.xlsx")


#TO DO LIST:
    #Word list needs user and channel also strip punctuation and other stuff
    #Date time is in UTC needs to be converted to CST (including daylight savings)
