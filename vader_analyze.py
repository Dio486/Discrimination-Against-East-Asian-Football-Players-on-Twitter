import pandas as pd
import requests
import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

def get_vader_scores(data, file_name):
    analyzer = SentimentIntensityAnalyzer()
    for i in data.index:
        vs = analyzer.polarity_scores(str(data.loc[i,'content']))
        data.loc[i, 'score'] = str(vs)
        data.loc[i, 'neg_prob'] = vs['neg']
        data.loc[i, 'neu_prob'] = vs['neu']
        data.loc[i, 'pos_prob'] = vs['pos']
        data.loc[i, 'compound_prob'] = vs['compound']
    data.to_excel('./data/twitter_{}_emotion.xlsx'.format(file_name), index=False)

df = pd.read_excel('./parameters.xlsx',engine='openpyxl')

files = df['path'].tolist()
print(files)
for file_name in files:
    data = pd.read_excel('./done/twitter_{}.xlsx'.format(file_name),engine='openpyxl')
    get_vader_scores(data, file_name)
    print('{} done'.format(file_name))
