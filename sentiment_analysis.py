import pandas as pd
import numpy as np
import re
from textblob import TextBlob
from deep_translator import GoogleTranslator
import emoji
from nltk.tokenize import word_tokenize
from posgres_conn import get_engine_from_settings

def remove_special_char(text):
    text = re.sub(r"[^a-zA-Z0-9\s]","", text)
    return text

def remove_emoji(text):
    new_text = re.sub(emoji.get_emoji_regexp(), r"", text)
    return new_text

def remove_multiple_char(text):
    text_token = word_tokenize(text)

    words = []
    for word in text_token:
        if re.search(r"(.)\1{2,}", word):
            new_word = re.split(r"(.)\1{2,}", word)
            new_word = ''.join(new_word)
        else:
            new_word = word
        words.append(new_word)
    new_text = ' '.join(words)
    return new_text

def translator(text):
  new_text = GoogleTranslator(source='id', target='en').translate(text)
  return new_text

def sentiment_score(text):
    testimonial = TextBlob(text)
    score = testimonial.sentiment[0]
    return score

def cleaning(df):
    #drop null
    df.dropna(inplace=True)
    
    #case folding
    df['content'] = df['content'].str.lower()

    #remove special character
    df['content'] = df['content'].apply(remove_special_char)

    #remove emoji
    df['content'] = df['content'].apply(remove_emoji)

    #remove multiple character
    df['content'] = df['content'].apply(remove_multiple_char)

    #remove blank row
    df = df.replace(r'^\s*$', float("NaN"), regex=True)
    df = df.replace(r'^([0-9]*)$', float("NaN"), regex=True)
    df = df.replace(r'^[A-Za-z]$', float("NaN"), regex=True)
    df.dropna(inplace=True)

    return df

def main():
    engine = get_engine_from_settings()

    #extract from postgresql
    query = open('./query_extract_sentiment.sql', 'r')
    df = pd.read_sql_query(query.read(), engine)
    query.close()
    
    #cleaning
    df = cleaning(df)

    #translate id to en
    df['content_en'] = df['content'].apply(translator)

    #calculate sentiment score
    df['sentiment_score'] = df['content_en'].apply(sentiment_score)

    #mapping sentiment score to label
    df['sentiment_label'] = pd.cut(x=df['sentiment_score'], bins=[-1,-0.000009,0.000009,1],
                        labels=['negative', 'neutral','positive'])

    #load to postgresql
    temp_df = df[['review_id','sentiment_score','sentiment_label']]
    temp_df.to_sql('temp_table', engine, if_exists='replace')

    query_update = """UPDATE reviews AS r
                SET sentiment_score = t.sentiment_score,
                    sentiment_label = t.sentiment_label
                FROM temp_table AS t
                WHERE r.review_id = t.review_id"""
    engine.execute(query_update)

if __name__=='__main__':
    main()