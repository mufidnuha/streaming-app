import ssl
ssl._create_default_https_context = ssl._create_unverified_context
from google_play_scraper import Sort, reviews_all
import pandas as pd
import numpy as np
import argparse
from posgres_conn import get_engine_from_settings

def main(params):
    app_id = params.app_id
    app_name = params.app_name
    language = params.language
    country = params.country
    table_name = 'reviews'

    #ingest all the newest reviews in google play
    results = reviews_all(
        app_id=app_id,
        sleep_milliseconds=0, # defaults to 0
        lang=language, # defaults to 'en'
        country=country, # defaults to 'us'
        sort=Sort.NEWEST, # defaults to Sort.MOST_RELEVANT
    )

    #create dataframe and transform
    df = pd.DataFrame(np.array(results),columns=['review'])
    df = df.join(pd.DataFrame(df.pop('review').tolist()))
    df['language'] = language
    df['country'] = country
    df['app_name'] = app_name
    df['sentiment_score'] = 0.0
    df['sentiment_label'] = None
    df['source'] = 'google play store'

    df = df.rename(columns={"reviewId":"review_id",
                            #"userName":"username",
                            "reviewCreatedVersion":"version",
                            "at":"date"})
                            #"thumbsUpCount":"tumbs_up_count",
                            #"replyContent":"replied_content",
                            #"repliedAt":"replied_date",
                            #"content":"content_text"})
    df = df[['review_id','date','app_name','version','source','language','country','content','score','sentiment_score','sentiment_label']]
    df['review_id'] = df['review_id'].str[3:]

    #load to postgresql
    engine = get_engine_from_settings()
    #df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')
    print("Success load {num_reviews} {app_name} reviews from Google Play Store to PostgreSQL".format(num_reviews=df.shape[0], app_name=app_name))

if __name__=='__main__':

    parser = argparse.ArgumentParser(description='Ingest Review on Google Play to PostgreSQL')
    parser.add_argument('--app_id', help='id of application in google play store') #ex: 'com.tencent.qqlivei18n'
    parser.add_argument('--app_name', help='name of application') #ex: 'wetv'
    parser.add_argument('--language', help='id language of reviews in google play store') #ex: 'id for indonesian
    parser.add_argument('--country', help='id country of reviews in google play store') #ex: 'id' for indonesia

    args = parser.parse_args()
    main(args)