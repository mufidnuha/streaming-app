from app_store_scraper import AppStore
import pandas as pd
import numpy as np
import uuid
import argparse
from posgres_conn import get_engine_from_settings


def main(params):
    app_id = params.app_id #'wetv'
    app_name = params.app_name #'1441531611'
    country = params.country
    language = country
    source = 'apple store'
    table_name = 'reviews'

    #ingest all reviews in apple store
    results = AppStore(country=country, app_name=app_name, app_id=app_id)
    results.review(how_many=1000000)

    #create dataframe and transform
    df = pd.DataFrame(np.array(results.reviews),columns=['review'])
    df = df.join(pd.DataFrame(df.pop('review').tolist()))

    df['content'] = df['title'] + " " + df['review']
    df['review_id'] = df.apply(lambda _: str(uuid.uuid1()).replace("-", ""), axis=1)
    df['language'] = language
    df['country'] = country
    df['app_name'] = app_name
    df['source'] = source
    df['sentiment_score'] = 0.0
    df['sentiment_label'] = None
    df['version'] = None

    df = df.rename(columns={"rating":"score"})
    df = df[['review_id','date','app_name','version','source','language','country','content','score','sentiment_score','sentiment_label']]

    #load to postgresql
    engine = get_engine_from_settings()
    df.to_sql(name=table_name, con=engine, if_exists='append')
    print("Success load {num_reviews} {app_name} reviews from Apple Store to PostgreSQL".format(num_reviews=df.shape[0], app_name=app_name))

if __name__=='__main__':

    parser = argparse.ArgumentParser(description='Ingest Review on Apple Store to PostgreSQL')
    parser.add_argument('--app_id', help='id of application in apple store store') #ex: 'com.tencent.qqlivei18n'
    parser.add_argument('--app_name', help='name of application') #ex: 'wetv'
    parser.add_argument('--country', help='id country of reviews in apple store store') #ex: 'id' for indonesia

    args = parser.parse_args()
    main(args)
