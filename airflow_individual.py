#from airflow import DAG
from marquez_airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import BaseOperator
from airflow.operators.dummy_operator import DummyOperator 
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook

import tweepy
import pandas as pd
import io
import json
from datetime import datetime, timedelta, date

import requests
from bs4 import BeautifulSoup
from time import sleep

import logging

log = logging.getLogger(__name__)


# =============================================================================
# 1. Set up the main configurations of the dag
# =============================================================================
default_args = {
    'start_date': datetime(2021, 4, 30),
    'owner': 'Airflow',
    'filestore_base': '/tmp/airflowtemp/',
    'bucket_name': 'ucl-msin0166-2021-mwaa',
#    'prefix': 'test_folder',
    'db_name': Variable.get("schema_postgres_bingyu", deserialize_json=True)['db_name'],
    'aws_conn_id': "aws_default",
    'consumer_key':"mT81o4cCwLhB3v8T19obCLl9m",
    'consumer_secret':"khIx52CeapnNU1Ux8NjJig2hNzAux7hQBol2q7ZfMzAHC2fSa9",
    'access_token': "712363196-buAMU1eDePCkbjWhtT8BAlry2et9SeA6oY8CWRPN",
    'access_token_secret':"rDXek6tRjNO9nrw43fSKxhaQlxzu0rsiYzAiNUSqvLZHi",   
    'postgres_conn_id': 'postgres_conn_id_bingyu',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG('airflow_individual',
          description='test for twitter web scraping',
          schedule_interval='@weekly', # cron scheduling for more detailed time
          catchup=False,
          default_args=default_args,
          max_active_runs=1)

# =============================================================================
# 2. Define different functions
# =============================================================================
def scrape_data_twitter(**kwargs):

    # authorization of consumer key and consumer secret
    auth = tweepy.OAuthHandler(kwargs['consumer_key'], kwargs['consumer_secret'])
    
    # set access to user's access key and access secret 
    auth.set_access_token(kwargs['access_token'], kwargs['access_token_secret'])
    
    # calling the api 
    api = tweepy.API(auth)

    log.info('Start scraping')

    #task_instance = kwargs['ti']
    # tweets features
    tweet_id=[]
    user_id=[]
    text=[]
    retweeted=[]
    favorited=[]
    url=[]
    created_at=[]
    # user features
    screen_name=[]
    name=[]
    location=[]
    description=[]
    followers=[]
    friends=[]
    listed=[]
    user_created_at=[]
    favourites=[]
    verified=[]
    statuses=[]
    profile_bg_image=[]

    # Retweets and replies exclusive, until today (inclusive)
    until_date = (date.today()+timedelta(days = 1)).strftime("%Y-%m-%d")
    query = 'Gloomhaven -filter:retweets -filter:replies'
    for i in tweepy.Cursor(api.search, q = query,lang="en",until=until_date, tweet_mode = 'extended').items():
        # tweets
        tweet_id.append(i.id_str)
        user_id.append(i.user.id_str)
        text.append(i.full_text)
        retweeted.append(i.retweet_count)
        favorited.append(i.favorite_count)
        url.append(i.entities['urls'][0]['url']) if len(i.entities['urls'])>0 else url.append(None)
        created_at.append(i.created_at.strftime('%Y-%m-%d %H:%M:%S'))
        # users
        screen_name.append(i.user.screen_name)
        name.append(i.user.name)
        location.append(i.user.location)
        description.append(i.user.description)
        followers.append(i.user.followers_count)
        friends.append(i.user.friends_count)
        listed.append(i.user.listed_count)
        favourites.append(i.user.favourites_count)
        statuses.append(i.user.statuses_count)
        verified.append(i.user.verified)
        profile_bg_image.append(i.user.profile_use_background_image)
        user_created_at.append(i.user.created_at.strftime('%Y-%m-%d %H:%M:%S'))
    
    log.info('Scraping twitter finished')
    
    df_tweets = pd.DataFrame({'tweet_id':tweet_id,'user_id':user_id,'text':text,'retweeted':retweeted,'favorited':favorited,'url_in_text':url,'created_at':created_at})
    #df_tweets = df_tweets.to_json()
    
    df_users = pd.DataFrame({'user_id':user_id,'screen_name':screen_name,'name':name,'location':location,'description':description,'followers':followers,'friends':friends,'listed':listed,'favourites':favourites,'statuses':statuses,'verified':verified,'profile_bg_image':profile_bg_image,'created_at':user_created_at})
    df_users = df_users.drop_duplicates(['user_id']).reset_index(drop=True)
    #df_users = df_users.to_json()

    return df_tweets.values.tolist(),df_users.values.tolist()



def scrape_data_bgg(**kwargs):
    # Deal with exceptions for requests
    def request(msg, slp=1):
        '''A wrapper to make robust https requests.'''
        status_code = 500  
        while status_code != 200:
            sleep(slp) 
            try:
                r = requests.get(msg)
                status_code = r.status_code
                if status_code != 200:
                    print("Server Error! Response Code %i. Retrying..." % (r.status_code))
            except:
                print("An exception has occurred, waiting one seconds...")
                sleep(1)
        return r
    
    import sys
    sys.setrecursionlimit(1000000)
    
    # Initialize a dataframe
    df_all = pd.DataFrame(columns=["rank","game_id", "title","geek_rating","avg_rating","votes", "pic_url"])
    npage = 1
    pages = 2
    # Scrape the first several pages
    while npage<=pages:
        # Get full HTML for a specific page in the full list
        r = request("https://boardgamegeek.com/browse/boardgame/page/%i" % (npage))
        soup = BeautifulSoup(r.text, "html.parser")    
        
        # Get rows on this page
        table = soup.find_all("tr", attrs={"id": "row_"})
        df = pd.DataFrame(columns=["rank","game_id", "title","geek_rating","avg_rating","votes", "pic_url"], index=range(len(table)))
        
        # Loop through rows
        for idx, row in enumerate(table):
            # Row may or may not start with a "boardgame rank" link, if YES then strip it
            links = row.find_all("a")
            if "name" in links[0].attrs.keys():
                del links[0]
            
            gamelink = links[1]  # URL
            game_id = int(gamelink["href"].split("/")[2])  # Game ID
            game_name = gamelink.contents[0]  # Game name
            imlink = links[0]
            pic_url = imlink.contents[0]["src"]  # URL of thumbnail
            
            # Rank
            rank_str=row.find_all("td", attrs={"class": "collection_rank"})[0].contents[2]
            rank=int("".join(rank_str.split()))
            # Geek rating
            geek_rating_str = row.find_all("td", attrs={"class": "collection_bggrating"})[0].contents[0]
            geek_rating = eval("".join(geek_rating_str.split()))
            # Average rating
            avg_rating_str = row.find_all("td", attrs={"class": "collection_bggrating"})[1].contents[0]
            avg_rating = eval("".join(avg_rating_str.split()))
            # Number of Voters
            votes_str = row.find_all("td", attrs={"class": "collection_bggrating"})[2].contents[0]
            votes = int("".join(votes_str.split()))
            
            df.iloc[idx,:] = [rank, game_id, game_name, geek_rating, avg_rating, votes,pic_url]

        df_all = pd.concat([df_all, df], axis=0)
        npage += 1
        sleep(2)
    log.info('Scraping BGG finished')
    df_all=df_all.reset_index(drop=True)
    #df_all = df_all.to_json()
    return df_all.values.tolist()


def save_result_to_postgres_db(**kwargs):

    # Get the task instance
    task_instance = kwargs['ti']
    print(task_instance)

    # Get the output of twitter
    scraped_tweets,scraped_users = task_instance.xcom_pull(task_ids="scrape_data_twitter_task")
    
    log.info('xcom from scrape_data_twitter:{0}'.format(scraped_tweets))
    log.info('xcom from scrape_data_twitter:{0}'.format(scraped_users))

    # Get the output of game rank
    scraped_game_rank = task_instance.xcom_pull(task_ids="scrape_data_bgg_task")
    
    log.info('xcom from scrape_data_twitter:{0}'.format(scraped_game_rank))
    
    
    # Connect to the database
    pg_hook = PostgresHook(postgres_conn_id=kwargs["postgres_conn_id"], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    log.info('Initialised connection')
            
    # usesrs
    s1 = """INSERT INTO twitter.users(user_id,screen_name,name,location,description,followers,friends,listed,favourites,statuses,verified,profile_bg_image,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (user_id) DO UPDATE SET (user_id,screen_name,name,location,description,followers,friends,listed,favourites,statuses,verified,profile_bg_image,created_at)=(EXCLUDED.user_id,EXCLUDED.screen_name,EXCLUDED.name,EXCLUDED.location,EXCLUDED.description,EXCLUDED.followers,EXCLUDED.friends,EXCLUDED.listed,EXCLUDED.favourites,EXCLUDED.statuses,EXCLUDED.verified,EXCLUDED.profile_bg_image,EXCLUDED.created_at);"""
    cursor.executemany(s1, scraped_users)
    conn.commit()

    log.info('Finished saving users data to database')    
    
    # tweets
    s2 = """INSERT INTO twitter.tweets(tweet_id,user_id,text,retweeted,favourited,url_in_text,created_at) VALUES (%s, %s, %s, %s,%s,%s,%s) ON CONFLICT (tweet_id) DO UPDATE SET (tweet_id,user_id,text,retweeted,favourited,url_in_text,created_at)=(EXCLUDED.tweet_id,EXCLUDED.user_id,EXCLUDED.text,EXCLUDED.retweeted,EXCLUDED.favourited,EXCLUDED.url_in_text,EXCLUDED.created_at);"""
    cursor.executemany(s2, scraped_tweets)
    conn.commit()

    log.info('Finished saving the scraped tweets to database')

    # BGG game rank
    s3 = """INSERT INTO games.game_rank(rank,game_id,title,geek_rating,avg_rating,votes,pic_url) VALUES (%s, %s, %s, %s,%s,%s,%s) ON CONFLICT (game_id) DO UPDATE SET (rank,game_id,title,geek_rating,avg_rating,votes,pic_url)=(EXCLUDED.rank,EXCLUDED.game_id,EXCLUDED.title,EXCLUDED.geek_rating,EXCLUDED.avg_rating,EXCLUDED.votes,EXCLUDED.pic_url);"""
    cursor.executemany(s3, scraped_game_rank)
    conn.commit()
    conn.close()
         

# =============================================================================
# 3. Set up the main configurations of the dag
# =============================================================================                        
scrape_data_twitter_task = PythonOperator(
    task_id='scrape_data_twitter_task',
    provide_context=True,
    python_callable=scrape_data_twitter,
    op_kwargs=default_args,
    dag=dag)

scrape_data_bgg_task = PythonOperator(
    task_id='scrape_data_bgg_task',
    provide_context=True,
    python_callable=scrape_data_bgg,
    op_kwargs=default_args,
    dag=dag)

save_result_to_postgres_db_task = PythonOperator(
    task_id='save_result_to_postgres_db_task',
    provide_context=True,
    python_callable=save_result_to_postgres_db,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    op_kwargs=default_args,
    dag=dag)



# =============================================================================
# 4. Indicating the order of the dags
# =============================================================================

scrape_data_twitter_task >> scrape_data_bgg_task >> save_result_to_postgres_db_task