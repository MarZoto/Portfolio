import pandas as pd
import pandahouse as ph
import numpy as np
from airflow.decorators import dag, task
from datetime import datetime, timedelta


connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20220820',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }

connection_test = {'host': 'https://clickhouse.lab.karpov.courses', 
                           'password': '656e2b0c9c',
                           'user': 'student-rw',
                           'database': 'test'}


default_args = {
    'owner': 'm-medvedeva-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 6)}

schedule_interval = '0 22 * * *'

def cat(x): 
    
    if x < 20:
        category = '< 20'
    
    elif 20 <= x < 30:
        category = '20 - 30'
    elif 30<= x < 45:    
        category = '30 - 45'
    else:
        category = '45+'
        
    return category


@dag(default_args=default_args,
     schedule_interval=schedule_interval,
     catchup=False)
def dag_m_medvedeva_7():
    
    @task()
    def extract_1():
        query_1 = '''
                SELECT
                    user_id,
                    toDate(time) as event_date,
                    age,
                    gender,
                    os, 
                    countIf(action = 'like') as likes,
                    countIf(action = 'view') as views
                FROM
                    simulator_20220820.feed_actions
                WHERE
                    toDate(time) = today() - 1
                GROUP BY
                    user_id,
                    event_date,
                    age,
                    gender,
                    os
                '''
        df_all_likes_views = ph.read_clickhouse(query=query_1, connection=connection)

        return df_all_likes_views

    @task()
    def extract_2():
        query_2 = '''
                SELECT
                     *
                FROM
                    (SELECT
                        user_id,
                        gender,
                        age,
                        os,
                        toDate(time) as event_date,
                        COUNT(reciever_id) as messages_sent,
                        COUNT(DISTINCT reciever_id) as users_sent
                    FROM
                        simulator_20220820.message_actions
                    WHERE
                        toDate(time) = today() - 1
                    GROUP BY
                        user_id,
                        gender,
                        age,
                        os,
                        event_date) as s  

                    FULL OUTER JOIN

                        (SELECT
                            reciever_id as user_id_,
                            COUNT(user_id) as messages_received,
                            COUNT(DISTINCT user_id) as users_received
                        FROM
                            simulator_20220820.message_actions
                        WHERE
                            toDate(time) = today() - 1
                        GROUP BY
                            user_id_) as r
                        ON s.user_id = r.user_id_
                    '''
        df_all_messages = ph.read_clickhouse(query=query_2, connection=connection)

        return df_all_messages

    @task()
    def concat(df_all_likes_views, df_all_messages):
        df = df_all_likes_views.merge(df_all_messages, 
                           on = ['user_id', 'gender', 'age', 'os', 'event_date'], 
                           how = 'outer')\
                    .drop('user_id_', axis=1)

        df = df.loc[df['user_id'] != 0].fillna(0)

        return df

    @task()
    def gender(df):
        df_gender = df.groupby(['event_date', 'gender'], as_index = False)['likes', 'views', 
                                                                           'messages_sent','users_sent',
                                                                           'messages_received', 'users_received'].sum()
        df_gender['dimension'] = 'gender'

        return df_gender.rename({'gender':'dimension_value'}, axis=1)

    @task()
    def os(df):
        df_os = df.groupby(['event_date', 'os'], as_index = False)['likes', 'views', 
                                                                   'messages_sent', 'users_sent', 
                                                                   'messages_received', 'users_received'].sum()
        df_os['dimension'] = 'os'

        return df_os.rename({'os':'dimension_value'}, axis=1)

    @task()
    def age(df):

        df['age'] = df['age'].apply(cat)

        df_age = df.groupby(['event_date', 'age'], as_index = False)['likes', 'views', 
                                                                     'messages_sent', 'users_sent', 
                                                                     'messages_received', 'users_received'].sum()
        df_age['dimension'] = 'age'

        return df_age.rename({'age':'dimension_value'}, axis=1)

    @task
    def concat_2(df_gender, df_os, df_age):
        final_df = pd.concat([df_gender, df_os, df_age], axis=0)
        final_df = final_df[['event_date', 'dimension_value', 'dimension', 
                             'likes', 'views', 'messages_sent', 'users_sent', 
                             'messages_received', 'users_received']]
        final_df['likes'] = final_df['likes'].astype(int) 
        final_df['views'] = final_df['views'].astype(int) 
        final_df['messages_sent'] = final_df['messages_sent'].astype(int)
        final_df['messages_received'] = final_df['messages_received'].astype(int)
        final_df['users_received'] = final_df['users_received'].astype(int)
        final_df['users_sent'] = final_df['users_sent'].astype(int)
         

        
        return final_df  
    
    @task()
    def load(final_df):
                       
               
        create_table = '''CREATE TABLE IF NOT EXISTS test.m_medvedeva
            (
            event_date DATE,
            dimension String,
            dimension_value String,
            views UInt64,
            likes UInt64,
            messages_received UInt64,
            messages_sent UInt64,
            users_received UInt64,
            users_sent UInt64
            ) ENGINE = MergeTree()
            ORDER BY (event_date);
            '''

        ph.execute(query=create_table, connection=connection_test)
        ph.to_clickhouse(df=final_df, 
                                 table='m_medvedeva', 
                                 index=False, 
                                 connection=connection_test)

                
    df_all_likes_views= extract_1()
    df_all_messages = extract_2()
    df = concat(df_all_likes_views, df_all_messages)
    df_gender = gender(df)
    df_os = os(df)
    df_age = age(df)
    final_df = concat_2(df_gender, df_os, df_age)
    load(final_df)

dag_m_medvedeva_7 = dag_m_medvedeva_7()