import telegram
import numpy as np
# позовляет потоки переправлять в буффер а не сохранять в файловую систему
import io
import seaborn as sns
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta

import numpy as np
import matplotlib.pyplot as plt

import warnings

warnings.simplefilter('ignore')

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# connetction BD
connection = {'host': 'https://clickhouse.lab.karpov.courses',
              'password': 'dpo_python_2020',
              'user': 'student',
              'database': 'simulator_20220820.feed_actions'
              }

# настройка бота
my_token = '5733759253:AAGclXnoYy-vcRt_iMXSzsZOERGLg95uBsM'
bot = telegram.Bot(token=my_token)
chat_id = 587930023



# Дефолтные параметры
default_args = {
    'owner': 'm-medvedeva-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 9),
}
 

# Интервал запуска DAG (Cron-выражение)
schedule_interval = '0 11 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def m_medvedeva_bot_2():
    @task()
    def extract_1():
        query_1 = """
        SELECT 
        round(countIf(user_id, action='like') / countIf(user_id, action='view'), 2) as CTR, 
        round(countIf(action ='like'),0) as likes, 
        round(countIf(action ='view'),0) as views,
        round(count(distinct user_id),0) as DAU,
        toDate(time) as event_date
        FROM simulator_20220820.feed_actions 
        WHERE toDate(time) = today()-1
        group by event_date
        """

        df_1 = ph.read_clickhouse(query_1, connection=connection)

        return df_1

    @task()
    def extract_2():
        query_2 = """
        SELECT count(distinct user_id) as DAU,
        toDate(time) as event_date,
        countIf(user_id, action='like') / countIf(user_id, action='view') as CTR, 
        countIf(action ='like') as likes, 
        countIf(action ='view') as views
        FROM simulator_20220820.feed_actions
        WHERE toDate(time) > today() -7 
        GROUP by event_date
        """

        df_2 = ph.read_clickhouse(query_2, connection=connection)

        return df_2

    @task()
    def transform_1(df_1, chat_id):
        msg = f'''Отчет за {(df_1.iloc[0]['event_date'].date())} 
        Метрики за день:
        CTR - {(df_1.iloc[0]['CTR'])} 
        likes - {df_1.iloc[0]['likes']} 
        views - {df_1.iloc[0]['views']} 
        DAU - {df_1.iloc[0]['DAU']} 
        '''

        # отправляем сообщение
        bot.sendMessage(chat_id=chat_id, text=msg)

    @task()
    def transform_2(df_2, chat_id):
        # Формируем графики  
        fig, axes = plt.subplots(2, 2, figsize=(14, 14))
        sns.set_style("darkgrid")
        fig.subplots_adjust(hspace=0.3)  
        fig.subplots_adjust(wspace=0.3)
        sns.lineplot(ax=axes[0, 0], data=df_2, x='event_date', y='DAU')
        axes[0, 0].set_title('DAU за последние 7 дней', fontsize=14, fontweight="bold")
        axes[0, 0].tick_params(axis='x', rotation=35)
        sns.lineplot(ax=axes[0, 1], data=df_2, x='event_date', y='CTR')
        axes[0, 1].set_title('CTR за последние 7 дней', fontsize=14, fontweight="bold")
        axes[0, 1].tick_params(axis='x', rotation=35)
        sns.lineplot(ax=axes[1, 0], data=df_2, x='event_date', y='likes')
        axes[1, 0].set_title('Лайки за последние 7 дней', fontsize=14, fontweight="bold")
        axes[1, 0].tick_params(axis='x', rotation=35)
        sns.lineplot(ax=axes[1, 1], data=df_2, x='event_date', y='views')
        axes[1, 1].set_title('Просмотры за последние 7 дней', fontsize=14, fontweight="bold")
        axes[1, 1].tick_params(axis='x', rotation=35)

        plot_object = io.BytesIO() 
        plt.savefig(plot_object) 
        plot_object.seek(0) 
        plot_object.name = 'test_dau.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    df_1 = extract_1()
    df_2 = extract_2()
    transform_1(df_1, chat_id)
    transform_2(df_2, chat_id)


m_medvedeva_bot_2 = m_medvedeva_bot_2()