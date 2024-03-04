import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Зададим параметры
default_args = {
    'owner': 'a-lesihina', # Владелец операции 
    'depends_on_past': False, # Зависимость от прошлых запусков
    'retries': 1, # Кол-во попыток выполнить DAG
    'retry_delay': timedelta(minutes=1), # Промежуток между перезапусками
    'start_date': datetime(2022, 11, 15) # Дата начала выполнения DAG
                }

# Интервал запуска DAG'а каждый день в 11:00
schedule_interval = '0 11 * * *'

#указываем номер ID чата с ботом
chat_id = -817148946
#chat_id = 414021950
my_token = '5675394480:AAEkJqg0HF6_UeKl0nLZwkjRdrhwSsXLyU0'
bot = telegram.Bot(token=my_token)


connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'simulator_20221020',
    'user': 'student',
    'password': 'dpo_python_2020'
}

q_1 = """
    SELECT user_id,
        sum(action = 'like') as likes,
        sum(action = 'view') as views
    FROM simulator_20221020.feed_actions 
    WHERE toDate(time) = yesterday()
    GROUP BY 1
    """

q_2 = """
    SELECT toDate(time) as date,
      sum(action = 'like') as likes,
      sum(action = 'view') as views,
      COUNT(DISTINCT user_id) as users,
      likes / views as ctr
    FROM simulator_20221020.feed_actions 
    WHERE date between yesterday() - 7 and yesterday()
    GROUP BY 1
    """

q_3 = """
    SELECT toDate(time) as date,
      COUNT(DISTINCT user_id) as users
    FROM simulator_20221020.message_actions 
    WHERE date between yesterday() - 7 and yesterday()
    GROUP BY 1
    """


# Функция для DAG'а
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def a_les_full_dag():

    @task
    def extract_daily(q_1):
        df = ph.read_clickhouse(q_1, connection=connection)
        return df
    
    @task
    def transform_and_send_daily(df):
        dau = df.shape[0]
        likes = df['likes'].sum()
        views = df['views'].sum()
        ctr = likes / views

        msg = f'''
        Кол-во посетителей за вчерашний день: {dau}.
        Просмотрено постов: {views}.
        Поставлено лайков: {likes}.
        CTR за вчера: {ctr:.2f}.
        '''
        
        bot.sendMessage(chat_id=chat_id, text=msg)

    @task
    def extract_weekly_feed(q_2):
        df = ph.read_clickhouse(q_2, connection=connection)
        return df

    @task
    def transform_and_send_weekly_feed(df):
        for metric in ['users', 'views', 'likes', 'ctr']:
            sns.lineplot(data=df, x='date', y=metric)
            plt.xticks(rotation=20)
            plt.title(metric)
            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = metric + '.png'
            plt.close()
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)
            
    @task
    def extract_weekly_msg(q_3):
        df = ph.read_clickhouse(q_3, connection=connection)
        return df
    
    @task
    def transform_and_send_weekly_msg(df):
        metric = 'users'
        sns.lineplot(data=df, x='date', y=metric)
        plt.xticks(rotation=20)
        plt.title(metric + ' use messages')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = metric + '.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
            
            
    df_daily = extract_daily(q_1)
    transform_and_send_daily(df_daily)
    df_weekly_feed = extract_weekly_feed(q_2)
    transform_and_send_weekly_feed(df_weekly_feed)
    df_weekly_msg = extract_weekly_msg(q_3)
    transform_and_send_weekly_msg(df_weekly_msg)
        
a_les_full_dag = a_les_full_dag()
