import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import pandas as pd
import requests
from datetime import date
import pandahouse as ph
import io
import sys
import os

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'simulator_20221020',
    'user': 'student',
    'password': 'dpo_python_2020'
}
# Зададим параметры
default_args = {
    'owner': 'a-lesihina', # Владелец операции 
    'depends_on_past': False, # Зависимость от прошлых запусков
    'retries': 1, # Кол-во попыток выполнить DAG
    'retry_delay': timedelta(minutes=1), # Промежуток между перезапусками
    'start_date': datetime(2022, 11, 19) # Дата начала выполнения DAG
                }

# Интервал запуска DAG
schedule_interval = '15 * * * *'

def check_anomalys(df, metric,a=3, n=6):
    df['25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['75'] - df['25']
    df['up'] = df['75'] + a*df['iqr']
    df['low'] = df['25'] - a*df['iqr']

    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()

    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    return is_alert, df

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def a_les_alert():
    
    @task()
    def extract_data():
        query = """select
                        t1.ts,
                        t1.date,
                        t1.hm,
                        t1.users_feed,
                        t1.views,
                        t1.likes,
                        t1.CTR,
                        t2.users_mess,
                        t2.sent_mess

                        from (
                        SELECT toStartOfFifteenMinutes(time) as ts,
                                                toDate(time) as date,
                                                formatDateTime(ts, '%R') as hm,
                                                uniqExact(user_id) as users_feed,
                                           countIf(action = 'view') as views, 
                                           countIf(action = 'like') as likes,
                                           (countIf(action = 'like')/countIf(action = 'view')) as CTR
                                        FROM 
                                            simulator_20221020.feed_actions 
                                        where 
                                            time >= today()-1 and time < toStartOfFifteenMinutes(now())

                                        group by ts, date, hm
                                        order by ts
                        ) t1
                        JOIN
                        (
                        SELECT toStartOfFifteenMinutes(time) as ts,
                                                    uniqExact(user_id) users_mess,
                                                    count(user_id) sent_mess

                                        FROM 
                                            simulator_20221020.message_actions 
                                        where 
                                            time >= today()-1 and time < toStartOfFifteenMinutes(now())

                                        group by ts
                                        order by ts
                        ) t2
                         on t1.ts = t2.ts

            """
        data = ph.read_clickhouse(query = query, connection=connection)
        return data
    
    @task()
    def run_alerts(data, chat=None):
        chat_id = chat or -817148946 #414021950
        my_token='5675394480:AAEkJqg0HF6_UeKl0nLZwkjRdrhwSsXLyU0'
        bot = telegram.Bot(token=my_token)
        metrics_list = ['users_feed','views', 'likes', 'CTR','users_mess','sent_mess']
        for metric in metrics_list:
            print(metric)
            df = data[['ts','date','hm', metric]].copy()
            is_alert, df = check_anomalys(df, metric)
            slope = 1 - (df[metric].iloc[-1]/df[metric].iloc[-2])
            if slope > 0:
                emoji = '📈'
                txt = 'Аномальный рост'
            else:
                emoji = '📉'
                txt = 'Аномальный спад'

            if is_alert == 1:
                msg = f'{txt}{emoji}\nМетрика {metric}:\nТекущее значение {round(df[metric].iloc[-1],2)}\nОтклонение от предыдущего значения {abs(round(1 - (df[metric].iloc[-1]/df[metric].iloc[-2]),2))}%\nhttps://superset.lab.karpov.courses/superset/dashboard/2215/'
                sns.set(rc={'figure.figsize' : (16,10)})
                plt.tight_layout()

                ax = sns.lineplot(x=df['ts'], y=df[metric], label=metric)
                ax = sns.lineplot(x=df['ts'], y=df['up'], label='up')
                ax = sns.lineplot(x=df['ts'], y=df['low'], label='low')

                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)
                ax.set(xlabel = 'time')
                ax.set(ylabel = metric)

                ax.set_title(metric)
                ax.set(ylim=(0, None))

                plot_object = io.BytesIO()
                plt.savefig(plot_object) 
                plot_object.seek(0)
                plot_object.name = f'{metric}.png'
                plt.close()

                bot.sendMessage(chat_id=chat_id, text = msg)
                bot.sendPhoto(chat_id=chat_id, photo = plot_object)
        return 
    data = extract_data()
    run_alerts(data=data)
a_les_alert = a_les_alert()
