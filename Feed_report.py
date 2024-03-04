#бибилиотеки
import io
import matplotlib.pyplot as plt
import pandas as pd
import pandahouse as ph
import seaborn as sns
import telegram

from airflow.decorators import dag, task
from datetime import date, datetime, timedelta

#подключаемся к базе данных
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20221020',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }
# Зададим параметры
default_args = {
    'owner': 'a-lesihina', # Владелец операции 
    'depends_on_past': False, # Зависимость от прошлых запусков
    'retries': 1, # Кол-во попыток выполнить DAG
    'retry_delay': timedelta(minutes=1), # Промежуток между перезапусками
    'start_date': datetime(2022, 11, 15) # Дата начала выполнения DAG
                }

# Установим расписание
schedule_interval = '0 11 * * *' 

#определяем токен бота
my_token = '5675394480:AAEkJqg0HF6_UeKl0nLZwkjRdrhwSsXLyU0'
bot = telegram.Bot(token=my_token)

#указываем номер ID чата с ботом
chat_id = -817148946
#chat_id = 414021950

#достаем данные за вчера
dates = """
    select 
        toDate(time) as days,
        count(distinct user_id) as DAU,
        countIf(action='view') as views,
        countIf(action='like') as likes,
        countIf(action='like')/countIf(action='view')*100 as CTR
    from simulator_20221020.feed_actions 
    where toDate(time) >= today() - 7 and toDate(time) <= yesterday()
    group by toDate(time)
    order by 1
    """

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def a_lesihina_bot():

    @task
    def my_bot_report():
        df = ph.read_clickhouse(dates, connection=connection)
        msg = '''Cтатистика за вчерашний день:\nDAU: {DAU}\nViews: {views}\nLikes: {likes}\nCTR: {CTR:.2%}'''\
        .format(days=str(df.days[0]).split(' ')[0],
            DAU     = df.DAU[0],
            views   = df.views[0],
            likes   = df.likes[0],
            CTR     = df.CTR[0])  
        bot.sendMessage(chat_id=chat_id, text=msg)      
        
        
        df_2 = ph.read_clickhouse(dates, connection=connection)
        fig, ax = plt.subplots(2, 2, figsize=(25, 20))
        ax[0, 0].plot(df_2['days'], df_2['DAU'], marker="o", label="uniqal users", color = 'indigo')
        ax[0, 0].set_xlabel('date')
        ax[0, 0].set_ylabel('DAU')
        ax[0, 0].set_title('DAU за прошедшую неделю', fontsize=20, fontweight="bold")

        ax[0, 1].plot(df_2['days'], df_2['CTR'], marker="o", label="uniqal users", color = 'darkorange')
        ax[0, 1].set_xlabel('date')
        ax[0, 1].set_ylabel('CTR')
        ax[0, 1].set_title('CTR за прошедшую неделю', fontsize=20, fontweight="bold")

        ax[1, 0].plot(df_2['days'], df_2['views'], marker="o", label="uniqal users", color = 'crimson')
        ax[1, 0].set_xlabel('date')
        ax[1, 0].set_ylabel('views')
        ax[1, 0].set_title('Views за прошедшую неделю', fontsize=20, fontweight="bold")

        ax[1, 1].plot(df_2['days'], df_2['likes'], marker="o", label="uniqal users", color = 'green')
        ax[1, 1].set_xlabel('date')
        ax[1, 1].set_ylabel('likes')
        ax[1, 1].set_title('Likes за прошедшую неделю', fontsize=20, fontweight="bold")

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'my_report_bot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
    my_bot_report()
    
a_lesihina_bot = a_lesihina_bot()
