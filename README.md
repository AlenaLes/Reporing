# Настройка отправки отчетности и алертов в  telegram

## Краткое описание

Проект направлен на ежедневную отправку информации по основным метрикам в telegram в установленное время. Данные взяты из приложения, объединяющее ленту новостей и сообщений. Также в чат telegram направляются алерты с информации от отклоении метрик. 

Стэк:

- JupiterHub
- Clickhouse
- Airflow
- Superset
- Python

## Ежедневная отчетность по ленте новостей

Подклоючаемся к базе данных и сразу установим все данные для отправки итоговых отчетов в нужный чат. Отчеты за полный вчерашний день будут приходить каждый день в 11:00 по МСК.

![image](https://user-images.githubusercontent.com/100629361/207143473-1388d215-7915-49e0-bd58-4fd34c91fcf2.png)

В Jupiter notebook можно посмотреть подробную настройку графиков и способ подсчеа метрик.

В результате ежедневно получаем следующий отчет:
![image](https://user-images.githubusercontent.com/100629361/207144872-ff32a48a-23a5-4349-bd34-1ad715e436ff.png)

## Ежедневная отчетность по ленте новостей и сообщениям
Данные для таблцы получаем тем же образом, что и на предыдущей отчетности.

В данной отчетности отражается информация:

- значения ключевых метрик за предыдущий день,
- график с значениями метрик за предыдущие 7 дней, 
- отражены метрики: DAU, Просмотры, Лайки, CTR

В результате получаем следующую отчетность:

![image](https://user-images.githubusercontent.com/100629361/207147146-f7b039cf-d7fa-4047-a0e9-8d0e5f2dd667.png)

![image](https://user-images.githubusercontent.com/100629361/207147232-86a37f3e-e6a7-40ba-8922-cd38d62d29e4.png)
![image](https://user-images.githubusercontent.com/100629361/207147256-2ca8e85b-401d-4eed-b5cb-f35eefc51a7f.png)

## Настройка алертов

Система с периодичностью в 15 минут проверяет ключевые метрики, такие как активные пользователи в ленте / мессенджере, просмотры, лайки, CTR, количество отправленных сообщений. 

В случае обнаружения аномального значения, в чат направляется алерт и график.

Используем квартили для определения верхнийх и нижних допустимых значений.

![image](https://user-images.githubusercontent.com/100629361/207148644-d5615656-f16d-49c4-8365-c48a9ae75bcc.png)

После с помощью SQL объединяем данные в одну таблицу для создания единого таска и настраиваем внешний вид сообщения и графиков. На выходе полчаем следующее:

![image](https://user-images.githubusercontent.com/100629361/207148813-e684c6c8-45dd-4538-9867-abcc1cd8e631.png)

