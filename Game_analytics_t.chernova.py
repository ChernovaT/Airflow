import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

import json
from urllib.parse import urlencode

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

login = 't.chernova-16'
year = 1994 + hash(f'{login}') % 23
file_name = 'vgsales.csv'

default_args = {
    'owner': 't.chernova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 14),
    'schedule_interval': '0 12 * * *'
}

def send_message(context):
    token = '2004470053:AAH5Vzr5t0GZW5kZOsT7TOmDN4xN5fv6ec8'
    chat_id = 1032470824
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    params = {'chat_id': chat_id, 'text': message}
    base_url = f'https://api.telegram.org/bot{token}/'
    url = base_url + 'sendMessage?' + urlencode(params)
    resp = requests.get(url)
    

@dag(default_args=default_args, catchup=False)
def game_analytics():
    @task()
    def get_data_for_year():
        data = pd.read_csv(file_name)
        data = data.rename(columns= lambda c: c.lower())
        data_for_year = data.query('year == @year')
        return data_for_year
    
    @task()
    def best_game(data_for_year):
        most_sales_game = (
                            data_for_year.groupby('name', as_index=False)
                            .agg({'global_sales': 'sum'})
                            .sort_values(by='global_sales', ascending=False)
                            .head(1)
                            .name
                           )
        return most_sales_game
    
    @task()
    def top_eu_sales_genre(data_for_year):
        eu_sales_by_genre = data_for_year.groupby('genre', as_index=False).agg({'eu_sales': 'sum'})
        eu_sales_max = eu_sales_by_genre.eu_sales.max()
        top_genre_in_eu = eu_sales_by_genre[eu_sales_by_genre.eu_sales == eu_sales_max].genre
        return top_genre_in_eu
    
    @task()
    def top_platform_na(data_for_year):
        na_sales_more_million = (
                                data_for_year.groupby(['platform','name'], as_index=False)
                                .agg({'na_sales': 'sum'})
                                .query('na_sales > 1')
        )
        platform_na = na_sales_more_million.groupby('platform', as_index=False).agg({'name': 'count'})
        top_platform_na = platform_na[platform_na.name == platform_na.name.max()].platform
        return top_platform_na
    
    @task
    def top_pablishers_by_avg_sales_jp(data_for_year):
        jp_avg_sales = data_for_year.groupby('publisher', as_index=False).agg({'jp_sales': 'mean'})
        jp_max_avg_sales = jp_avg_sales.jp_sales.max()
        jp_top_publishers = jp_avg_sales[jp_avg_sales.jp_sales == jp_max_avg_sales].publisher
        return jp_top_publishers
    
    @task
    def num_games_eu_vs_jp(data_for_year):
        better_sales_eu_vs_jp = (
                        data_for_year.groupby('name')
                        .agg({'eu_sales': 'sum', 'jp_sales': 'sum'})
                        .query('eu_sales > jp_sales')
                        )
        num_games = better_sales_eu_vs_jp.count()
        return num_games
    
    @task(on_success_callback=send_message)
    def print_data(most_sales_game, top_genre_in_eu, top_platform_na, jp_top_publishers, num_games):
        print(f'Cамая продаваемая игра в мире в {year} году: {most_sales_game.iloc[0]}')
        print(f'Самые популярные жанры игр в Европе в {year} году: {" ,".join(top_genre_in_eu.to_list())}')
        print(f'Больше всего игр, которые продались более чем миллионым тиражом, в Северной Америке в {year} году было на платформах: {" ,".join(top_platform_na.to_list())}')
        print(f'Самые высокие средние продажи в Японии в {year} году были у производителей: {" ,".join(jp_top_publishers.to_list())}')
        print(f'{num_games.eu_sales} игр продались лучше в Европе чем в Японии в {year} году')
    
    data_for_year = get_data_for_year()
    most_sales_game = best_game(data_for_year)
    top_genre_in_eu = top_eu_sales_genre(data_for_year)
    top_platform_na = top_platform_na(data_for_year)
    jp_top_publishers = top_pablishers_by_avg_sales_jp(data_for_year)
    num_games = num_games_eu_vs_jp(data_for_year)
    print_data(most_sales_game, top_genre_in_eu, top_platform_na, jp_top_publishers, num_games)


game_analytics = game_analytics()