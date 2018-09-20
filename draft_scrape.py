import requests
from bs4 import BeautifulSoup
import pymysql
from time import sleep
from random import randint
import config


def open_connection():
    hostname = config.HOST
    username = config.USER
    password = config.PASS
    database = config.DB
    port = 3306
    return pymysql.connect(hostname, username, password, database, port)


def set_draft(scraped_draft, dictionary):
    draft_text = scraped_draft.parent.text.split(',')
    draft_number = int(''.join(filter(str.isdigit, draft_text[2].split()[0])))
    dictionary['draft_num'] = draft_number
    draft_year = int(draft_text[3].split()[0].strip())
    dictionary['draft_year'] = draft_year


def commit_data(connection, data):
    cursor = connection.cursor()
    sql = """UPDATE nbaProfile SET draftNum=%s, draftYear=%s where id=%s"""
    if data['draft_num'] is not None:
        cursor.execute(sql, (data['draft_num'], data['draft_year'], data['id']))

    connection.commit()


conn = open_connection()

file = open('id_list.txt', 'r')  # Open list of player ids

players = file.read().splitlines()  # Remove newlines from ids

for player in players[10:]:

    print(player)

    data_fields = ['id', 'draft_num', 'draft_year']

    data_results = {key: None for key in data_fields}  # Populate dictionary with empty values

    data_results['id'] = player

    page = requests.get("https://www.basketball-reference.com/players/" + player[:1] + "/" + player + ".html")

    soup = BeautifulSoup(page.text, 'html.parser')

    info = soup.body.find('div', itemtype='https://schema.org/Person')  # Isolate profile information

    scraped_fields = info.find_all('strong')[2:-1]  # Select all fields that have a bold section label

    for field in scraped_fields:
        field_label = field.text.strip()[:-1].lower()
        if field_label == 'draft':
            set_draft(field, data_results)

    commit_data(conn, data_results)

    sleep(randint(5, 8))

conn.close()
