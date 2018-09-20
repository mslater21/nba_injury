import requests
from bs4 import BeautifulSoup
import pymysql
import datetime
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


def set_position(scraped_position, dictionary):
    scraped_position = scraped_position.next_sibling[:-10].strip()
    position_text = "".join(item[0] for item in scraped_position.split())
    position_text = position_text.replace('a', '/')
    dictionary['position'] = position_text


def set_shooting(scraped_shooting, dictionary):
    dictionary['shoots'] = scraped_shooting.next_sibling.strip()


def set_birth_info(scraped_birth, dictionary):
    birthdate = scraped_birth.parent.find('span', itemprop='birthDate')['data-birth']
    split_birthdate = birthdate.split('-')
    if split_birthdate[1][0] == 0:
        month = int(split_birthdate[1][1])
    else:
        month = int(split_birthdate[1])
    if split_birthdate[2][0] == 0:
        day = int(split_birthdate[2][1])
    else:
        day = int(split_birthdate[2])
    year = int(split_birthdate[0])
    dictionary['born'] = datetime.date(year, month, day)
    birthplace = scraped_birth.parent.find('span', itemprop='birthPlace').text.strip()[3:]
    dictionary['birthplace'] = birthplace.replace('\xa0', ' ')


def set_college(scraped_college, dictionary):
    dictionary['college'] = scraped_college.parent.a.text


def set_high_school(scraped_high_school, dictionary):
    hs_text = scraped_high_school.next_sibling.strip()
    fixed_text = ''
    for word in hs_text.split():
        if word == 'in':
            break
        fixed_text = fixed_text + word + ' '
    dictionary['high school'] = fixed_text.strip()


def set_draft(scraped_draft, dictionary):
    draft_text = scraped_draft.parent.text.split(',')
    draft_number = int(''.join(filter(str.isdigit, draft_text[2].split()[0])))
    dictionary['draft_num'] = draft_number
    draft_year = int(draft_text[3].split()[0].strip())
    dictionary['draft_year'] = draft_year


def extract_data(section_label, contents, dictionary):
    switcher = {
        'position':
            set_position,
        'shoots':
            set_shooting,
        'born':
            set_birth_info,
        'college':
            set_college,
        'high school':
            set_high_school,
        'draft':
            set_draft
    }
    func = switcher.get(section_label)
    if func is not None:
        func(contents, dictionary)


def commit_data(connection, data):
    cursor = connection.cursor()
    sql = "INSERT INTO nbaProfile (id, firstName, lastName, height, weight, positions, shootingHand, " \
          "birthDate, birthPlace, college, highSchool, draftYear, draftNum)" \
          " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.execute(sql, (data['id'], data['first_name'], data['last_name'], data['height'], int(data['weight']),
                         data['position'], data['shoots'], data['born'], data['birthplace'], data['college'],
                         data['high school'], data['draft_year'], data['draft_num']))

    connection.commit()


conn = open_connection()

file = open('id_list.txt', 'r')  # Open list of player ids

players = file.read().splitlines()  # Remove newlines from ids

for player in players[10:]:

    data_fields = ['id', 'first_name', 'last_name', 'height', 'weight', 'position', 'shoots', 'born', 'birthplace',
                   'college', 'high school', 'draft_num', 'draft_year']

    data_results = {key: None for key in data_fields}  # Populate dictionary with empty values

    page = requests.get("https://www.basketball-reference.com/players/" + player[:1] + "/" + player + ".html")

    soup = BeautifulSoup(page.text, 'html.parser')

    info = soup.body.find('div', itemtype='https://schema.org/Person')  # Isolate profile information

    data_results['id'] = player

    data_results['first_name'] = info.h1.text.split(' ')[0]

    data_results['last_name'] = info.h1.text.split(' ')[1]

    data_results['height'] = info.find('span', itemprop='height').text

    data_results['weight'] = info.find('span', itemprop='weight').text.replace('lb', '')

    scraped_fields = info.find_all('strong')[2:-1]  # Select all fields that have a bold section label

    for field in scraped_fields:
        field_label = field.text.strip()[:-1].lower()
        extract_data(field_label, field, data_results)

    print(player)

    commit_data(conn, data_results)

    sleep(randint(5, 10))

conn.close()





