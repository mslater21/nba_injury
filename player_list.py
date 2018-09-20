from bs4 import BeautifulSoup
import requests
from time import sleep
from random import randint


def make_player_id(first_name, last_name, id_list):
    if len(last_name) > 4:
        id = last_name[:5].lower()
    else:
        id = last_name
    id = id + first_name[:2].lower()
    if id in id_list:
        id = id + "02"
    else:
        id_list.append(id)
        id = id + "01"
    return id


letters = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
           'w', 'y', 'z']

player_list = []

file = open('id_list.txt', 'w')

for letter in letters:

    page = requests.get("https://www.basketball-reference.com/players/" + letter + "/")

    soup = BeautifulSoup(page.text, 'html.parser')

    player_table = soup.body.find_all('table', id='players')[0].tbody.find_all('tr')

    for player in player_table:
        if int(player.find_all('td')[1].text) >= 2000:
            player_list.append(player.th['data-append-csv'])
            file.write('%s\n' % player.th['data-append-csv'])

    print(letter + ' ' + str(len(player_list)))

    sleep(randint(5, 8))
