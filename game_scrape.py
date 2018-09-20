import requests
from bs4 import BeautifulSoup
import pymysql
from time import sleep
from random import randint
import config
import datetime


def open_connection():
    hostname = config.HOST
    username = config.USER
    password = config.PASS
    database = config.DB
    port = 3306
    return pymysql.connect(hostname, username, password, database, port)


def get_box_scores(connection, game_id, home_team, away_team):
    box_score_page = requests.get("https://www.basketball-reference.com/boxscores/" + game_id + ".html")

    box_score_soup = BeautifulSoup(box_score_page.text, 'html.parser')

    home_basic_id = 'box_' + home_team.lower() + '_basic'

    home_adv_id = 'box_' + home_team.lower() + '_advanced'

    away_basic_id = 'box_' + away_team.lower() + '_basic'

    away_adv_id = 'box_' + away_team.lower() + '_advanced'

    home_table = box_score_soup.find('table', id=home_basic_id).tbody.find_all('tr')

    home_table_adv = box_score_soup.find('table', id=home_adv_id).tbody.find_all('tr')

    away_table = box_score_soup.find('table', id=away_basic_id).tbody.find_all('tr')

    away_table_adv = box_score_soup.find('table', id=away_adv_id).tbody.find_all('tr')

    get_basic_stats(connection, game_id, home_table)

    get_basic_stats(connection, game_id, away_table)

    get_advanced_stats(connection, game_id, home_table_adv)

    get_advanced_stats(connection, game_id, away_table_adv)

    sleep(randint(4, 8))


def get_basic_stats(connection, game_id, rows):
    stat_data_fields = ['id', 'game_id', 'minutes', 'fg_made',
                        'fg_attempts', 'fg%', '3p_made', '3p_attempts', '3P%',
                        'ft_made', 'ft_attempts', 'ft%', 'orebs', 'drebs', 'assists', 'steals',
                        'blocks', 'tov', 'pf', 'points', 'plus/minus']

    for stat_row in rows:
        dictionary = {key: None for key in stat_data_fields}
        if stat_row.td is not None and stat_row.find('td', attrs={'data-stat': 'reason'}) is None:
            dictionary['game_id'] = game_id
            dictionary['id'] = stat_row.th['data-append-csv']
            if stat_row.find('td', attrs={'data-stat': 'mp'}).text != '':
                minutes_raw = stat_row.find('td', attrs={'data-stat': 'mp'}).text.split(':')
                if int(minutes_raw[1]) > 30:
                    dictionary['minutes'] = int(minutes_raw[0]) + 1
                else:
                    dictionary['minutes'] = int(minutes_raw[0])
                dictionary['fg_made'] = int(stat_row.find('td', attrs={'data-stat': 'fg'}).text)
                dictionary['fg_attempts'] = int(stat_row.find('td', attrs={'data-stat': 'fga'}).text)
                if stat_row.find('td', attrs={'data-stat': 'fg_pct'}).text != '':
                    dictionary['fg%'] = float(stat_row.find('td', attrs={'data-stat': 'fg_pct'}).text)
                dictionary['3p_made'] = int(stat_row.find('td', attrs={'data-stat': 'fg3'}).text)
                dictionary['3p_attempts'] = int(stat_row.find('td', attrs={'data-stat': 'fg3a'}).text)
                if stat_row.find('td', attrs={'data-stat': 'fg3_pct'}).text != '':
                    dictionary['3P%'] = float(stat_row.find('td', attrs={'data-stat': 'fg3_pct'}).text)
                dictionary['ft_made'] = int(stat_row.find('td', attrs={'data-stat': 'ft'}).text)
                dictionary['ft_attempts'] = int(stat_row.find('td', attrs={'data-stat': 'fta'}).text)
                if stat_row.find('td', attrs={'data-stat': 'ft_pct'}).text != '':
                    dictionary['ft%'] = float(stat_row.find('td', attrs={'data-stat': 'ft_pct'}).text)
                dictionary['orebs'] = int(stat_row.find('td', attrs={'data-stat': 'orb'}).text)
                dictionary['drebs'] = int(stat_row.find('td', attrs={'data-stat': 'drb'}).text)
                dictionary['assists'] = int(stat_row.find('td', attrs={'data-stat': 'ast'}).text)
                dictionary['steals'] = int(stat_row.find('td', attrs={'data-stat': 'stl'}).text)
                dictionary['blocks'] = int(stat_row.find('td', attrs={'data-stat': 'blk'}).text)
                dictionary['tov'] = int(stat_row.find('td', attrs={'data-stat': 'tov'}).text)
                dictionary['pf'] = int(stat_row.find('td', attrs={'data-stat': 'pf'}).text)
                dictionary['points'] = int(stat_row.find('td', attrs={'data-stat': 'pts'}).text)
                dictionary['plus/minus'] = stat_row.find('td', attrs={'data-stat': 'plus_minus'}).text

            print(dictionary)

            cursor = connection.cursor()

            sql = "INSERT INTO nbaBoxScoresBasic (playerId, gameId, " \
                  "minutes, fgMade, fgAttempts, fgPct, threesMade, threeAttempts, threePct, " \
                  "ftMade, ftAttempts, ftPct, oRebounds, dRebounds, " \
                  "assists, steals, blocks, tov, pFouls, points, plusMinus) VALUES " \
                  "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                  " %s, %s, %s, %s)"

            cursor.execute(sql, (dictionary['id'], dictionary['game_id'], dictionary['minutes'],
                                 dictionary['fg_made'], dictionary['fg_attempts'], dictionary['fg%'], dictionary['3p_made'],
                                 dictionary['3p_attempts'], dictionary['3P%'],
                                 dictionary['ft_made'], dictionary['ft_attempts'], dictionary['ft%'],
                                 dictionary['orebs'], dictionary['drebs'], dictionary['assists'], dictionary['steals'],
                                 dictionary['blocks'], dictionary['tov'], dictionary['pf'], dictionary['points'],
                                 dictionary['plus/minus']))


def get_advanced_stats(connection, game_id, rows):
    stat_data_fields = ['id', 'game_id', 'minutes', 'TS%', 'efg%', '3PAr', 'FTr', 'ORB%', 'DRB%', 'TRB%', 'AST%',
                        'STL%', 'BLK%', 'TOV%', 'USG%', 'ORtg', 'DRtg']

    for stat_row in rows:
        dictionary = {key: None for key in stat_data_fields}
        if stat_row.td is not None and stat_row.find('td', attrs={'data-stat': 'reason'}) is None:
            dictionary['game_id'] = game_id
            dictionary['id'] = stat_row.th['data-append-csv']
            if stat_row.find('td', attrs={'data-stat': 'mp'}).text != '':
                minutes_raw = stat_row.find('td', attrs={'data-stat': 'mp'}).text.split(':')
                if int(minutes_raw[1]) > 30:
                    dictionary['minutes'] = int(minutes_raw[0]) + 1
                else:
                    dictionary['minutes'] = int(minutes_raw[0])
                if stat_row.find('td', attrs={'data-stat': 'ts_pct'}).text != '':
                    dictionary['TS%'] = float(stat_row.find('td', attrs={'data-stat': 'ts_pct'}).text)
                if stat_row.find('td', attrs={'data-stat': 'efg_pct'}).text != '':
                    dictionary['efg%'] = float(stat_row.find('td', attrs={'data-stat': 'efg_pct'}).text)
                if stat_row.find('td', attrs={'data-stat': 'fg3a_per_fga_pct'}).text != '':
                    dictionary['3PAr'] = float(stat_row.find('td', attrs={'data-stat': 'fg3a_per_fga_pct'}).text)
                if stat_row.find('td', attrs={'data-stat': 'fta_per_fga_pct'}).text != '':
                    dictionary['FTr'] = float(stat_row.find('td', attrs={'data-stat': 'fta_per_fga_pct'}).text)
                if stat_row.find('td', attrs={'data-stat': 'orb_pct'}).text != '':
                    dictionary['ORB%'] = float(stat_row.find('td', attrs={'data-stat': 'orb_pct'}).text)
                if stat_row.find('td', attrs={'data-stat': 'drb_pct'}).text != '':
                    dictionary['DRB%'] = float(stat_row.find('td', attrs={'data-stat': 'drb_pct'}).text)
                if stat_row.find('td', attrs={'data-stat': 'trb_pct'}).text != '':
                    dictionary['TRB%'] = float(stat_row.find('td', attrs={'data-stat': 'trb_pct'}).text)
                if stat_row.find('td', attrs={'data-stat': 'ast_pct'}).text != '':
                    dictionary['AST%'] = float(stat_row.find('td', attrs={'data-stat': 'ast_pct'}).text)
                if stat_row.find('td', attrs={'data-stat': 'stl_pct'}).text != '':
                    dictionary['STL%'] = float(stat_row.find('td', attrs={'data-stat': 'stl_pct'}).text)
                if stat_row.find('td', attrs={'data-stat': 'blk_pct'}).text != '':
                    dictionary['BLK%'] = float(stat_row.find('td', attrs={'data-stat': 'blk_pct'}).text)
                if stat_row.find('td', attrs={'data-stat': 'tov_pct'}).text != '':
                    dictionary['TOV%'] = float(stat_row.find('td', attrs={'data-stat': 'tov_pct'}).text)
                if stat_row.find('td', attrs={'data-stat': 'usg_pct'}).text != '':
                    dictionary['USG%'] = float(stat_row.find('td', attrs={'data-stat': 'usg_pct'}).text)
                if stat_row.find('td', attrs={'data-stat': 'off_rtg'}).text != '':
                    dictionary['ORtg'] = int(stat_row.find('td', attrs={'data-stat': 'off_rtg'}).text)
                if stat_row.find('td', attrs={'data-stat': 'def_rtg'}).text != '':
                    dictionary['DRtg'] = int(stat_row.find('td', attrs={'data-stat': 'def_rtg'}).text)

            print(dictionary)

            cursor = connection.cursor()

            sql = "INSERT INTO nbaBoxScoresAdvanced (playerId, gameId, minutes, TSpct, threeRate, FTRate, OREBpct, " \
                  "DREBpct, TREBpct, assistPct, stealPct, blockPct, TOVpct, usagePct, oRtg, dRtg) VALUES " \
                  "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

            cursor.execute(sql, (dictionary['id'], dictionary['game_id'], dictionary['minutes'],
                                 dictionary['TS%'], dictionary['3PAr'],
                                 dictionary['FTr'], dictionary['ORB%'], dictionary['DRB%'],
                                 dictionary['TRB%'], dictionary['AST%'], dictionary['STL%'],
                                 dictionary['BLK%'], dictionary['TOV%'], dictionary['USG%'], dictionary['ORtg'],
                                 dictionary['DRtg']))


def commit_games(connection, data):
    cursor = connection.cursor()
    sql = "INSERT INTO nbaSchedule (gameId, startDate, startDay, startTime, homeTeam, homeScore, awayTeam, " \
          "awayScore, attendance)" \
          " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.execute(sql, (data['game_id'], data['date'], data['day_of_week'], data['start_time'], data['home'],
                         data['home_score'], data['away'], data['away_score'], data['attendance']))


conn = open_connection()

# SPECIFY DESIRED YEARS AND MONTHS HERE.
# Year refers to the year a season ends (ex: 2019-2019 season is listed as '19')

years = ['19']

months = ['october', 'november', 'december', 'january', 'february', 'march', 'april', 'may', 'june']

for year in years:
    for month in months:

        page = requests.get("https://www.basketball-reference.com/leagues/NBA_20" + year + "_games-" + month + ".html")

        soup = BeautifulSoup(page.text, 'html.parser')

        if '404' in soup.find('title').text:
            continue

        schedule_table_rows = soup.find('table', id='schedule').tbody.find_all('tr')

        data_fields = ['game_id', 'date', 'day_of_week', 'start_time', 'home', 'home_score', 'away', 'away_score',
                       'attendance']

        for row in schedule_table_rows:

            data_results = {key: None for key in data_fields}

            try:
                game_id = row.th['csk']
            except KeyError:
                continue

            data_results['game_id'] = game_id

            data_results['date'] = datetime.date(int(game_id[:4]), int(game_id[4:6]), int(game_id[6:8]))

            data_results['day_of_week'] = row.th.text.split(',')[0]

            start_time = row.find('td', attrs={'data-stat': 'game_start_time'}).text

            if 'pm' in start_time:
                hour = int(start_time.split(':')[0])
                fixed_time = str(hour + 12) + ':' + start_time.split(':')[1]
                start_time = fixed_time.replace(' pm', '')
            else:
                start_time = start_time.replace(' am', '')

            data_results['start_time'] = start_time

            data_results['home'] = game_id[-3:]

            data_results['home_score'] = int(row.find('td', attrs={'data-stat': 'home_pts'}).text)

            data_results['away'] = row.find('td', attrs={'data-stat': 'visitor_team_name'}).a['href'].split('/')[2]

            data_results['away_score'] = int(row.find('td', attrs={'data-stat': 'visitor_pts'}).text)

            data_results['attendance'] = int(row.find('td', attrs={'data-stat': 'attendance'}).text.replace(',', ''))

            print(data_results)

            commit_games(conn, data_results)

            get_box_scores(conn, game_id, data_results['home'], data_results['away'])

        sleep(randint(4, 8))

        print("Committing...")

        conn.commit()

conn.close()
