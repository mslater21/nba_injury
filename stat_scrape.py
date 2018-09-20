import requests
from bs4 import BeautifulSoup, Comment
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


def commit_season(connection, player_id, scraped_rows):
    data_fields = ['id', 'season', 'age', 'team', 'position', 'games', 'starts', 'minutes', 'fg_made', 'fg_attempts',
                   'fg%', '3p_made', '3p_attempts', '3P%', '2p_made', '2p_attempts', '2P%', 'effective_fg',
                   'ft_made', 'ft_attempts', 'ft%', 'orebs', 'drebs', 'assists', 'steals', 'blocks', 'tov', 'pf',
                   'points', 'salary', 'note']

    for row in scraped_rows:

        dictionary = {key: None for key in data_fields}

        dictionary['id'] = player_id

        if row.th is not None:
            dictionary['season'] = row.th.a.text
            dictionary['age'] = row.find('td', attrs={'data-stat': 'age'}).text
            if row.find('td', attrs={'data-stat': 'team_id'}).a is not None:
                dictionary['team'] = row.find('td', attrs={'data-stat': 'team_id'}).a.text
            else:
                dictionary['team'] = row.find('td', attrs={'data-stat': 'team_id'}).text
            dictionary['position'] = row.find('td', attrs={'data-stat': 'pos'}).text
            dictionary['games'] = int(row.find('td', attrs={'data-stat': 'g'}).text)
            dictionary['starts'] = int(row.find('td', attrs={'data-stat': 'gs'}).text)
            dictionary['minutes'] = float(row.find('td', attrs={'data-stat': 'mp_per_g'}).text)
            dictionary['fg_made'] = float(row.find('td', attrs={'data-stat': 'fg_per_g'}).text)
            dictionary['fg_attempts'] = float(row.find('td', attrs={'data-stat': 'fga_per_g'}).text)
            if row.find('td', attrs={'data-stat': 'fg_pct'}).text != '':
                dictionary['fg%'] = float(row.find('td', attrs={'data-stat': 'fg_pct'}).text)
            dictionary['3p_made'] = float(row.find('td', attrs={'data-stat': 'fg3_per_g'}).text)
            dictionary['3p_attempts'] = float(row.find('td', attrs={'data-stat': 'fg3a_per_g'}).text)
            if row.find('td', attrs={'data-stat': 'fg3_pct'}).text != '':
                dictionary['3P%'] = float(row.find('td', attrs={'data-stat': 'fg3_pct'}).text)
            dictionary['2p_made'] = float(row.find('td', attrs={'data-stat': 'fg2_per_g'}).text)
            dictionary['2p_attempts'] = float(row.find('td', attrs={'data-stat': 'fg2a_per_g'}).text)
            if row.find('td', attrs={'data-stat': 'fg2_pct'}).text != '':
                dictionary['2P%'] = float(row.find('td', attrs={'data-stat': 'fg2_pct'}).text)
            if row.find('td', attrs={'data-stat': 'efg_pct'}).text != '':
                dictionary['effective_fg'] = float(row.find('td', attrs={'data-stat': 'efg_pct'}).text)
            dictionary['ft_made'] = float(row.find('td', attrs={'data-stat': 'ft_per_g'}).text)
            dictionary['ft_attempts'] = float(row.find('td', attrs={'data-stat': 'fta_per_g'}).text)
            if row.find('td', attrs={'data-stat': 'ft_pct'}).text != '':
                dictionary['ft%'] = float(row.find('td', attrs={'data-stat': 'ft_pct'}).text)
            dictionary['orebs'] = float(row.find('td', attrs={'data-stat': 'orb_per_g'}).text)
            dictionary['drebs'] = float(row.find('td', attrs={'data-stat': 'drb_per_g'}).text)
            dictionary['assists'] = float(row.find('td', attrs={'data-stat': 'ast_per_g'}).text)
            dictionary['steals'] = float(row.find('td', attrs={'data-stat': 'stl_per_g'}).text)
            dictionary['blocks'] = float(row.find('td', attrs={'data-stat': 'blk_per_g'}).text)
            dictionary['tov'] = float(row.find('td', attrs={'data-stat': 'tov_per_g'}).text)
            dictionary['pf'] = float(row.find('td', attrs={'data-stat': 'pf_per_g'}).text)
            dictionary['points'] = float(row.find('td', attrs={'data-stat': 'pts_per_g'}).text)
        else:
            dictionary['season'] = row.td.text
            dictionary['age'] = int(row.find_all('td')[1].text)
            dictionary['note'] = row.find_all('td')[2].text.replace('\xa0', '')

        cursor = connection.cursor()

        sql = "INSERT INTO nbaPerGame (id, season, age, team, positionPlayed, gamesPlayed, gamesStarted, " \
              "minutes, fgMade, fgAttempts, fgPct, threesMade, threeAttempts, threePct, twosMade, " \
              "twoAttempts, twoPct, effectiveFgPct, ftMade, ftAttempts, ftPct, oRebounds, dRebounds, " \
              "assists, steals, blocks, tov, pFouls, points, note) VALUES " \
              "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        cursor.execute(sql, (dictionary['id'], dictionary['season'], dictionary['age'], dictionary['team'],
                             dictionary['position'], dictionary['games'], dictionary['starts'], dictionary['minutes'],
                             dictionary['fg_made'], dictionary['fg_attempts'], dictionary['fg%'], dictionary['3p_made'],
                             dictionary['3p_attempts'], dictionary['3P%'], dictionary['2p_made'],
                             dictionary['2p_attempts'], dictionary['2P%'], dictionary['effective_fg'],
                             dictionary['ft_made'], dictionary['ft_attempts'], dictionary['ft%'],
                             dictionary['orebs'], dictionary['drebs'], dictionary['assists'], dictionary['steals'],
                             dictionary['blocks'], dictionary['tov'], dictionary['pf'], dictionary['points'],
                             dictionary['note']))


def commit_per36(connection, player_id, table, if_playoff):

    data_fields = ['id', 'season', 'age', 'team', 'position', 'games', 'starts', 'minutes', 'fg_made', 'fg_attempts',
                   'fg%', '3p_made', '3p_attempts', '3P%', '2p_made', '2p_attempts', '2P%', 'ft_made', 'ft_attempts',
                   'ft%', 'orebs', 'drebs', 'assists', 'steals', 'blocks', 'tov', 'pf', 'points']

    for row in table[1:]:

        info = [item.get_text(strip=True) for item in row.select("th,td")]

        if info[0] == 'Career':
            break

        dictionary = {key: None for key in data_fields}

        dictionary['id'] = player_id
        dictionary['season'] = info[0]
        dictionary['age'] = info[1]
        dictionary['team'] = info[2]
        dictionary['position'] = info[4]
        dictionary['games'] = int(info[5])
        dictionary['starts'] = int(info[6])
        dictionary['minutes'] = int(info[7])
        if info[8] != '':
            dictionary['fg_made'] = float(info[8])
        if info[9] != '':
            dictionary['fg_attempts'] = float(info[9])
        if info[10] != '':
            dictionary['fg%'] = float(info[10])
        if info[11] != '':
            dictionary['3p_made'] = float(info[11])
        if info[12] != '':
            dictionary['3p_attempts'] = float(info[12])
        if info[13] != '':
            dictionary['3P%'] = float(info[13])
        if info[14] != '':
            dictionary['2p_made'] = float(info[14])
        if info[15] != '':
            dictionary['2p_attempts'] = float(info[15])
        if info[16] != '':
            dictionary['2P%'] = float(info[16])
        if info[17] != '':
            dictionary['ft_made'] = float(info[17])
        if info[18] != '':
            dictionary['ft_attempts'] = float(info[18])
        if info[19] != '':
            dictionary['ft%'] = float(info[19])
        if info[20] != '':
            dictionary['orebs'] = float(info[20])
        if info[21] != '':
            dictionary['drebs'] = float(info[21])
        if info[23] != '':
            dictionary['assists'] = float(info[23])
        if info[24] != '':
            dictionary['steals'] = float(info[24])
        if info[25] != '':
            dictionary['blocks'] = float(info[25])
        if info[26] != '':
            dictionary['tov'] = float(info[26])
        if info[27] != '':
            dictionary['pf'] = float(info[27])
        if info[28] != '':
            dictionary['points'] = float(info[28])

        cursor = connection.cursor()

        if if_playoff is True:
            table_name = 'nbaPlayoffPer36'
        else:
            table_name = 'nbaPer36'

        sql = "INSERT INTO " + table_name + " (id, season, age, team, positionPlayed, gamesPlayed, gamesStarted, " \
              "minutes, fgMade, fgAttempts, fgPct, threesMade, threeAttempts, threePct, twosMade, " \
              "twoAttempts, twoPct, ftMade, ftAttempts, ftPct, oRebounds, dRebounds, " \
              "assists, steals, blocks, tov, pFouls, points) VALUES " \
              "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        cursor.execute(sql, (dictionary['id'], dictionary['season'], dictionary['age'], dictionary['team'],
                             dictionary['position'], dictionary['games'], dictionary['starts'], dictionary['minutes'],
                             dictionary['fg_made'], dictionary['fg_attempts'], dictionary['fg%'], dictionary['3p_made'],
                             dictionary['3p_attempts'], dictionary['3P%'], dictionary['2p_made'],
                             dictionary['2p_attempts'], dictionary['2P%'], dictionary['ft_made'],
                             dictionary['ft_attempts'], dictionary['ft%'], dictionary['orebs'],
                             dictionary['drebs'], dictionary['assists'], dictionary['steals'],
                             dictionary['blocks'], dictionary['tov'], dictionary['pf'], dictionary['points']))


def commit_per100(connection, player_id, table, if_playoff):

    data_fields = ['id', 'season', 'age', 'team', 'position', 'games', 'starts', 'minutes', 'fg_made', 'fg_attempts',
                   'fg%', '3p_made', '3p_attempts', '3P%', '2p_made', '2p_attempts', '2P%', 'ft_made', 'ft_attempts',
                   'ft%', 'orebs', 'drebs', 'assists', 'steals', 'blocks', 'tov', 'pf', 'points', 'ORtg', 'DRtg']

    for row in table[1:]:

        info = [item.get_text(strip=True) for item in row.select("th,td")]

        if info[0] == 'Career':
            break

        dictionary = {key: None for key in data_fields}

        dictionary['id'] = player_id
        dictionary['season'] = info[0]
        dictionary['age'] = info[1]
        dictionary['team'] = info[2]
        dictionary['position'] = info[4]
        dictionary['games'] = int(info[5])
        dictionary['starts'] = int(info[6])
        dictionary['minutes'] = int(info[7])
        if info[8] != '':
            dictionary['fg_made'] = float(info[8])
        if info[9] != '':
            dictionary['fg_attempts'] = float(info[9])
        if info[10] != '':
            dictionary['fg%'] = float(info[10])
        if info[11] != '':
            dictionary['3p_made'] = float(info[11])
        if info[12] != '':
            dictionary['3p_attempts'] = float(info[12])
        if info[13] != '':
            dictionary['3P%'] = float(info[13])
        if info[14] != '':
            dictionary['2p_made'] = float(info[14])
        if info[15] != '':
            dictionary['2p_attempts'] = float(info[15])
        if info[16] != '':
            dictionary['2P%'] = float(info[16])
        if info[17] != '':
            dictionary['ft_made'] = float(info[17])
        if info[18] != '':
            dictionary['ft_attempts'] = float(info[18])
        if info[19] != '':
            dictionary['ft%'] = float(info[19])
        if info[20] != '':
            dictionary['orebs'] = float(info[20])
        if info[21] != '':
            dictionary['drebs'] = float(info[21])
        if info[23] != '':
            dictionary['assists'] = float(info[23])
        if info[24] != '':
            dictionary['steals'] = float(info[24])
        if info[25] != '':
            dictionary['blocks'] = float(info[25])
        if info[26] != '':
            dictionary['tov'] = float(info[26])
        if info[27] != '':
            dictionary['pf'] = float(info[27])
        if info[28] != '':
            dictionary['points'] = float(info[28])
        if info[30] != '':
            dictionary['ORtg'] = int(info[30])
        if info[31] != '':
            dictionary['DRtg'] = int(info[31])

        cursor = connection.cursor()

        if if_playoff is True:
            table_name = 'nbaPlayoffPer100'
        else:
            table_name = 'nbaPer100'

        sql = "INSERT INTO " + table_name + " (id, season, age, team, positionPlayed, gamesPlayed, gamesStarted, " \
              "minutes, fgMade, fgAttempts, fgPct, threesMade, threeAttempts, threePct, twosMade, " \
              "twoAttempts, twoPct, ftMade, ftAttempts, ftPct, oRebounds, dRebounds, " \
              "assists, steals, blocks, tov, pFouls, points, ORtg, DRtg) VALUES " \
              "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        cursor.execute(sql, (dictionary['id'], dictionary['season'], dictionary['age'], dictionary['team'],
                             dictionary['position'], dictionary['games'], dictionary['starts'], dictionary['minutes'],
                             dictionary['fg_made'], dictionary['fg_attempts'], dictionary['fg%'], dictionary['3p_made'],
                             dictionary['3p_attempts'], dictionary['3P%'], dictionary['2p_made'],
                             dictionary['2p_attempts'], dictionary['2P%'], dictionary['ft_made'],
                             dictionary['ft_attempts'], dictionary['ft%'], dictionary['orebs'],
                             dictionary['drebs'], dictionary['assists'], dictionary['steals'],
                             dictionary['blocks'], dictionary['tov'], dictionary['pf'], dictionary['points'],
                             dictionary['ORtg'], dictionary['DRtg']))


def commit_advanced(connection, player_id, table, if_playoff):

    data_fields = ['id', 'season', 'age', 'team', 'position', 'games', 'minutes', 'PER', 'TS%', '3PAr', 'FTr', 'ORB%',
                   'DRB%', 'TRB%', 'AST%', 'STL%', 'BLK%', 'TOV%', 'USG%',
                   'OWS', 'DWS', 'WS', 'WS/48', 'OBPM', 'DBPM', 'BPM', 'VORP']

    for row in table[1:]:

        info = [item.get_text(strip=True) for item in row.select("th,td")]

        if info[0] == 'Career':
            break

        dictionary = {key: None for key in data_fields}

        dictionary['id'] = player_id
        dictionary['season'] = info[0]
        dictionary['age'] = info[1]
        dictionary['team'] = info[2]
        dictionary['position'] = info[4]
        dictionary['games'] = int(info[5])
        dictionary['minutes'] = int(info[6])
        if info[7] != '':
            dictionary['PER'] = float(info[7])
        if info[8] != '':
            dictionary['TS%'] = float(info[8])
        if info[9] != '':
            dictionary['3PAr'] = float(info[9])
        if info[10] != '':
            dictionary['FTr'] = float(info[10])
        if info[11] != '':
            dictionary['ORB%'] = float(info[11])
        if info[12] != '':
            dictionary['DRB%'] = float(info[12])
        if info[13] != '':
            dictionary['TRB%'] = float(info[13])
        if info[14] != '':
            dictionary['AST%'] = float(info[14])
        if info[15] != '':
            dictionary['STL%'] = float(info[15])
        if info[16] != '':
            dictionary['BLK%'] = float(info[16])
        if info[17] != '':
            dictionary['TOV%'] = float(info[17])
        if info[18] != '':
            dictionary['USG%'] = float(info[18])
        if info[20] != '':
            dictionary['OWS'] = float(info[20])
        if info[21] != '':
            dictionary['DWS'] = float(info[21])
        if info[22] != '':
            dictionary['WS'] = float(info[22])
        if info[23] != '':
            dictionary['WS/48'] = float(info[23])
        if info[25] != '':
            dictionary['OBPM'] = float(info[25])
        if info[26] != '':
            dictionary['DBPM'] = float(info[26])
        if info[27] != '':
            dictionary['BPM'] = float(info[27])
        if info[28] != '':
            dictionary['VORP'] = float(info[28])

        cursor = connection.cursor()

        if if_playoff is True:
            table_name = 'nbaPlayoffAdvanced'
        else:
            table_name = 'nbaAdvanced'

        sql = "INSERT INTO " + table_name + " (id, season, age, team, positionPlayed, gamesPlayed, minutes, " \
              "PER, TSpct, threeRate, FTRate, OREBpct, DREBpct, " \
              "TREBpct, assistPct, stealPct, blockPct, TOVpct, " \
              "usagePct, oWinShares, dWinShares, winShares, winSharesPer48, oPlusMinus, dPlusMinus, PlusMinus, VORP)" \
              " VALUES " \
              "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        cursor.execute(sql, (dictionary['id'], dictionary['season'], dictionary['age'], dictionary['team'],
                             dictionary['position'], dictionary['games'], dictionary['minutes'],
                             dictionary['PER'], dictionary['TS%'], dictionary['3PAr'],
                             dictionary['FTr'], dictionary['ORB%'], dictionary['DRB%'],
                             dictionary['TRB%'], dictionary['AST%'], dictionary['STL%'],
                             dictionary['BLK%'], dictionary['TOV%'], dictionary['USG%'],
                             dictionary['OWS'], dictionary['DWS'], dictionary['WS'], dictionary['WS/48'],
                             dictionary['OBPM'], dictionary['DBPM'], dictionary['BPM'], dictionary['VORP']))

        # connection.commit()


def commit_playoff(connection, player_id, table):

    data_fields = ['id', 'season', 'age', 'team', 'position', 'games', 'starts', 'minutes', 'fg_made', 'fg_attempts',
                   'fg%', '3p_made', '3p_attempts', '3P%', '2p_made', '2p_attempts', '2P%', 'effective_fg', 'ft_made',
                   'ft_attempts', 'ft%', 'orebs', 'drebs', 'assists', 'steals', 'blocks', 'tov', 'pf', 'points']

    for row in table[1:]:

        info = [item.get_text(strip=True) for item in row.select("th,td")]

        if info[0] == 'Career':
            break

        dictionary = {key: None for key in data_fields}

        dictionary['id'] = player_id
        dictionary['season'] = info[0]
        dictionary['age'] = info[1]
        dictionary['team'] = info[2]
        dictionary['position'] = info[4]
        dictionary['games'] = int(info[5])
        dictionary['starts'] = int(info[6])
        dictionary['minutes'] = float(info[7])
        dictionary['fg_made'] = float(info[8])
        dictionary['fg_attempts'] = float(info[9])
        if info[10] != '':
            dictionary['fg%'] = float(info[10])
        dictionary['3p_made'] = float(info[11])
        dictionary['3p_attempts'] = float(info[12])
        if info[13] != '':
            dictionary['3P%'] = float(info[13])
        dictionary['2p_made'] = float(info[14])
        dictionary['2p_attempts'] = float(info[15])
        if info[16] != '':
            dictionary['2P%'] = float(info[16])
        if info[17] != '':
            dictionary['effective_fg'] = float(info[17])
        dictionary['ft_made'] = float(info[18])
        dictionary['ft_attempts'] = float(info[19])
        if info[20] != '':
            dictionary['ft%'] = float(info[20])
        dictionary['orebs'] = float(info[21])
        dictionary['drebs'] = float(info[22])
        dictionary['assists'] = float(info[24])
        dictionary['steals'] = float(info[25])
        dictionary['blocks'] = float(info[26])
        dictionary['tov'] = float(info[27])
        dictionary['pf'] = float(info[28])
        dictionary['points'] = float(info[29])

        cursor = connection.cursor()

        sql = "INSERT INTO nbaPlayoffPerGame (id, season, age, team, positionPlayed, gamesPlayed, gamesStarted, " \
              "minutes, fgMade, fgAttempts, fgPct, threesMade, threeAttempts, threePct, twosMade, " \
              "twoAttempts, twoPct, effectiveFgPct, ftMade, ftAttempts, ftPct, oRebounds, dRebounds, " \
              "assists, steals, blocks, tov, pFouls, points) VALUES " \
              "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        cursor.execute(sql, (dictionary['id'], dictionary['season'], dictionary['age'], dictionary['team'],
                             dictionary['position'], dictionary['games'], dictionary['starts'], dictionary['minutes'],
                             dictionary['fg_made'], dictionary['fg_attempts'], dictionary['fg%'], dictionary['3p_made'],
                             dictionary['3p_attempts'], dictionary['3P%'], dictionary['2p_made'],
                             dictionary['2p_attempts'], dictionary['2P%'], dictionary['effective_fg'],
                             dictionary['ft_made'], dictionary['ft_attempts'], dictionary['ft%'], dictionary['orebs'],
                             dictionary['drebs'], dictionary['assists'], dictionary['steals'],
                             dictionary['blocks'], dictionary['tov'], dictionary['pf'], dictionary['points']))


def commit_salary(connection, player_id, salary_info):
    data_fields = ['id', 'season', 'salary', 'note']
    for items in salary_info[1:]:
        dictionary = {key: None for key in data_fields}
        tds = [item.get_text(strip=True) for item in items.select("th,td")]
        if tds[0] != "Career":
            dictionary['id'] = player_id
            dictionary['season'] = tds[0]
            try:
                dictionary['salary'] = int(tds[3].replace('$', '').replace(',', ''))
            except ValueError:
                dictionary['note'] = tds[3]

            cursor = connection.cursor()

            sql = "INSERT INTO nbaSalary (player_id, season, salary, note) VALUES (%s, %s, %s, %s)"

            cursor.execute(sql, (dictionary['id'], dictionary['season'], dictionary['salary'], dictionary['note']))


conn = open_connection()

file = open('id_list.txt', 'r')  # Open list of player ids

players = file.read().splitlines()  # Remove newlines from ids

for player in players:

    print(player)

    page = requests.get("https://www.basketball-reference.com/players/" + player[:1] + "/" + player + ".html",
                        headers={"User-Agent":"Mozilla/5.0"})

    soup = BeautifulSoup(page.text, 'html.parser')

    tables = []

    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        data = BeautifulSoup(comment, "html.parser")
        if len(data.select("table.row_summable tr")) > 0:
            tables.append(data.select("table.row_summable tr"))

    nbaPer36 = tables[1]
    nbaPer100 = tables[2]
    nbaAdvanced = tables[3]
    if len(tables) > 6:
        try:
            nbaPlayoff = tables[6]
            nbaPlayoffPer36 = tables[8]
            nbaPlayoffPer100 = tables[9]
            nbaPlayoffAdvanced = tables[10]
            print("Playoff")
            commit_playoff(conn, player, nbaPlayoff)
            print("Playoff36")
            commit_per36(conn, player, nbaPlayoffPer36, True)
            print("Playoff100")
            commit_per100(conn, player, nbaPlayoffPer100, True)
            print("PlayoffAdv")
            commit_advanced(conn, player, nbaPlayoffAdvanced, True)
        except IndexError:
            continue

    salary_data = None

    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        data = BeautifulSoup(comment, "html.parser")
        if len(data.select("table.suppress_glossary tr")) > 0:
            print("Salary")
            salary_data = data.select("table.suppress_glossary tr")
            commit_salary(conn, player, salary_data)

    nbaSeasons = soup.body.find('table', id='per_game')  # Isolate per game table

    print("Season")

    commit_season(conn, player, nbaSeasons.tbody.find_all('tr'))

    print("36")

    commit_per36(conn, player, nbaPer36, False)

    print("100")

    commit_per100(conn, player, nbaPer100, False)

    print("Adv\n")

    commit_advanced(conn, player, nbaAdvanced, False)

    sleep(randint(5, 10))

    conn.commit()

conn.close()





