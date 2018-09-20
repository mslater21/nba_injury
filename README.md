This folder contains all the files needed for scraping player data. 

**IMPORTANT: As of now, the stat_scrape file only works for players that played _at least one season in the 2000-01 season or later_. The draft_scrape and profile_scrape will work for all players.**

# You will need:
- PyCharm from JetBrains
- Python 3
- MySQL Workbench/MySQL Shell
- Access to the Sports Stats Database at BYU
- VPN access to the BYU statistics department
- Google Chrome

#### CREATE A config.py FILE with your USER, PASS, DB, and HOST or else it won't connect. Create it in the working directory with the scrape files.
```
HOST='hostname'
USER='yourusername'
PASS='yourpassword'
DB='nameofteststatsdb'
```
The port is usually 3306. Port may be changed inside of the scrape files under the 'open_connection' function.

# Description of files:

## player_list:
This file scrapes the ids of every player to have played since 1947. Inside of the file, there is an if statement containing the for loop actually doing the scraping. It looks like this:
```
for player in player_table:
    if int(player.find_all('td')[1].text) >= 2000:
        player_list.append(player.th['data-append-csv'])
        file.write('%s\n' % player.th['data-append-csv'])
```
The if statement's parameters are defaulted to scraping all players who played during the 1999-00 season or later, but any kind of parameter can be used. For example, new players drafted in 2018 can be scraped by setting: 
```
if int(player.find_all('td')[0].text) == 2018:
```
Index 0 points to the first year of the player's career, index 1 points to the last.

All scraped ids are stored in a text file named id_list.txt.

## profile_scrape:
Scrapes profile data for every player listed in id_list.txt. Data includes name, position(s), birthplace and date, college, high school, and draft information.

## draft_scrape:
Somewhat obsolete. Draft information is now gathered by the profile_scrape file.

## stat_scrape:
This file scrapes per game, per 36 minutes, per 100 possessions, and advanced stats for regular season and playoffs for every player listed in id_list.txt. Salary information is also collected. 

**IMPORTANT: Any players who DID NOT play at least one season during or after the 2000-01 season will be skipped by the program. Advanced shooting stats were not recorded until the 2000-01 season, and due to the absence of those tables in older player pages, the current program will not scrape them correctly.**

## game_scrape:
This file scrapes all regular season and postseason games for the years specified at the top of the main function. It also scrapes both regular and advanced box scores for all players of the game. 
The scrape commits to the database after every month. If the scrape fails before the month is comlete, the date bounds have to be changed so the scrape restarts at the beginning of that month.
I'm thinking about changing the way this file works to make a game id file, similar to stat_scrape, and then requesting the box scores from that list to make recovering from stoppages easier.

# IF THE SCRAPE STOPS FOR SOME REASON:
For all scrapes other than player_list, this is an easy fix. Each file should print the player id of the page it is currently scraping. Simply check the last player to be added, find the id in the id_list.txt file, and delete all names above that player's name (I suggest making a master list to keep all of the names for future scrapes). The program may now be restarted.

The stat_scrape file will also print each stat table as it works on it. This allows you to find what table contains the issue that causes the program to fail.

# Setting up MySQL shell:
Refer to the README in the forms folder for information on setting up the MySQL shell.

### Author
**Mitch Slater**
