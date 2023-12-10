#Imports
import nfl_data_py as nfl
import requests
from bs4 import BeautifulSoup
import pandas as pd
import random
import numpy as np

# Weekly performance data
def import_weekly_performance_data(years):
    # Import weekly performance data
    weekly_performance_data = nfl.import_weekly_data(years, downcast=True)

    # Filter weekly performance data
    filtered_weekly_performance_data = weekly_performance_data.loc[weekly_performance_data['season_type'] == "REG"]

    return filtered_weekly_performance_data

################################################

# Function to load and process roster data
def load_and_process_kicker_data(year, roster_data):
    # Load Kicker data
    url = f"https://www.fantasypros.com/nfl/reports/leaders/k.php?year={year}"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    table = soup.find('table')
    kicker = pd.read_html(str(table))[0]

    # Merge with Roster data
    kicker = kicker.merge(roster_data[['player_name', 'team', 'player_id']], how='left', left_on='Player', right_on='player_name')
    
    # Generate player IDs for kickers without one
    kicker['player_id'] = kicker.apply(lambda row: row['player_id'] if pd.notna(row['player_id']) else f'00-00{str(random.randint(80000, 89999)).zfill(5)}', axis=1)

    # Drop unnecessary columns
    kicker = kicker.drop(columns=['player_name', 'team', '#'])

    # Set multi-index
    kicker.set_index(['Player', 'Pos', 'Team', 'AVG', 'TTL', 'player_id'], inplace=True)
    
    # Reshape into long format
    long_kicker = kicker.reset_index().melt(id_vars=['Player', 'Pos', 'Team', 'AVG', 'TTL', 'player_id'], var_name='Week', value_name='Points')
    
    # Sort and reset index
    long_kicker.sort_values(['Player', 'Week'], inplace=True)
    long_kicker.reset_index(drop=True, inplace=True)

    return long_kicker


################################################

# Function to load and process defense data
def load_and_process_defense_data(year):
    # Load Defense data
    url = f"https://www.fantasypros.com/nfl/reports/leaders/dst.php?year={year}"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    table = soup.find('table')
    defense = pd.read_html(str(table))[0]

    # Generate unique player IDs for defenses
    start_counter = 90000
    defense['player_id'] = ['00-00' + str(start_counter + i).zfill(5) for i in range(len(defense))]
    
    # Drop unnecessary columns
    defense = defense.drop(['#'], axis=1)

    # Set multi-index
    defense.set_index(['Player', 'Pos', 'Team', 'AVG', 'TTL', 'player_id'], inplace=True)
    
    # Reshape into long format
    long_defense = pd.melt(defense.reset_index(), id_vars=['Player', 'Pos', 'Team', 'AVG', 'TTL', 'player_id'], var_name='Week', value_name='Points')
    
    # Extract week number from 'Week' column
    long_defense['Week'] = long_defense['Week'].str.extract('(\d+)').astype(int)
    
    # Sort and reset index
    long_defense.sort_values(['Player', 'Week'], inplace=True)
    long_defense.reset_index(drop=True, inplace=True)

    return long_defense

################################################

# Schedule data
def import_schedule_data(years):
    # Import schedule data
    schedule_data = nfl.import_schedules(years)

    # Filter schedule data
    filtered_schedule_data = schedule_data[['game_id', 'home_team', 'away_team', 'week', 'home_score', 'away_score']]

    return filtered_schedule_data

################################################

# Function to scrape ADP data and create dataframe
def scrape_adp_data(year):
    url = f'https://www.fantasypros.com/nfl/adp/ppr-overall.php?year={year}'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', attrs={'id': 'data'})

    # Extracting headers and indices of interested columns
    headers = [header.text for header in table.find_all('th')]
    adp_columns = ['Rank', 'Player Team (Bye)', 'POS', 'AVG']

    # Extracting data row by row
    table_data = [
        [col.text.strip() for col in row.find_all('td')]
        for row in table.find_all('tr')[1:]
    ]

    # Creating DataFrame with interested columns
    adp_data = pd.DataFrame(table_data, columns=headers)
    adp_data = adp_data[adp_columns]
    
    # Extracting player names, teams, and bye weeks
    def extract_info(row):
        if row and '(' in row and ')' in row:
            bye = row[row.index('(') + 1:row.index(')')]
            player = row[:row.index('(')].strip()
            player = player.replace(' O', '')
            player = player.replace(' Jr.', '').replace(' II', '').replace(' III', '')

            last_word = player.split(' ')[-1]

            if last_word in team_abbrs:
                team = last_word
                player = ' '.join(player.split(' ')[:-1])
            else:
                team = 'None'

            return player, team, bye

        elif row.endswith('DST'):
            player = row
            team = player
            bye = 'None'

        else:
            row = row.replace(' O', '').replace(' Jr.', '').replace(' II', '').replace(' III', '')
            return row, 'None', 'None'

    adp_data[['Player', 'Team', 'Bye']] = adp_data['Player Team (Bye)'].apply(extract_info).apply(pd.Series)
    adp_data = adp_data.drop(columns=['Player Team (Bye)'])

    # Update team values for specific players
    for name, team in dst_abbreviations.items():
        adp_data.loc[adp_data['Player'].str.contains(name), 'Team'] = team


    return adp_data

# List of team abbreviations
team_abbrs = ['ARI', 'ATL', 'BAL', 'BUF', 'CAR', 'CHI', 'CIN', 'CLE', 'DAL',
              'DEN', 'DET', 'GB', 'HOU', 'IND', 'JAC', 'KC', 'LAC', 'LAR',
              'LV', 'MIA', 'MIN', 'NE', 'NO', 'NYG', 'NYJ', 'PHI', 'PIT',
              'SEA', 'SF', 'TB', 'TEN', 'WAS']

# Abbreviations for dst
dst_abbreviations = {
    "Arizona Cardinals DST": "ARI",
    "Atlanta Falcons DST": "ATL",
    "Baltimore Ravens DST": "BAL",
    "Buffalo Bills DST": "BUF",
    "Carolina Panthers DST": "CAR",
    "Chicago Bears DST": "CHI",
    "Cincinnati Bengals DST": "CIN",
    "Cleveland Browns DST": "CLE",
    "Dallas Cowboys DST": "DAL",
    "Denver Broncos DST": "DEN",
    "Detroit Lions DST": "DET",
    "Green Bay Packers DST": "GB",
    "Houston Texans DST": "HOU",
    "Indianapolis Colts DST": "IND",
    "Jacksonville Jaguars DST": "JAC",
    "Kansas City Chiefs DST": "KC",
    "Los Angeles Chargers DST": "LAC",
    "Los Angeles Rams DST": "LAR",
    "Las Vegas Raiders DST": "LV",
    "Miami Dolphins DST": "MIA",
    "Minnesota Vikings DST": "MIN",
    "New England Patriots DST": "NE",
    "New Orleans Saints DST": "NO",
    "New York Giants DST": "NYG",
    "New York Jets DST": "NYJ",
    "Philadelphia Eagles DST": "PHI",
    "Pittsburgh Steelers DST": "PIT",
    "Seattle Seahawks DST": "SEA",
    "San Francisco 49ers DST": "SF",
    "Tampa Bay Buccaneers DST": "TB",
    "Tennessee Titans DST": "TEN",
    "Washington Commanders DST": "WAS"
}

################################################