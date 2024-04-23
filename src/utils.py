import os
import sys
import pickle
import numpy as np 
import pandas as pd
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error

from src.exception import CustomException
from src.logger import logging


# function to get historical data

def get_historical_data(index, player_gw_data, df):

    player_id = player_gw_data.loc[index, 'player_id']
    gameweek = player_gw_data.loc[index, 'gameWeek']
    print('player ID :',player_id,'gameweek :', gameweek)

    temp=player_gw_data[player_gw_data['player_id']==player_id][player_gw_data['gameWeek'] < gameweek][player_gw_data['gameWeek'] >= gameweek-5]

    cols = ['minutes_played','clean_sheets', 'bps', 'player_starts', 'expected_goals', 'expected_assists', 'expected_goal_involvements', 'expected_goals_conceded', 'total_points']
    last5_gw_mean = temp[cols].mean(axis=0)
    last5_gw_mean = list(last5_gw_mean)

    # historical_data_cols = ['minutes_played_last5','clean_sheets_last5', 'bps_last5', 'player_starts_last5', 'expected_goals_last5', 'expected_assists_last5', 'expected_goal_involvements_last5', 'expected_goals_conceded_last5', 'total_points_last5']
    # df.loc[index, historical_data_cols] = last5_gw_mean
    return last5_gw_mean


# function to get fixture difficulty for each player for each match

def get_difficulty_and_is_home_team(index, data, fixture_data):

    player_id = data.loc[index, 'player_id']
    team_id = data.loc[index, 'team_id']
    gameweek = data.loc[index, 'gameWeek']
    fixture_id = data.loc[index, 'fixture_id']
    print('player ID :',player_id, 'team ID :',team_id, 'gameweek :', gameweek, 'fixture_id :', fixture_id)

    selected_fixture = fixture_data[fixture_data['match_id'] == fixture_id]

    # returns difficulty and is_home_team values

    if team_id == selected_fixture['home_team'].values[0] :
    #   data.loc[index, 'difficulty'] = selected_fixture['home_diff'].values[0]
    #   data.loc[index, 'is_home_team'] = 1
      return [selected_fixture['home_diff'].values[0], 1]

    elif team_id == selected_fixture['away_team'].values[0]:
    #   data.loc[index, 'difficulty'] = selected_fixture['away_diff'].values[0]
      return [selected_fixture['away_diff'].values[0], 0]

    else:
    #   data.loc[index, 'difficulty'] = -1
      return [-1, 0]
