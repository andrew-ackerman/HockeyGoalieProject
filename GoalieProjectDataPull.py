import operator
import requests
import pandas as pd
import json
import math
from datetime import date

teams = ['ANA','ARI','BOS','BUF','CGY','CAR','CHI','COL','CBJ','DAL',
'DET','EDM','FLA','LAK','MIN','MTL','NSH','NJD','NYI','NYR','OTT','PHI',
'PIT','SEA','SJS','STL','TBL','TOR','VAN','VGK','WSH','WPG']

# Returns the # of records from the NHL API for a given season. This # will be used in a differnet function to pull stats in intervals derived from the # of records.
# season : The season code of the data(20202021,20212022,etc.).
# numRecords : Int value of the total number of records for the given season code. 
def getNumRecords(season):
    api_nhl = f"https://api.nhle.com/stats/rest/en/goalie/startedVsRelieved?isAggregate=false&isGame=true&start=0&factCayenneExp=gamesPlayed%3E=1&cayenneExp=gameTypeId=2%20and%20seasonId%3C={season}%20and%20seasonId%3E={season}"
    pull = requests.get(api_nhl).content
    numRecords = json.loads(pull)['total']
    return numRecords

# Returns the goalie stat data from the NHL API for the total records in a season. Also sends the raw data to a CSV file.
# start/end_season : The season codes used to pull data.
# goalie_stats_raw : Dataframe of goalie data pulled from the NHL API for the defined seasons.
def getStats(start_season,end_season):
    stats_raw = []
    for season in range(start_season,(end_season+1),10001):
        total = getNumRecords(season)
        for interval in range(0,total,50):
            api_nhl = f"https://api.nhle.com/stats/rest/en/goalie/startedVsRelieved?isAggregate=false&isGame=true&start={interval}&factCayenneExp=gamesPlayed%3E=1&cayenneExp=gameTypeId=2%20and%20seasonId%3C={season}%20and%20seasonId%3E={season}"
            pull = requests.get(api_nhl).content
            stats_raw.extend(json.loads(pull)['data'])
    goalie_stats_raw = pd.DataFrame(stats_raw)
    goalie_stats_raw.columns = ['Game Date','Game ID','GP','Games Relieved (GR)','GR GA','GR L','GR OTL','GR Save %','GR Saves','GR SA','GR T','GR W',
    'Games Started (GS)','GS GA','GS L','GS OTL','GS Save %','GS Saves','GS SA','GS T','GS W','Goalie Full Name','Home/Road','Goalie Last','L','Opp','OTL','Player ID','Save %','Catches','Team','Ties','W'] 
    path = "C:/Users/Joe/Documents/Hockey Data/Goalie Project/Goalie_Stats_RAW.csv"
    goalie_stats_raw = goalie_stats_raw.sort_values(['Team','Game Date'])
    goalie_stats_raw.to_csv(path)  
    return goalie_stats_raw

# Cleans raw data by removing unnecessary columns and filling in NaN SV% with zeroes. Changes "Game ID" to Season and cuts ID to first four digits. 
# Sends cleaned data to a csv.
# df : The dataframe getting cleaned and returned.
def cleanData(df):
    df = df.drop(labels = ['GP','GR T','GS T','Goalie Full Name','Catches','Ties'],axis = 1)
    df['Game ID'] = df['Game ID'].astype(str).str[:4]
    df = df.rename(columns = {'Game ID' : 'Season'})
    path = "C:/Users/Joe/Documents/Hockey Data/Goalie Project/Goalie_Stats_WRK.csv"
    df.to_csv(path) 
    return df

# Returns the total number of games for a team in a season. For use in the calcStats function.
# df : Cleaned dataframe.
# season : Four digit season.
# team : Team abbriviation from the teams list.
# teamTotal : Number of games for a team in a season.
def getTeamTotal(df,season,team):
    teamTotal = 0
    gen = (i for i in range(0,len(df.index),1) if int(df.iloc[i]['Season']) == season and df.iloc[i]['Team'] == team and df.iloc[i]['Games Relieved (GR)']==0)
    for i in gen:
        teamTotal+=1
    return teamTotal

# Returns a team-specific set of goalies who played during the season. For use in the calcStats function.
# df : Cleaned dataframe.
# season : Four digit season.
# team : Team abbriviation from the teams list.
# goalies : Set of goalie names for the given team.
def getGoalieNames(df,season,team):
    goalies = set()
    gen = (i for i in range(0,len(df.index),1) if int(df.iloc[i]['Season']) == season and df.iloc[i]['Team'] == team)
    for i in gen:
        goalies.add(df.iloc[i]['Goalie Last'])
    return goalies

# Returns an empty dictionary to keep track of games started and relieved for goalies of a team.
# goalies : Set of goalies for a team in a given season
# is_int : Boolean to intialize dict with empty string or zero.
# counts : Initialized dictionary with goalie key and count value.
def getCountsDict(goalies,is_int):
    counts = dict()
    for goalie in goalies:
        if is_int == True:
            counts[goalie] = 0
        else:
            counts[goalie] = ''
    return counts

# Returns a team-specific set of game dates for the season. For use in the calcStats function.
# df : Cleaned dataframe.
# season : Four digit season.
# team : Team abbriviation from the teams list.
# dates : Set of game dates for the given team.    
def getGameDates(df,season,team):
    dates = set()
    gen = (i for i in range(0,len(df.index),1) if int(df.iloc[i]['Season']) == season and df.iloc[i]['Team'] == team)
    for i in gen:
        dates.add(df.iloc[i]['Game Date'])
    return dates

# Function to calculate the number of days rested for a goalie. Utilizes the goalies set to keep track of days rested within a dictionary.
# i : Count for indexing over dataframe.
# days_count : Dictionary that keeps track of last date in which a goalie played.
# df : Cleaned dataframe
# game_date : Date of game in which goalie played
# goalie : Name of goalie
def getDaysRested(i,days_count,df,game_date,goalie):
    if i == 0 or days_count[goalie] == '':
        df.at[df.index[i],'Days Rest'] = 'First Game'
        last_game = game_date
        last_game = date.fromisoformat(last_game)
        days_count[goalie] = last_game
    else:
        last_game = days_count[goalie]
        current_game = date.fromisoformat(game_date)
        days_rest = current_game - last_game
        days_rest = days_rest.days
        df.at[df.index[i],'Days Rest'] = (days_rest - 1)
        days_count[goalie] = current_game
    return days_count, df

# Calculates Days Rested, determines and defines starters and backups
# df : Cleaned dataframe
# season_from : Season to begin collecting data
# season_to : Season to end collecting data
# Returns dataframe with new calculated columns
def calcStats(df,season_from,season_to):
    df['Days Rest'] = ""                                                                                                                #Adds columns to dataframe that will be filled with calculated values.
    df['Starter'] = "" 
    df['Multiple Starters'] = ""
    df['Backup'] = ""                                                                             
    for season in range (int(season_from/10000),int(season_to/10000)+1,1):                                                               #Loops dataframe for each season
        for team in teams:
            quarter_count = 0                                                                                                                #Loops dataframe for each team in each season (ignores VGK and SEA prior to inagural seasons)
            tot_games = getTeamTotal(df,season,team)                                                                                     #Get total of games per team in the season 
            quarter = int(math.ceil(tot_games/4))                                                                                        #Splits total of games into quarter intervals, rounds up to avoid missing games
            goalies = getGoalieNames(df,season,team)                                                                                     #Gets set of goalie names for each team in the season
            gs_count = getCountsDict(goalies,True)                                                                                       #Dictionaries of counts to be used in calculations (ties value to goalie key)
            gr_count = getCountsDict(goalies,True)
            days_count = getCountsDict(goalies,False)
            starters = dict()
            backups = dict()
            games_count = 0
            game_dates = getGameDates(df,season,team)                                                                                    #Gets dates a team played in the season                                                                                                                                                                                                            
            gen = (i for i in range(0,len(df.index),1) if int(df.iloc[i]['Season']) == season and df.iloc[i]['Team'] == team)            #Loops through dataframe for each date a team played in a season
            for i in gen:    
                for game_date in game_dates:
                    if game_date == df.iloc[i]['Game Date']:
                        for goalie in goalies:
                            if df.iloc[i]['Goalie Last'] == goalie and df.iloc[i]['Games Started (GS)'] == 1:                             #Records number of starts and reliefs (at least one shot faced in a relief) for each quarter
                                gs_count[goalie]+=1
                                getDaysRested(i,days_count,df,game_date,goalie)
                            elif df.iloc[i]['Goalie Last'] == goalie and df.iloc[i]['Games Relieved (GR)'] == 1 and df.iloc[i]['GR SA'] >= 1:
                                gr_count[goalie]+=1
                                getDaysRested(i,days_count,df,game_date,goalie)  
                if i!=0 and df.iloc[i]['Game Date'] == df.iloc[i-1]['Game Date']:
                    pass
                else:
                    games_count+=1
                    gs_count = dict(sorted(gs_count.items(), key = operator.itemgetter(1), reverse = True))
                    gr_count = dict(sorted(gr_count.items(), key = operator.itemgetter(1), reverse = True))                               #Keeps track of stats for each quarter of the season.Takes into account 2013 lockout and the COVID shortened seasons (2019/2020).
                if games_count == quarter or (quarter_count == 3 and (quarter == 21 and games_count == 19) or (quarter == 18 and games_count == 15 and tot_games == 69) 
                                                                  or (quarter == 18 and games_count == 16 and tot_games == 70) or (quarter == 18 and games_count == 15 and tot_games == 71)):
                    quarter_count+=1
                    games_count = 0
# The next block of code is to calculate the goalie stats to be tracked. Would be better in its own function for sake of readability, but is easier to keep here due to VGK and SEA recent expansions. Will put in function for future update
                    most_starts = list(gs_count.values())[0]                                                                              #Calculates the start counts for each goalie, defines starter(s) and backups
                    most_reliefs = list(gr_count.values())[0]
                    for g,start_count in gs_count.items():
                        if start_count == most_starts or most_starts-start_count <= 2:
                            starters[g] = start_count
                        elif start_count>0:
                            backups[g] = start_count
                    if len(starters) == 1:
                        starting_goalie = str(list(starters.keys())[0])
                        df.at[df.index[i],'Starter'] = starting_goalie
                        df.at[df.index[i],'Multiple Starters'] = 'False'
                    elif len(starters) > 1:
                        df.at[df.index[i],'Multiple Starters'] = 'True'
                    if len(backups) == 1:
                        backup_goalie = str(list(backups.keys())[0])
                        df.at[df.index[i],'Backup'] = backup_goalie
                    elif len(backups) > 1:
                        most_backup_starts = list(backups.values())[0]
                        backup = list()
                        for b,bs in backups.items():
                            if bs == most_backup_starts:                                                                                  #Rare scenario on if backup goalies have same amount of starts AND reliefs. Chooses a random goalie, can be changed manually later on.
                                backup.append(b)
                        if len(backup) == 1:
                            df.at[df.index[i],'Backup'] = backup[0]
                        else:
                            single_backup = list()
                            for bu in backup:
                                if gr_count[bu] == most_reliefs:
                                    single_backup.append(bu)
                                    df.at[df.index[i],'Backup'] = single_backup[0]
                            backup.clear()
                            single_backup.clear()               
                    gs_count = dict.fromkeys(gs_count, 0)
                    gr_count = dict.fromkeys(gr_count, 0)
                    starters.clear()
                    backups.clear()
    path = "C:/Users/Joe/Documents/Hockey Data/Goalie Project/Goalie_Stats_Calc.csv"
    df.to_csv(path) 
    return df                                                                       

def main():
    season_from = 20112012
    season_to = 20212022
    df_stats_raw = getStats(season_from,season_to)
    df_stats_wrk = cleanData(df_stats_raw)
    df_stats_wrk = calcStats(df_stats_wrk,season_from,season_to)
    print('Done')

if __name__ == "__main__":
    main()

#Things left to do in Python:
#Get goalie stats (any advanced?) (Add advanced stats RBS, GSAX after the calStats)
#Export to SQL