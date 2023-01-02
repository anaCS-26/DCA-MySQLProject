import requests
from bs4 import BeautifulSoup
import pandas as pd
import mysql.connector
import time
from tqdm import tqdm
import sqlalchemy
import pyodbc
import os

os.system("cls")

def getMatchOvers(matchType, matchGrade):
    if(matchType == "1inn" and ("40OT" in matchGrade or "FF" in matchGrade)):
        return 40
    elif(matchType == "1inn" and ("RCF" in matchGrade or "F15" in matchGrade)):
        return 15
    elif(matchType == "1inn" and ("TW-25" in matchGrade or "PR25II" in matchGrade or "PRO25" in matchGrade)):
        return 25
    else:
        return 20

def creatMatchInnings(team, inning, pandasTable):
    try:
        batting_count = 1
        for player in pandas[pandasTable].itertuples():

            if(player.Batsman == "extras"):
                break

            batsman = player.Batsman
            if("+" in batsman):
                wicket_keeper = "Y"
                batsman = batsman.strip("+ ")
            else:
                wicket_keeper = "N"

            if("*" in batsman):
                captain = "Y"
                batsman = batsman.strip("* ")
            else:
                captain = "N"

            if(player.Fieldsman == "dnb"):
                runs = 0
                bls = 0
                fours = 0
                sixes = 0
                how_out = " "
                bowler = " "
            else:
                runs = int(player.Runs)
                bls = int(player.Bls)
                fours = int(pandas[pandasTable].iloc[player.Index, 5])
                sixes = int(pandas[pandasTable].iloc[player.Index, 6])
                if(pd.isnull(player.Fieldsman)):
                    how_out = " "
                else:
                    how_out = player.Fieldsman
                if(pd.isnull(player.Bowler)):
                    bowler = " "
                else:
                    bowler = player.Bowler

            if(runs >= 40 and runs < 50):
                missed_fifty = "Y"
            else:
                missed_fifty = "N"

            if(runs >= 90 and runs < 100):
                missed_century = "Y"
            else:
                missed_century = "N"

            batting_position = batting_count
            batting_count += 1

        newRow = [
            link.strip('cstm.htm '),
            inning,
            batting_position,
            batsman,
            team,
            how_out,
            bowler,
            runs,
            bls,
            fours,
            sixes,
            captain,
            wicket_keeper,
            missed_fifty,
            missed_century
        ]
        match_innings.loc[len(match_innings)] = newRow
    except Exception as e:
        print(e)
        print("\nMissing Match Innings table link: " + matchUrl)

def createMatchFOW(innning, pandasTable):
    try:
        fow_string = ""
        for fow in pandas[pandasTable].itertuples():
            fow_string += fow.FOW.strip(" ")

        fow_string = fow_string.split(')')
        fows = []
        for i in range(0, len(fow_string)):
            fows.append(fow_string[i].strip(" "))

        fows.pop()

        for i in range(0, len(fows)):

            wicket_no = fows[i].split('-')[0]
            runs = fows[i].split('-')[1].split('(')[0]
            player_name = fows[i].split('(')[1]
            newRow = [
                link.strip('cstm.htm '),
                innning,
                wicket_no,
                runs,
                player_name
            ]
            match_fow.loc[len(match_fow)] = newRow

    except:
        print("\nMissing Match FOW table link: " + matchUrl)

def createMatchExtras(innning, team, pandasTable):
    try:
        if(pandas[pandasTable].iloc[len(pandas[pandasTable].index)-2, 0] == "penalty runs"):
            penalty_runs = int(pandas[pandasTable].iloc[len(pandas[pandasTable].index)-2, 3])
            extras = pandas[pandasTable].iloc[len(pandas[pandasTable].index)-3, 2].strip("() ").split(' ')
        else:
            extras = pandas[pandasTable].iloc[len(pandas[pandasTable].index)-2, 2].strip("() ").split(' ')
            penalty_runs = 0
        
        byes = extras[0].strip("b")
        leg_byes = extras[1].strip("lb")
        wides = extras[2].strip("w")
        noballs = extras[3].strip("nb")
        
        newRow = [
            link.strip('cstm.htm '),
            innning,
            team,
            byes,
            leg_byes,
            wides,
            noballs,
            penalty_runs
        ]
        match_extra.loc[len(match_extra)] = newRow
    except:
        print("\nMissing Match Extras table link: " + matchUrl)

start_time = time.time()


mydb = mysql.connector.connect(
    host="",#host
    user="",#user
    password="",#password
    database="",#Database
    port=3306
)

cur = mydb.cursor()
cur = mydb.cursor(buffered=True)
cur.execute("USE") #Database)

#get all the links from the website
url = "https://dca4u.com/Statz/Overall/cstr74.htm"
mainPage = pd.read_html(url)

r = requests.get(url)
soup = BeautifulSoup(r.content, 'html.parser')
links = soup.find_all('a')

htmllinks = []
for link in range(1, len(links)-2, 2):
    htmllinks.append(links[link].get('href'))

count = 0

MatchesTable = pd.DataFrame(
    columns=[
        'match_id', 
        'match_date',
        'season',
        'team_1', 
        'team_2', 
        'Toss_won_by', 
        'umpires', 
        'scorers',
        'team1_runs',
        'team1_wickets',
        'team1_overs', 
        'team2_runs', 
        'team2_wickets', 
        'team2_overs',
        'player_of_match',
        'winner',
        'loser',
        'win_how',
        'win_by',
        'match_tied_flg',
        'match_grade',
        'ground',
        'match_type',
        'match_overs'
    ]
)

match_innings = pd.DataFrame(
    columns=[
        'match_id',
        'inning_no',
        'batting_position',
        'batsman',
        'team_name',
        'how_out',
        'bowler',
        'runs',
        'balls_played',
        'fours',
        'sixes',
        'captain',
        'wicket_keeper',
        'missed_fifty',
        'missed_century'
    ]
)

match_fow = pd.DataFrame(
    columns=[
        'match_id',
        'inning_no',
        'wicket_no',
        'runs',
        'player_name'
    ]
)

match_extra = pd.DataFrame(
    columns=[
        'match_id',
        'inning_no',
        'team',
        'byes',
        'leg_byes',
        'wides',
        'no_balls',
        'penalty_runs'
    ]
)

for row in tqdm(mainPage[0].itertuples(), total=len(mainPage[0].index), desc="Links Processed"):

    link = htmllinks[count]
    count += 1

    cur.execute("SELECT count(*) FROM match_innings WHERE match_id = " + str(link.strip("cs.htm ")))
    linkCount = cur.fetchone()[0]
    if(linkCount > 0):
        continue
    

    Team1 = row.Team1.strip('1234567890/ ')
    Team2 = row.Team2.strip('1234567890/ ')

    if(pd.isnull(row.Winner)):
        winner = "Incomplete"
        loser = "Incomplete"
    else:
        winner = row.Winner.split(' ')[0:2]
        winner = ' '.join(winner)

    if(Team1 == winner):
        loser = Team2
    else:
        loser = Team1

    if(winner == "Match Tied"):
        winhow = "TIE"
        loser = "Match Tied"
        winby = 0
        matchTieFlag = "Y"
    else:
        if(winner != "Incomplete"):
            matchTieFlag = "N"
            if("runs" in row.Winner):
                winhow = "runs"
            else:
                winhow = "wkts"
            
            winby = row.Winner.split(" ")[-2]
        else:
            winhow = "Incomplete"
            winby = 0
            matchTieFlag = "N"
    
    grade = row.Grade
    ground = row.Ground
    matchType = row.Type


    MatchDate = row.Date.split(' ', 1)[0]
    Day = MatchDate[0:2]
    Month = MatchDate[3:6]
    Year = MatchDate[7:11]
    case = {
        'Jan': '01',
        'Feb': '02',
        'Mar': '03',
        'Apr': '04',
        'May': '05',
        'Jun': '06',
        'Jul': '07',
        'Aug': '08',
        'Sep': '09',
        'Oct': '10',
        'Nov': '11',
        'Dec': '12'
    }
    Month = case.get(Month, 'Invalid Month')
    if(int(Month) < 7):
        season = str(int(Year) - 1) + '-' + Year
    else:
        season = Year + '-' + str(int(Year) + 1)

    MatchDate = Year + '-' + Month + '-' + Day

    matchUrl = "https://dca4u.com/Statz/Overall/" + link.strip(" ")
    pandas = pd.read_html(matchUrl)

    try:
        team1_wickets = pandas[1].iloc[len(pandas[1].index)-1, 2]
        team1_wickets = team1_wickets.split(' ')[0]
    except:
        team1_wickets = 0

    try:
        team2_wickets = pandas[4].iloc[len(pandas[4].index)-1, 2]
        team2_wickets = team2_wickets.split(' ')[0]
    except:
        team2_wickets = 0

    request = requests.get(matchUrl)
    soup = BeautifulSoup(request.content, 'html.parser')
    divs = soup.find_all('div', class_="cst_title")
    try:
        team1_overs = divs[1].text
        team1_overs = team1_overs.split("(")[1].split(" ")[1].strip(") ")
    except:
        print("\nMatch table not found (Team 1) link:" + matchUrl)
        team1_overs = 0

    try:
        team2_overs = divs[2].text.split('() ')[-1]
        team2_overs = team2_overs.split("(")[1].split(" ")[1].strip(") ")
    except:
        print("\nMatch table not found (Team 2) link:" + matchUrl)
        team2_overs = 0

    try:
        toss_won = ""
        umpires = ""
        scorers = ""
        player_of_match = ""
        home_side = ""
        for row in pandas[0].itertuples():
            if("Toss won by" in row._1):
                toss_won = row._2
            if("Umpire notes" in row._1 or "Umpires" in row._1):
                umpires = row._2
            if("Scorer notes" in row._1):
                scorers = row._2
            if("Player of Match" in row._1):
                player_of_match = row._2
            if("Home Side" in row._1):
                home_side = row._2

        match_type = matchType.strip(' ')
        grade = grade.strip(' ')
        match_overs = getMatchOvers(matchType, grade)

        newRow = [
                link.strip('cstm.htm '), 
                MatchDate,
                season,
                Team1,
                Team2,
                toss_won,
                umpires,
                scorers,
                pandas[1].iloc[len(pandas[1].index)-1, 3],
                team1_wickets,
                team1_overs,
                pandas[4].iloc[len(pandas[4].index)-1, 3],
                team2_wickets,
                team2_overs,
                player_of_match,
                winner,
                loser,
                winhow,
                winby,
                matchTieFlag,
                grade,
                ground,
                matchType,
                match_overs
            ]
        MatchesTable.loc[len(MatchesTable)] = newRow
    except:
        print("\nWhole Match table not found link:" + matchUrl)

    divs = soup.find_all("div", class_="cst_title")
    firstTeam = divs[1].text.split(' ')[0:2]
    firstTeam = ' '.join(firstTeam)
    if(firstTeam == Team1):
        secondTeam = Team2
    else:
        secondTeam = Team1

    creatMatchInnings(firstTeam, 1, 1)
    creatMatchInnings(secondTeam, 2, 4)

    createMatchFOW(1, 2)
    createMatchFOW(2, 5)

    createMatchExtras(1, firstTeam, 1)
    createMatchExtras(2, secondTeam, 4)

print()

count = 0
for row in tqdm(MatchesTable.itertuples(), total=MatchesTable.shape[0], desc="Uploading Matches"):
    try:
        val = [row.match_id, row.match_date, row.season, row.team_1, row.team_2, row.Toss_won_by, row.umpires, row.scorers, row.team1_runs, row.team1_wickets, row.team1_overs, row.team2_runs, row.team2_wickets, row.team2_overs, row.player_of_match, row.winner, row.loser, row.win_how, row.win_by, row.match_tied_flg, row.match_grade, row.ground, row.match_type, row.match_overs]
        cur.execute("INSERT INTO matches (match_id, match_date, season, team_1, team_2, Toss_won_by, umpires, scorers, team1_runs, team1_wickets, team1_overs, team2_runs, team2_wickets, team2_overs, player_of_match, winner, loser, win_how, win_by, match_tied_flg, match_grade, ground, match_type, match_overs) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", val)
    except Exception as e:
        mydb.commit()
        print("\nError:", e)
        print("match_id: " + str(row.match_id))
    if(count % 100 == 0):
        mydb.commit()
    if(count % 500 == 0):
        cur.execute("SELECT 1")
    count += 1
mydb.commit()

print()

for row in tqdm(match_innings.itertuples(), total=match_innings.shape[0], desc="Uploading Innings"):
    try:
        val = [row.match_id, row.inning_no, row.batting_position, row.batsman, row.team_name, row.how_out, row.bowler, row.runs, row.balls_played, row.fours, row.sixes, row.captain, row.wicket_keeper, row.missed_fifty, row.missed_century]
        cur.execute("INSERT INTO match_innings (match_id, inning_no, batting_position, batsman, team_name, how_out, bowler, runs, balls_played, fours, sixes, captain, wicket_keeper, missed_fifty, missed_century) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", val)
    except Exception as e:
        mydb.commit()
        print("\nError:", e)
        print("match_id: " + str(row.match_id))
    if(count % 100 == 0):
        mydb.commit()
    if(count % 500 == 0):
        cur.execute("SELECT 1")
    count += 1
mydb.commit()
print()

for row in tqdm(match_fow.itertuples(), total=match_fow.shape[0], desc="Uploading FOW"):
    try:
        val = [row.match_id, row.inning_no, row.wicket_no, row.runs, row.player_name]
        cur.execute("INSERT INTO match_fow (match_id, inning_no, wicket_no, runs, player_name) VALUES (%s, %s, %s, %s, %s)", val)
    except Exception as e:
        mydb.commit()
        print("\nError:", e)
        print("match_id: " + str(row.match_id))
    if(count % 100 == 0):
        mydb.commit()
    if(count % 500 == 0):
        cur.execute("SELECT 1")
    count += 1
mydb.commit()
print()

for row in tqdm(match_extra.itertuples(), total=match_extra.shape[0], desc="Uploading Extras"):
    try:
        val = [row.match_id, row.inning_no, row.team, row.byes, row.leg_byes, row.wides, row.no_balls, row.penalty_runs]
        cur.execute("INSERT INTO match_extras (match_id, inning_no, team, byes, leg_byes, wides, no_balls, penalty_runs) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", val)
    except Exception as e:
        mydb.commit()
        print("\nError:", e)
        print("match_id: " + str(row.match_id))
    if(count % 100 == 0):
        mydb.commit()
    if(count % 500 == 0):
        cur.execute("SELECT 1")
    count += 1
mydb.commit()

print()
print("------------------SQL Data Upload Complete------------------")

cur.close()

#print minutes and seconds it took to run the script
print("------Script completed in " + str(int((time.time() - start_time)/60)) + " : " + str(round((time.time() - start_time)%60, 2)) + " seconds------ ")