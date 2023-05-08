import requests
from bs4 import BeautifulSoup
import pandas as pd
import mysql.connector
import time
from tqdm import tqdm
import os

os.system("cls")
#hello

# Gets the number of overs based on match type and grade
def getMatchOvers(matchType, matchGrade):
    if(matchType == "1inn" and ("40OT" in matchGrade or "FF" in matchGrade)):
        return 40
    elif(matchType == "1inn" and ("RCF" in matchGrade or "F15" in matchGrade)):
        return 15
    elif(matchType == "1inn" and ("TW-25" in matchGrade or "PR25II" in matchGrade or "PRO25" in matchGrade or "CMT16" in matchGrade)):
        return 25
    else:
        return 20

#Creates the match inning table
def createMatchInnings(team, inning, pandasTable):
    try:
        #starting from batting position 1
        batting_count = 1
        #Each row in the table represents a players stats
        for player in pandas[pandasTable].itertuples():

            if(player.Batsman == "extras"):
                break

            batsman = player.Batsman

            # If the player has a + next to their name, they are the wicket keeper
            if("+" in batsman):
                wicket_keeper = "Y"
                batsman = batsman.strip("+ ")
            else:
                wicket_keeper = "N"

            # If the player has a * next to their name, they are the captain
            if("*" in batsman):
                captain = "Y"
                batsman = batsman.strip("* ")
            else:
                captain = "N"

            # if the player did not bat, set all stats to 0
            if(player.Fieldsman == "dnb"):
                runs = 0
                bls = 0
                fours = 0
                sixes = 0
                how_out = " "
                bowler = " "
            else:
                # if the player did bat, set all stats to their actual values
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

            # if the player missed a 50 or 100, set the missed_50 or missed_100 to Y
            if(runs >= 40 and runs < 50):
                missed_fifty = "Y"
            else:
                missed_fifty = "N"

            if(runs >= 90 and runs < 100):
                missed_century = "Y"
            else:
                missed_century = "N"

            # set the batting position
            batting_position = batting_count
            batting_count += 1

            # create a new row in the match_innings table
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
            # add the row to the match_innings table
            match_innings.loc[len(match_innings)] = newRow
    except Exception as e:
        # if there is an error, print the error and the link to the match
        print(e)
        print("\nMissing Match Innings table link: " + matchUrl)

# Creates the match bowling table
def createMatchFOW(innning, pandasTable):
    try:
        # The data is collected from the FOW table on the webpage
        fow_string = ""

        # format the string using split and strip to remove unwanted characters and spaces
        for fow in pandas[pandasTable].itertuples():
            fow_string += fow.FOW.strip(" ")

        fow_string = fow_string.split(')')
        fows = []
        for i in range(0, len(fow_string)):
            # fows will now be a list of players contained in the FOW table
            fows.append(fow_string[i].strip(" "))

        # remove the last element in the list as it is empty
        fows.pop()

        for i in range(0, len(fows)):
            # for each player in the list, create a new row in the match_fow table
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
            # add the row to the match_fow table
            match_fow.loc[len(match_fow)] = newRow

    except:
        # if there is an error, print the error and the link to the match
        print("\nMissing Match FOW table link: " + matchUrl)

# Creates the match bowling table
def createMatchExtras(innning, team, pandasTable):
    try:
        # The data is collected from the Extras row in the Batsman table on the webpage
        # Checking if there are penalty runs
        if(pandas[pandasTable].iloc[len(pandas[pandasTable].index)-2, 0] == "penalty runs"):
            # If there are penalty runs, set the penalty_runs variable to the number of penalty runs
            penalty_runs = int(pandas[pandasTable].iloc[len(pandas[pandasTable].index)-2, 3])
            extras = pandas[pandasTable].iloc[len(pandas[pandasTable].index)-3, 2].strip("() ").split(' ')
        else:
            # If there are no penalty runs, set the penalty_runs variable to 0 and set the extras variable to the extras row
            extras = pandas[pandasTable].iloc[len(pandas[pandasTable].index)-2, 2].strip("() ").split(' ')
            penalty_runs = 0
        
        # format the extras row using split and strip to remove unwanted characters and spaces and populate each variable
        byes = extras[0].strip("b")
        leg_byes = extras[1].strip("lb")
        wides = extras[2].strip("w")
        noballs = extras[3].strip("nb")
        
        # create a new row in the match_extra table
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
        # add the row to the match_extra table
        match_extra.loc[len(match_extra)] = newRow
    except:
        # if there is an error, print the error and the link to the match
        print("\nMissing Match Extras table link: " + matchUrl)

# Starts the timer
start_time = time.time()

# Creating the connection to the database
# For security reasons, the host, username, password, database name and port number have been removed
mydb = mysql.connector.connect(
    host="host", #host
    user="username", #username
    password="password", #password
    database="databsae name", #database name
    port=0000 #port number
)

# Creating the cursor
cur = mydb.cursor()
cur = mydb.cursor(buffered=True)
cur.execute("USE DCASTATZ") #Database)

#Using the website link below to recieve all the match links
url = "https://dca4u.com/Statz/Overall/cstr74.htm"
mainPage = pd.read_html(url)

#Creating the request to the website
r = requests.get(url)
soup = BeautifulSoup(r.content, 'html.parser')
# Finding all the links on the page
links = soup.find_all('a')

# Creating a list of all the links on the page
htmllinks = []
for link in range(1, len(links)-2, 2):
    htmllinks.append(links[link].get('href'))


count = 0

# Creating the match tables template
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

# Creating the match innings table template
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

# Creating the match FOW table template
match_fow = pd.DataFrame(
    columns=[
        'match_id',
        'inning_no',
        'wicket_no',
        'runs',
        'player_name'
    ]
)

# Creating the match extras table template
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

# Go through each link on the page. Each link represents a match. tqdm is used to show the progress of the loop
for row in tqdm(mainPage[0].itertuples(), total=len(mainPage[0].index), desc="Links Processed"):

    # Get the link from the list of links
    link = htmllinks[count]
    count += 1

    # Checks if the match has already been added to the database
    cur.execute("SELECT count(*) FROM match_innings WHERE match_id = " + str(link.strip("cs.htm ")))
    linkCount = cur.fetchone()[0]
    if(linkCount > 0):
        continue
    
    # Getting the teams to use later
    Team1 = row.Team1.strip('1234567890/ ')
    Team2 = row.Team2.strip('1234567890/ ')

    # If the match does not have a winner or loser, the match is incomplete
    if(pd.isnull(row.Winner)):
        winner = "Incomplete"
        loser = "Incomplete"
    else:
        # Otherwise get the winner of the match by parsing
        winnerRow = row.Winner.split(' ')
        # If the match was tied or there was no result, the winner and loser are the winner row
        if(("Match" in winnerRow and "Tied" in winnerRow) or ("No" in winnerRow and "result" in winnerRow)):
            winner = winnerRow[0] + " " + winnerRow[1]
            loser = winnerRow[0] + " " + winnerRow[1]
        else:
            # Otherwise get the winner and loser by parsing the winner row until the win is found
            winnerIndex = row.Winner.split(' ').index("Win")
            winner = row.Winner.split(' ')[0:winnerIndex]
            winner = ' '.join(winner)

    # If the winner is team 1, the loser is team 2. Otherwise the loser is team 1
    if(Team1 == winner):
        loser = Team2
    else:
        loser = Team1

    # If the match was tied then winhow is TIE and winby is 0 and the flag for tie is set to Y
    if(winner == "Match Tied"):
        winhow = "TIE"
        loser = "Match Tied"
        winby = 0
        matchTieFlag = "Y"
    else:
        # If the match was not incomplete then tie flag is set to N and winhow is set to whatever is written in the Winner row and winby is whatever is written in the Winner row
        if(winner != "Incomplete"):
            matchTieFlag = "N"
            if("runs" in row.Winner):
                winhow = "runs"
            else:
                winhow = "wkts"
            
            winby = row.Winner.split(" ")[-2]
        else:
            # If the match was incomplete then winhow is set to incomplete and winby is 0 and the tie flag is set to N
            winhow = "Incomplete"
            winby = 0
            matchTieFlag = "N"
    
    # Getting the match grade, ground, and match type
    grade = row.Grade
    ground = row.Ground
    matchType = row.Type

    # Getting the match date and season
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

    # Making the date into a format that can be used in the MySQL databse. YYYY-MMM-DD
    MatchDate = Year + '-' + Month + '-' + Day

    # Getting the match url of the specific match
    matchUrl = "https://dca4u.com/Statz/Overall/" + link.strip(" ")
    pandas = pd.read_html(matchUrl)

    # Once we have access to the individual match page, we can get each teams wickets
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

    # I tried to recieve the title of the first table which included the amount of overs the team has played. Also contained who bowled first
    # However, the table is not always given so I had to use try and except to catch the error
    request = requests.get(matchUrl)
    soup = BeautifulSoup(request.content, 'html.parser')
    divs = soup.find_all('div', class_="cst_title")
    try:
        # Getting the overs for team 1
        team1_overs = divs[1].text
        team1_overs = team1_overs.split("(")[1].split(" ")[1].strip(") ")
    except:
        print("\nMatch table not found (Team 1) link:" + matchUrl)
        # If no table is found, then the overs are set to 0
        team1_overs = 0

    try:
        # Getting the overs for team 2
        team2_overs = divs[2].text.split('() ')[-1]
        team2_overs = team2_overs.split("(")[1].split(" ")[1].strip(") ")
    except:
        print("\nMatch table not found (Team 2) link:" + matchUrl)
        # If no table is found, then the overs are set to 0
        team2_overs = 0

    # Getting the toss won, umpires, scorers, player of the match, and home side within the individual match page
    try:
        toss_won = ""
        umpires = ""
        scorers = ""
        player_of_match = ""
        home_side = ""
        # In the match table, the information is stored in the first column. So we iterate through each row in the first column
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

        # Getting the amount of overs based on the match type and grade
        match_overs = getMatchOvers(matchType, grade)

        # Once all the data is collected, we can add it to the MatchesTable
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
        # Adding the row to the MatchesTable
        MatchesTable.loc[len(MatchesTable)] = newRow
    except:
        print("\nWhole Match table not found link:" + matchUrl)

    # The title is then recived for the Batsman table for both teams
    divs = soup.find_all("div", class_="cst_title")
    firstTeamIndex = divs[1].text.split(' ').index("1st")
    firstTeam = divs[1].text.split(' ')[0:firstTeamIndex]
    firstTeam = ' '.join(firstTeam)

    # Getting the first and second team based on title information
    if(firstTeam == Team1):
        secondTeam = Team2
    else:
        secondTeam = Team1

    # Creating the MatchInnings table for both teams
    try:
        createMatchInnings(firstTeam, 1, 1)
    except:
        print("\nMatch Innings table not found link:" + matchUrl)
    
    try:
        createMatchInnings(secondTeam, 2, 4)
    except:
        print("\nMatch Innings table not found link:" + matchUrl)

    # Creating the matchFOW table for both teams
    try:
        createMatchFOW(1, 2)
    except:
        print("\nMatch FOW table not found link:" + matchUrl)
    
    try:
        createMatchFOW(2, 5)
    except:
        print("\nMatch FOW table not found link:" + matchUrl)

    # Creating the MatchExtras table for both teams
    try:
        createMatchExtras(1, firstTeam, 1)
    except:
        print("\nMatch Extras table not found link:" + matchUrl)
    
    try:
        createMatchExtras(2, secondTeam, 4)
    except:
        print("\nMatch Extras table not found link:" + matchUrl)

print()

# Since we have already established a connection to the database, we can now upload the data to the database
count = 0
# For each row in the matches pandas table, we can upload the data to the database
for row in tqdm(MatchesTable.itertuples(), total=MatchesTable.shape[0], desc="Uploading Matches"):
    try:
        val = [row.match_id, row.match_date, row.season, row.team_1, row.team_2, row.Toss_won_by, row.umpires, row.scorers, row.team1_runs, row.team1_wickets, row.team1_overs, row.team2_runs, row.team2_wickets, row.team2_overs, row.player_of_match, row.winner, row.loser, row.win_how, row.win_by, row.match_tied_flg, row.match_grade, row.ground, row.match_type, row.match_overs]
        cur.execute("INSERT INTO matches (match_id, match_date, season, team_1, team_2, Toss_won_by, umpires, scorers, team1_runs, team1_wickets, team1_overs, team2_runs, team2_wickets, team2_overs, player_of_match, winner, loser, win_how, win_by, match_tied_flg, match_grade, ground, match_type, match_overs) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", val)
    except Exception as e:
        # if there is an error during upload, we can print the error and the match_id
        mydb.commit()
        print("\nError:", e)
        print("match_id: " + str(row.match_id))
    # To make sure the database does not crash, we can commit every 100 rows
    if(count % 100 == 0):
        mydb.commit()
    # To make sure database is still connected, we can check every 500 rows
    if(count % 500 == 0):
        cur.execute("SELECT 1")
    count += 1
# Committing the final changes
mydb.commit()

print()

# For each row in the match_innings pandas table, we can upload the data to the database
for row in tqdm(match_innings.itertuples(), total=match_innings.shape[0], desc="Uploading Innings"):
    try:
        val = [row.match_id, row.inning_no, row.batting_position, row.batsman, row.team_name, row.how_out, row.bowler, row.runs, row.balls_played, row.fours, row.sixes, row.captain, row.wicket_keeper, row.missed_fifty, row.missed_century]
        cur.execute("INSERT INTO match_innings (match_id, inning_no, batting_position, batsman, team_name, how_out, bowler, runs, balls_played, fours, sixes, captain, wicket_keeper, missed_fifty, missed_century) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", val)
    except Exception as e:
        # if there is an error during upload, we can print the error and the match_id
        mydb.commit()
        print("\nError:", e)
        print("match_id: " + str(row.match_id))
        # To make sure the database does not crash, we can commit every 100 rows
    if(count % 100 == 0):
        mydb.commit()
    if(count % 500 == 0):
        cur.execute("SELECT 1")
    count += 1
# Committing the final changes
mydb.commit()

print()

# For each row in the match_fow pandas table, we can upload the data to the database
for row in tqdm(match_fow.itertuples(), total=match_fow.shape[0], desc="Uploading FOW"):
    try:
        val = [row.match_id, row.inning_no, row.wicket_no, row.runs, row.player_name]
        cur.execute("INSERT INTO match_fow (match_id, inning_no, wicket_no, runs, player_name) VALUES (%s, %s, %s, %s, %s)", val)
    except Exception as e:
        # if there is an error during upload, we can print the error and the match_id
        mydb.commit()
        print("\nError:", e)
        print("match_id: " + str(row.match_id))
    # To make sure the database does not crash, we can commit every 100 rows
    if(count % 100 == 0):
        mydb.commit()
    # To make sure database is still connected, we can check every 500 rows
    if(count % 500 == 0):
        cur.execute("SELECT 1")
    count += 1
# Committing the final changes
mydb.commit()

print()

# For each row in the match_extras pandas table, we can upload the data to the database
for row in tqdm(match_extra.itertuples(), total=match_extra.shape[0], desc="Uploading Extras"):
    try:
        val = [row.match_id, row.inning_no, row.team, row.byes, row.leg_byes, row.wides, row.no_balls, row.penalty_runs]
        cur.execute("INSERT INTO match_extras (match_id, inning_no, team, byes, leg_byes, wides, no_balls, penalty_runs) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", val)
    except Exception as e:
        # if there is an error during upload, we can print the error and the match_id
        mydb.commit()
        print("\nError:", e)
        print("match_id: " + str(row.match_id))
    # To make sure the database does not crash, we can commit every 100 rows
    if(count % 100 == 0):
        mydb.commit()
    # To make sure database is still connected, we can check every 500 rows
    if(count % 500 == 0):
        cur.execute("SELECT 1")
    count += 1
# Committing the final changes
mydb.commit()

print()
print("------------------SQL Data Upload Complete------------------")

# Close the cursor and the database connection
cur.close()
mydb.close()

#print minutes and seconds it took to run the script
print("------Script completed in " + str(int((time.time() - start_time)/60)) + " : " + str(round((time.time() - start_time)%60, 2)) + " seconds------ ")