import csv
from collections import defaultdict
from datetime import datetime, timedelta

# Specify the path to your CSV file
csv_file_path = "nba_schedule.csv"

schedule = defaultdict(lambda: defaultdict(str))

# Open the CSV file
with open(csv_file_path, 'r') as csv_file:
    # Create a CSV reader object with a dictionary interface
    csv_reader = csv.DictReader(csv_file)

    # Iterate over the rows in the CSV file
    for row in csv_reader:
        # You can access values by column name
        homeTeam = row['Home/Neutral']
        guestTeam = row['Visitor/Neutral']
        dateObj = datetime.strptime(row['Date'], "%a %b %d %Y")
        schedule[homeTeam][dateObj] = guestTeam
        schedule[guestTeam][dateObj] = homeTeam


#### opponent back to back count
# opponentBackToBack = defaultdict(int)
# for team, teamSchedule in schedule.items():
#     for date, opponent in teamSchedule.items():
#         previousDay = date - timedelta(days=1)
#         if previousDay in schedule[opponent]:
#             opponentBackToBack[team] += 1
#
# for key, value in sorted(opponentBackToBack.items(), key=lambda item: item[1], reverse=True):
#     print(key, value)


#### self back to back count
selfBackToBack = defaultdict(int)
for team, teamSchedule in schedule.items():
    for date, _ in teamSchedule.items():
        previousDay = date - timedelta(days=1)
        if previousDay in teamSchedule:
            selfBackToBack[team] += 1
for key, value in sorted(selfBackToBack.items(), key=lambda item: item[1], reverse=True):
    print(key, value)