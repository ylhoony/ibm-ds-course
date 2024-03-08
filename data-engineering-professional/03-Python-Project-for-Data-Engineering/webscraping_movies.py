import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

path = "./webscraping"

# Initialization of known entities
url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = f'{path}/Movies.db'
table_name = 'Top_50'
csv_path = f'{path}/top_50_films.csv'
df = pd.DataFrame(columns=["Average Rank", "Film", "Year", "Rotten Tomatoes' Top 100"])
count = 0

# Loading the webpage for Webscraping
html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')

# Analyzing the HTML code for relevant information

# Scraping of required information
tables = data.find_all('tbody')
rows = tables[0].find_all('tr')

for row in rows:
    if count < 25:
        col = row.find_all('td')

        if len(col) != 0:
            data_dict = {"Average Rank": col[0].contents[0],
                         "Film": col[1].contents[0],
                         "Year": int(col[2].contents[0]),
                         "Rotten Tomatoes' Top 100": col[3].contents[0]}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
            count += 1
    else:
        break

print(df[df["Year"] > 1999])

# Storing the data
df.to_csv(csv_path)

conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists='replace', index=False)
conn.close()
