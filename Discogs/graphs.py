import pyodbc, os, sys, re
import matplotlib.pyplot as plt
import pandas as pd

def make_autopct(values):
    def my_autopct(pct):
        total = sum(values)
        val = int(round(pct*total/100.0))
        return '{p:.2f}%  ({v:d})'.format(p=pct,v=val)
    return my_autopct

def is_cyrillic(text):
    return bool(re.search('[\u0400-\u04FF]', text))

if __name__ == "__main__":
    try:
        if not len(sys.argv) in [2]:
            print('Usage: %s OPTION' % sys.argv[0])
            print('OPTIONS = [A, B, C, D, E]')
            os.abort()
        elif "AaBbCcDdEe".find(sys.argv[1]) == -1:
            print("Invalid option!")
            os.abort()
        
        conn = pyodbc.connect("Driver={SQL Server};Server=localhost\\SQLEXPRESS;Database=Discogs;Trusted_Connection=yes;")
        sql = ''
        title = ''
        if sys.argv[1] == 'a' or sys.argv[1] == 'A':
            sql = """SELECT TOP 6 Name AS Genre, Count(*) AS AlbumsNumber
                    FROM AlbumGenres
                    GROUP BY Name
                    ORDER BY AlbumsNumber DESC"""
            title = 'Najzastupljeniji žanrovi'
        elif sys.argv[1] == 'b' or sys.argv[1] == 'B':
            sql = """select count(case when Duration < 91 then 1 else null end) as '0-90',
                        count(case when Duration > 90 and Duration < 181 then 1 else null end) as '91-180',
                        count(case when Duration > 180 and Duration < 241 then 1 else null end) as '181-240',
                        count(case when Duration > 240 and Duration < 301 then 1 else null end) as '241-300',
                        count(case when Duration > 300 and Duration < 361 then 1 else null end) as '301-360',
                        count(case when Duration > 360 then 1 else null end) as '360+'
                    from Tracks"""
            title = 'Broj pesama prema trajanju'
        elif sys.argv[1] == 'c' or sys.argv[1] == 'C':
            sql = """select count(case when Year > 1949 and Year < 1960 then 1 else null end) as '1950-1959',
                        count(case when Year > 1959 and Year < 1970 then 1 else null end) as '1960-1969',
                        count(case when Year > 1969 and Year < 1980 then 1 else null end) as '1970-1979',
                        count(case when Year > 1979 and Year < 1990 then 1 else null end) as '1980-1989',
                        count(case when Year > 1989 and Year < 2000 then 1 else null end) as '1990-1999',
                        count(case when Year > 1999 and Year < 2010 then 1 else null end) as '2000-2009',
                        count(case when Year > 2009 and Year < 2020 then 1 else null end) as '2010-2019'
                    from Albums"""
            title = 'Broj albuma po dekadama'
        elif sys.argv[1] == 'd' or sys.argv[1] == 'D':
            sql = "SELECT Name FROM Albums"
            title = 'Broj albuma čiji su naslovi latinica ili ćirilica'
        elif sys.argv[1] == 'e' or sys.argv[1] == 'E':
            sql = """SELECT count(case when x.Genres = 1 then 1 else null end) as 'Jedan žanr',
                        count(case when x.Genres = 2 then 1 else null end) as 'Dva žanra',
                        count(case when x.Genres = 3 then 1 else null end) as 'Tri žanra',
                        count(case when x.Genres > 3 then 1 else null end) as 'Četiri ili više žanrova'
                    FROM
                    (
                        SELECT a.Id, COUNT(*) as Genres
                        FROM Albums a
                        LEFT JOIN AlbumGenres ag ON ag.AlbumId = a.Id
                        GROUP BY a.Id
                    ) x"""
            title = 'Broj žanrova kojima album pripada'

        df = pd.read_sql(sql, conn)

        if sys.argv[1] == 'a' or sys.argv[1] == 'A':
            values = [int(x) for x in df['AlbumsNumber'].tolist()]
            labels = df['Genre'].tolist()
        elif sys.argv[1] == 'b' or sys.argv[1] == 'B' or sys.argv[1] == 'c' or sys.argv[1] == 'C' or sys.argv[1] == 'e' or sys.argv[1] == 'E':
            values = df.iloc[0]
            labels = list(df)
        elif sys.argv[1] == 'd' or sys.argv[1] == 'D':
            total = len(df.index)
            cyrillic = sum(1 for x in df['Name'].tolist() if is_cyrillic(x))
            latin = total - cyrillic
            values = [latin, cyrillic]
            labels = ['Latinica', 'Ćirilica']

        plt.figure(figsize=plt.figaspect(1))
        plt.title(title)
        plt.pie(values, labels=labels, autopct=make_autopct(values))
        plt.show()

        conn.close()
    except Exception as ex:
        print("Error: " + str(ex))
        conn.close()