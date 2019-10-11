import pandas as pd
import pyodbc, os, sys

if __name__ == "__main__":
    try:
        if not len(sys.argv) in [3]:
            print('Usage: %s OPTION OUTPUT_XLSX_FILE' % sys.argv[0])
            print('OPTIONS = [A, B, C, D, E, F]')
            os.abort()
        elif "AaBbCcDdEeFf".find(sys.argv[1]) == -1:
            print("Invalid option!")
            os.abort()

        conn = pyodbc.connect("Driver={SQL Server};Server=localhost\\SQLEXPRESS;Database=Discogs;Trusted_Connection=yes;")
        sql = ''
        if sys.argv[1] == 'a' or sys.argv[1] == 'A':
            sql = """SELECT Name AS Genre, Count(*) AS AlbumsNumber
                    FROM AlbumGenres
                    GROUP BY Name
                    ORDER BY AlbumsNumber DESC"""
        elif sys.argv[1] == 'b' or sys.argv[1] == 'B':
            sql = """SELECT Name AS Style, Count(*) AS AlbumsNumber
                    FROM AlbumStyles
                    GROUP BY Name
                    ORDER BY AlbumsNumber DESC"""
        elif sys.argv[1] == 'c' or sys.argv[1] == 'C':
            sql = """SELECT DENSE_RANK() OVER(ORDER BY Versions DESC) AS #, Versions, art.Name as Artist, alb.Name as Album
                    FROM Albums alb
                    LEFT JOIN Artists art ON art.Id = alb.ArtistId
                    WHERE Versions >=
                    (
                        SELECT TOP 1 Versions
                        FROM (
                            SELECT TOP 20 Versions
                            FROM Albums
                            GROUP BY Versions
                            ORDER BY Versions DESC
                            ) AS MyTable
                        ORDER BY Versions ASC
                    )
                    ORDER BY Versions DESC"""
        elif sys.argv[1] == 'd' or sys.argv[1] == 'D':
            sql = """SELECT TOP 100 Name, Credits
                    FROM Artists
                    ORDER BY Credits DESC"""
            sql2 = """SELECT TOP 100 Name, Vocals
                    FROM Artists
                    ORDER BY Vocals DESC"""
            sql3 = """SELECT TOP 100 a.Name, COUNT(*) AS Arrangements
                    FROM Tracks t
                    LEFT JOIN Artists a ON a.Id = t.ArrangedBy
                    WHERE t.ArrangedBy IS NOT NULL
                    GROUP BY a.Name
                    ORDER BY Arrangements DESC"""
            sql4 = """SELECT TOP 100 a.Name, COUNT(*) AS Lyrics
                    FROM Tracks t
                    LEFT JOIN Artists a ON a.Id = t.LyricsBy
                    WHERE t.LyricsBy IS NOT NULL
                    GROUP BY a.Name
                    ORDER BY Lyrics DESC"""
            sql5 = """SELECT TOP 100 a.Name, COUNT(*) AS Music
                    FROM Tracks t
                    LEFT JOIN Artists a ON a.Id = t.MusicBy
                    WHERE t.MusicBy IS NOT NULL
                    GROUP BY a.Name
                    ORDER BY Music DESC"""
        elif sys.argv[1] == 'e' or sys.argv[1] == 'E':
            sql = """SELECT TOP 100 t.Name, Count(*) as Count,
                    STRING_AGG(CAST('Id: ' + CAST(albums.Id AS nvarchar(10)) + '; Name: ' + albums.Name AS nvarchar(max)), CHAR(13)+CHAR(10)) AS Albums,
                    STRING_AGG(CAST('Id: ' + CAST(albums.Id AS nvarchar(10)) + '; Country: ' + albums.Country + '; Year: ' + CAST(albums.Year AS nvarchar(10)) AS nvarchar(max)), CHAR(13)+CHAR(10)) AS Albums,
                    STRING_AGG(CAST('Id: ' + CAST(albums.Id AS nvarchar(10)) + '; Format: ' + albums.Format AS nvarchar(max)), CHAR(13)+CHAR(10)) AS Details,
                    STRING_AGG(CAST('Id: ' + CAST(albums.Id AS nvarchar(10)) + '; Genres: ' + albums.Genres AS nvarchar(max)), CHAR(13)+CHAR(10)) AS Genres,
                    STRING_AGG(CAST('Id: ' + CAST(albums.Id AS nvarchar(10)) + '; Styles: ' + albums.Styles AS nvarchar(max)), CHAR(13)+CHAR(10)) AS Styles
                FROM Tracks t
                LEFT JOIN (
                    SELECT alb.Id, alb.Name, alb.Format, alb.Country, alb.Year, albumSt.Styles, albumGe.Genres FROM ALBUMS alb
                    LEFT JOIN
                    (
                        SELECT a.Id, STRING_AGG(st.Name, '; ') AS Styles
                        FROM Albums a
                        LEFT JOIN AlbumStyles st on st.AlbumId = a.Id
                        GROUP BY a.Id
                    ) albumSt on albumSt.Id = alb.Id
                    LEFT JOIN
                    (
                        SELECT a.Id, STRING_AGG(ge.Name, '; ') AS Genres
                        FROM Albums a
                        LEFT JOIN AlbumGenres ge on ge.AlbumId = a.Id
                        GROUP BY a.Id
                    ) albumGe on albumGe.Id = alb.Id
                ) albums on t.AlbumId = albums.Id
                GROUP BY t.Name
                ORDER BY Count DESC"""
        elif sys.argv[1] == 'f' or sys.argv[1] == 'F':
            sql = """SELECT Name, Sites
                    FROM Artists
                    WHERE Sites IS NOT NULL"""
        
        df = pd.read_sql(sql, conn)

        if sys.argv[1] == 'd' or sys.argv[1] == 'D':
            df2 = pd.read_sql(sql2, conn)
            df = pd.concat([df, df2], axis=1)

            df3 = pd.read_sql(sql3, conn)
            df = pd.concat([df, df3], axis=1)

            df4 = pd.read_sql(sql4, conn)
            df = pd.concat([df, df4], axis=1)
            
            df5 = pd.read_sql(sql5, conn)
            df = pd.concat([df, df5], axis=1)

        df.to_excel(sys.argv[2])

        print("Results saved to: " + sys.argv[2])

        conn.close()
    except Exception as ex:
        print("Error: " + str(ex))
        conn.close()