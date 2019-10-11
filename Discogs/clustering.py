import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder, MinMaxScaler
from sklearn.cluster import KMeans, Birch
from matplotlib import pyplot
from mpl_toolkits.mplot3d import Axes3D
from collections import defaultdict
import pyodbc, os, sys

def normalize(df):
    x = df.values #returns a numpy array
    min_max_scaler = MinMaxScaler()
    x_scaled = min_max_scaler.fit_transform(x)
    return pd.DataFrame(x_scaled)

if __name__ == "__main__":
    try:
        conn = pyodbc.connect("Driver={SQL Server};Server=localhost\\SQLEXPRESS;Database=Discogs;Trusted_Connection=yes;")
            
        if not len(sys.argv) in [4, 5, 6]:
            print('Usage: %s ALGORITHM CLUSTERS_NUMBER ATTRIBUTES' % sys.argv[0])
            print('ALGORITHM = K (KMeans) or B (Birch)')
            print('ATTRIBUTES = G (Genres) Y (Year) S (Styles)')
            os.abort()
        elif "kKbB".find(sys.argv[1]) == -1:
            print("Invalid algorithm!")
            os.abort()
        elif int(sys.argv[2]) < 1:
            print("Invalid clusters number!")
            os.abort()
        else:
            atr = []
            if 'GgYySs'.find(sys.argv[3]) == -1:
                print("Invalid attribute!")
                os.abort()
            else:
                atr.append(sys.argv[3].upper())
            
            if len(sys.argv) > 4 and 'GgYySs'.find(sys.argv[4]) == -1:
                print("Invalid attribute!")
                os.abort()
            elif len(sys.argv) > 4:
                atr.append(sys.argv[4].upper())

            if len(sys.argv) > 5 and 'GgYySs'.find(sys.argv[5]) == -1:
                print("Invalid attribute!")
                os.abort()
            elif len(sys.argv) > 5:
                atr.append(sys.argv[5].upper())
            
            atr = list(set(atr))

        alg_type = sys.argv[1]
        num = int(sys.argv[2])

        d = defaultdict(LabelEncoder)
            
        sql = """SELECT alb.Id, alb.Name, alb.Year, albumSt.Styles, albumGe.Genres FROM ALBUMS alb
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
                WHERE albumSt.Styles is not null"""
        df = pd.read_sql(sql, conn)
        
        if 'G' not in atr:
            df = df.drop('Genres', 1)
        if 'Y' not in atr:
            df = df.drop('Year', 1)
        if 'S' not in atr:
            df = df.drop('Styles', 1)
        col = df.columns
        #df = df.sample(10000)
        
        # Encoding the variable
        fit2 = df.apply(lambda x: d[x.name].fit_transform(x))
        fit = fit2.drop('Id', 1)
        fit = fit.drop('Name', 1)
        fit = normalize(fit)
        
        if alg_type == 'K' or alg_type == 'k':
            alg = KMeans(n_clusters=num)
        else:
            alg = Birch(n_clusters=num, threshold=0.1)
            
        alg.fit(fit)

        clusters = {}
        n = 0
        for item in alg.labels_:
            if item in clusters:
                clusters[item].append(fit2.iloc[n])
            else:
                clusters[item] = [fit2.iloc[n]]
            n +=1
        
        cs = []
        for item in clusters:
            cs.append(pd.DataFrame(clusters[item], columns=col).apply(lambda x: d[x.name].inverse_transform(x)))

        i = 0
        for c in cs:
            c.to_excel("Cluster " + str(i) + ".xlsx")
            i += 1

        if(len(col) == 5):
            fig = pyplot.figure(figsize=(4, 3))
            ax = Axes3D(fig, rect=[0, 0, .95, 1], elev=48, azim=134)
            labels = alg.labels_
            ax.scatter(fit.iloc[:,0], fit.iloc[:,1], fit.iloc[:,2],
                        c=labels.astype(np.float), cmap='rainbow')
            
            if alg_type == 'K' or alg_type == 'k':
                ax.scatter(alg.cluster_centers_[:,0] ,alg.cluster_centers_[:,1],alg.cluster_centers_[:,2], color='black')  
                ax.set_title('KMeans')
            else:
                ax.set_title('Birch')

            ax.w_xaxis.set_ticklabels([])
            ax.w_yaxis.set_ticklabels([])
            ax.w_zaxis.set_ticklabels([])
            ax.set_xlabel('Year')
            ax.set_ylabel('Styles')
            ax.set_zlabel('Genre')
            ax.dist = 12
            pyplot.show()

        conn.close()
    except Exception as ex:
        print("Error: " + str(ex))
        conn.close()