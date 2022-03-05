#Integrasi database Mariadb
def query_mariadb(query):
    import pandas as pd
    import pymysql
    import mariadb
    # Connect to MariaDB 
    dbconn = mariadb.connect(
        user="root",
        password="pass123",
        host="127.0.0.1",
        port=3307,
        database="test"
    )
    # mariaDB Query to Pandas DataFrame
    query_result= pd.read_sql(query,dbconn)
    dbconn.close()
    return query_result

#Extract data Mariadb dengan query
sql = "SELECT * FROM person"
df = query_mariadb(sql)
#Integrasi Tigergraph
import pyTigerGraph as tg
conn = tg.TigerGraphConnection(host="https://_______.i.tgcloud.io", password="_______")
"""
#Membuat vertex dan graph jika belum ada schem pada Tigergraph
print(conn.gsql('''
  CREATE VERTEX test(PRIMARY_ID id INT, name STRING, email STRING, mobile STRING, credit STRING) WITH primary_id_as_attribute="true"
''', options=[]))
print(conn.gsql('''CREATE GRAPH Mask(test)''', options=[]))
"""
#Integrasi dengan Graph schema Tigergraph
conn.graphname = "Mask"
secret = conn.createSecret()
token = conn.getToken(secret, setToken=True)
print(token)
#Mapping dan loading data ke tigergraph
results=conn.gsql('''
  USE GRAPH Mask
  DROP JOB load_people
  BEGIN
  CREATE LOADING JOB load_people FOR GRAPH Mask {
  DEFINE FILENAME MyDataSource;
  LOAD MyDataSource TO VERTEX test VALUES($0, $1, $2, $3, $4) USING SEPARATOR=",", HEADER="true", EOL="\\n";
  }
  END
''', options=[])
print(results)
df.to_csv("mask_data.csv", index=None)
import json
#Load the file with the 'load_people' job
people_file='mask_data.csv'
results=conn.uploadFile(people_file, fileTag='MyDataSource', jobName='load_people')
print(json.dumps(results,indent=2))
