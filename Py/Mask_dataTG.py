"""
library
pip install pandas
pip install pyTigerGraph
pip install flat_table
"""
import pandas as pd
import pyTigerGraph as tg
import flat_table

# Connect to TigerGraph
conn = tg.TigerGraphConnection(host="https://____.i.tgcloud.io", password="_____")
conn.graphname = "Mask" # Graph Name
conn.apiToken = conn.getToken(conn.createSecret()) # Graph Token

# Create query gsql and run it
print(conn.gsql('''
CREATE QUERY st(/* Parameters here */) FOR GRAPH Mask { 
  /* Write query logic here */
  Seed = {test.*};
  tmp = SELECT s From Seed:s;
  PRINT tmp; 
}
INSTALL QUERY st
''', options=[]))
select = conn.runInstalledQuery("st", {})

# Get result query gsql and transform it to dataframe
df_tx_hop = pd.DataFrame(select)
df_tx_hop = flat_table.normalize(df_tx_hop)
df_tx_hop = df_tx_hop.drop(columns=["index", "tmp.v_type", "tmp.v_id"])

df_tx_hop['tmp.attributes.id'] = df_tx_hop['tmp.attributes.id'].astype(int) # Change Dtype dataframe, with compare dtype on vertex

# Function for Masking data
def mask_email(Var):
    mail=Var.split("@")[0]
    n=len(mail)
    alist=list(mail)
    alist[1:int(n)-1]='*'*int(n-2)
    output="".join(alist)+"@"+Var.split("@")[1]
    return output
def mask_mobile(Var):
    Var=str(Var)
    n=len(Var)
    alist=list(Var)
    alist[2:int(n)-2]='*'*int(n-4)
    output="".join(alist)
    return output
import re
def mask_credit(Var):
    output = re.sub('\d', '*', Var[:-4]) + Var[-4:]
    return output

# Apply Function Masking data to Dataframe
df_tx_hop['tmp.attributes.email']= df_tx_hop['tmp.attributes.email'].apply(mask_email)
df_tx_hop['tmp.attributes.mobile']= df_tx_hop['tmp.attributes.mobile'].apply(mask_mobile)
df_tx_hop['tmp.attributes.credit']= df_tx_hop['tmp.attributes.credit'].apply(mask_credit)

# load data Dataframe to TigerGraph
v_test = conn.upsertVertexDataFrame(df_tx_hop, "test", "tmp.attributes.id", attributes={"id": "tmp.attributes.id", "name": "tmp.attributes.name", "email": "tmp.attributes.email", "mobile": "tmp.attributes.mobile", "credit": "tmp.attributes.credit"})
