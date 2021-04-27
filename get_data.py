#%%
import re                          # Expressão regulares
import requests                    # Acessar páginas da internet
from bs4 import BeautifulSoup      
import pandas as pd
import sqlite3
#%%
def read_cartola_data(year):
    '''
    Read data from a given year of the CaRtola repository

    Parameters:
    year (int) - year inside the range 2018-2020.
    ''' 

    if year in [2018, 2019, 2020]:
        # URL para baixar os arquivos
        url = 'https://github.com/henriquepgomide/caRtola/tree/master/data/{}'.format(year)
        html = requests.get(url)
    
        soup = BeautifulSoup(html.text, 'lxml')
    
        dict_of_files = {}
        for tag in soup.find_all(r'a', attrs={'href': re.compile(r'rodada-([0-9]|[0-9][0-9])\.csv')}):
            href_str = tag.get('href')
            file_name = re.sub(r'/henriquepgomide/caRtola/blob/master/data/{}/'.format(year), 
                            '', 
                            href_str)
            
            file_url = re.sub(r'/henriquepgomide/caRtola/blob/master/data/{}/'.format(year), 
                            r'https://raw.githubusercontent.com/henriquepgomide/caRtola/master/data/{}/'.format(year), 
                            href_str)
            dict_of_files[file_name] = file_url

        list_of_dataframes = []
        for key, item in dict_of_files.items():
            df = pd.read_csv(item)
            df['rodada'] = key
            list_of_dataframes.append(df)
    
        df_cartola = pd.concat(list_of_dataframes)
    
        return df_cartola
    
    else:
        print('You need to add an year within the range: 2018 and 2020')
#%%
df = read_cartola_data(2018)
df_2 = read_cartola_data(2019)
df_3 = read_cartola_data(2020)
#%%
df['ano'] = 2018
df_2['ano'] = 2019
df_3['ano'] = 2020
#%%
df.info()
#%%
df_2.info()
#%%
df_3.info()
#%%
df_3.drop(columns={'athletes$atletas$scout','Unnamed: 0'},inplace=True)
#%%
df_2.drop(columns={'Unnamed: 0'},inplace=True)
df.drop(columns={'Unnamed: 0'},inplace=True)
#%%
df_2['atletas.clube_id'] = df_2['atletas.clube_id'].astype('object')
df_3['atletas.clube_id'] = df_3['atletas.clube_id'].astype('object')
#%%
df4 = pd.concat([df_2,df_3])
df5 = pd.concat([df4,df])
#%%
conn = sqlite3.connect(r'cartolafc_database.db')
#%%
df5.to_sql('database_until_2020',schema=None,index=False,if_exists='append',con=conn)
# %%
df5.to_excel(r'dados_gerais.xlsx',index=False)
# %%
