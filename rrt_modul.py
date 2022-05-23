#!/usr/bin/env python
# coding: utf-8

# In[ ]:

"""""
Функция ищет ролинг ретеншн
df_1 - датафрейм с логинами и установками
df_2 - датафрейм с логинами и началом ивента
"""""

def calculate_rolling_ret(df_1, df_2):
    import pandas as pd
    import numpy as np
    import datetime as dt
    
    #удаление дубликатов
    df_1 = df_1.drop_duplicates().reset_index(drop=True)
    df_2 = df_2.drop_duplicates().reset_index(drop=True)
    
    #мердж двух df 
    summary_installs_logins = df_1.merge(
    df_2,
    on = 'client_id',
    how = 'inner')
    
    #приведение сводной таблицы в нужный вид
    summary_installs_logins = (summary_installs_logins
                           .rename(columns = {'install_date':'min_date', 'event_timestamp':'max_date'})
                           .groupby('client_id',as_index = False)
                           .agg({'min_date':'min', 'max_date':'max'})
                          )
      
    #расчет лайфтайма
    summary_installs_logins['life_time'] = summary_installs_logins['max_date'] - summary_installs_logins['min_date']
    summary_installs_logins['life_time'] = summary_installs_logins['life_time']/np.timedelta64(1, 'D')
    summary_installs_logins['life_time'] = summary_installs_logins['life_time'].astype('int')
    
    #расчет количества уникальных пользователеей по lt
    summary_installs_logins['life_time']
    count_users = {}

    for char in range(summary_installs_logins['life_time'].min(), summary_installs_logins['life_time'].max()+1):
        result = summary_installs_logins[summary_installs_logins['life_time'] >= char]['client_id'].nunique()
        count_users[char] = result
    
    #расчет ролинг ретеншн рейт
    roll_retan_rate = pd.DataFrame(list(count_users.items()), columns=['lifetime', 'count_users'])
    roll_retan_rate['RRRT'] = (roll_retan_rate['count_users'] / roll_retan_rate['count_users']
                           .shift()
                           .fillna(roll_retan_rate['count_users'].max()))
    
    
    return roll_retan_rate




