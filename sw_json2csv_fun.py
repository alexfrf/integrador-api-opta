# -*- coding: utf-8 -*-
"""
Created on Tue Jan 21 22:08:55 2025

@author: aleex
Fichero experimental planteado para convertir ficheros json de WHOSCORED a csv
"""


import json
import pandas as pd
import os
import numpy as np
import shutil

os.path.abspath(__file__)



def lectura_json(ruta, fichero):
    with open("{}/{}".format(ruta,fichero), "r", encoding="utf-8") as f:
        data = json.load(f)
        return data

def get_plains(data,key):
    return data[key]


def extract_qualifiers(qualifiers,kv):
    if not isinstance(qualifiers, list):  # Si no es una lista, devolver diccionario vacío
        return {}

    extracted = {}
    for q in qualifiers:
        if isinstance(q, dict):  # Verifica que sea un diccionario
            type_info = q.get('qualifierId', {})  # Obtiene el diccionario 'type'
            display_name=kv[type_info]
            if q.get('value'):
                value = q.get('value')  # Extrae 'value'
            else:
                value=1
            if display_name:  # Solo agrega si displayName existe
                extracted[f"value_{display_name}"] = value
    return extracted

def get_match(data,expand_cols=["tournamentCalendar","contestant","competition"]):
    d={k: v for k, v in data['matchInfo'].items() if not isinstance(v, list) and not isinstance(v,dict)}
    df_match=pd.DataFrame(d,index=[get_plains(d,'id')]).reset_index().rename({"index":"matchId"},axis=1)
    for c in expand_cols:
        ha={0:"home",1:"away"}
        if c=='contestant':
            for x in ha:
                ref = {k: v for k, v in data['matchInfo'][c][x].items() if not isinstance(v, list) and not isinstance(v,dict)}
                for i in ref:
                    col=ha[x]+"_"+i
                    df_match[col]=ref[i]
        else:
            ref = {k: v for k, v in data['matchInfo'][c].items() if not isinstance(v, list) and not isinstance(v,dict)}
            for i in ref:
                col=c+"_"+i
                df_match[col]=ref[i]
    df_match["venueName"] = data['matchInfo']["venue"]["shortName"]
    df_match['expandedMaxMinute']=data['liveData']['matchDetails']['matchLengthMin']
    df_match['matchStatus']=data['liveData']['matchDetails']['matchStatus']
    #df_match['season']
    return df_match

def get_teams(data):
    df = {key: pd.DataFrame(value) for key, value in data['matchInfo'].items() if isinstance(value, list)}['contestant']
    df['country'] = df['country'].apply(lambda x: x['name'])
    df['matchId'] = get_plains(data['matchInfo'],'id')
    df['scores.halftime'] = np.where(df.position=="home",
                                     data['liveData']['matchDetails']['scores']['ht']['home'],
                                     data['liveData']['matchDetails']['scores']['ht']['away'])
    df['scores.fulltime'] = np.where(df.position=="home",
                                     data['liveData']['matchDetails']['scores']['ft']['home'],
                                     data['liveData']['matchDetails']['scores']['ft']['away'])
    df.columns=['teamId', 'teamName', 'shortName', 'officialName', 'code', 'field',
           'country', 'matchId', 'scores.halftime','scores.fulltime']
    return df

def get_players(data,file_playerformation="config/player_formation.xlsx"):
    players= data.drop_duplicates(subset=['playerId'],keep='first')[['playerId','playerName']]
    player_formation=pd.read_excel(file_playerformation)
    for i in player_formation.columns:
        player_formation[i]= player_formation[i].apply(lambda x: str(x))
    data=data[data.value_PlayerPosition.isna()==False]
    df_ts = data[data.type_displayName=='Team set up']
    df_subon = data[data.type_displayName.str.contains("Player on")]
    df_suboff = data[data.type_displayName.str.contains("Player off")]
    cols_expand = [
    'value_PlayerPosition',
    'value_Involved',
    'value_TeamPlayerFormation',
    'value_JerseyNumber'
    ]
    cols=[
    'value_PlayerPosition',
    'value_Involved',
    'value_TeamPlayerFormation',
    'value_JerseyNumber',
    'value_TeamFormation',
    'teamId',
    'matchId',
    'field'
    ]
    ncols=[
    'field_position',
    'playerId',
    'value_TeamPlayerFormation',
    'shirtNo',
    'value_TeamFormation',
    'teamId',
    'matchId',
    'field'
    ]

# Primero, aseguramos que las columnas sean listas (no strings con comas)
    for col in cols_expand:
        df_ts[col] = df_ts[col].apply(lambda x: x.split(', ') if isinstance(x, str) else x)
    
    df_ex = df_ts.explode(column=cols_expand, ignore_index=True)[cols]
    df_ex.columns=ncols
    df_ex['isFirstEleven']=np.where(df_ex.value_TeamPlayerFormation==0,0,1)
    df_ex= pd.merge(df_ex,player_formation,
                    how='left',left_on=['value_TeamPlayerFormation',"value_TeamFormation"],
                    right_on=['player_formation','team_formation'])
    df_ex.drop(['player_formation','team_formation'],inplace=True,axis=1)
    df_ex.rename({"player_formation_name":"position"},inplace=True,axis=1)
        
    df_subon['subbedOutPlayerId']= df_subon['relatedPlayerId']
    df_subon['subbedInExpandedMinute']=df_subon.minute
    df_subon['subbedInPeriod_value']=df_subon.periodId
    
    df_suboff['subbedInPlayerId']= df_suboff['relatedPlayerId']
    df_suboff['subbedOutExpandedMinute']=df_suboff.minute
    df_suboff['subbedOutPeriod_value']=df_suboff.periodId
    
    df_ex = pd.merge(df_ex,players,on="playerId",how='left')
    df_ex=pd.merge(df_ex,
                   df_subon[['playerId','subbedOutPlayerId','subbedInExpandedMinute',
                             'subbedInPeriod_value']],how='left',on="playerId")
    df_ex=pd.merge(df_ex,
                   df_suboff[['playerId','subbedInPlayerId','subbedOutExpandedMinute',
                             'subbedOutPeriod_value']],how='left',on="playerId")
    return df_ex



def get_events(data, 
               dict_cols=[], 
               file_types="config/typeId.csv",file_qualifiers="config/qualifiers.csv",
               merge_teams=['teamId','teamName','field'],
               drop_qualifiers=0):
    df = {key: pd.DataFrame(value) for key, value in data['liveData'].items() if isinstance(value, list)}['event']
    df['matchId']=get_plains(data["matchInfo"],'id')
    types = pd.read_csv(file_types,sep=";")
    df=pd.merge(df,types[['typeId','type_displayName']],how='left',on='typeId')
    
    qualifiers_df = pd.read_csv(file_qualifiers,sep=";")
    qualifiers_df['qname'] = qualifiers_df['QUALIFIER NAME'].apply(lambda x: x.title().replace(" ","").replace("+","").replace("-","").replace("Coordinate","").strip())
    du={}
    for i,j in qualifiers_df.iterrows():
        du[j['qualifierId']]=j['qname']
    
    df = df.drop(columns=[i for i in dict_cols if i in df.columns])
    df_expanded = df.join(df['qualifier'].apply(lambda x: pd.Series(extract_qualifiers(x,du))))
    if "value_Unknown" in df_expanded.columns:
        df_expanded.drop("value_Unknown",inplace=True,axis=1)
    
    # Eliminar la columna original 'qualifiers'
    if drop_qualifiers:
        df_expanded.drop(columns=['qualifiers'],inplace=True)
    
    
    df_match=get_match(data)
    df_tm=get_teams(data)
    
    df_expanded = pd.merge(df_expanded,df_tm[merge_teams],left_on="contestantId",right_on="teamId",how='left')
    df_expanded['endX'] = df_expanded["value_PassEndX"]
    df_expanded['endY'] = df_expanded["value_PassEndY"]
    df_expanded['blockedX'] = df_expanded["value_BlockedX"]
    df_expanded['blockedY'] = df_expanded["value_BlockedY"]
    df_expanded['goalMouthY'] = df_expanded['value_GoalmouthY']
    df_expanded['goalMouthZ'] = df_expanded['value_GoalmouthZ']
    if "value_RelatedEventId" in df_expanded.columns:
        df_expanded['relatedEventId'] = df_expanded['value_RelatedEventId'].apply(
        lambda x: int(x) if pd.notnull(x) and str(x).isdigit() else None
        )
        df_related = df_expanded[df_expanded['relatedEventId'].isna()==False]
        df_related = pd.merge(df_related[["relatedEventId",'teamId']],df_expanded[["eventId",'playerId','teamId']],
                              left_on=["relatedEventId",'teamId'],right_on=['eventId','teamId'])[['eventId','playerId']]
        df_related.rename({"eventId":"relatedEventId","playerId":"relatedPlayerId"},inplace=True,axis=1)
        df_expanded = pd.merge(df_expanded,df_related,on="relatedEventId",how='left')
    else:
        df_expanded["relatedPlayerId"]= ""
        df_expanded["relatedEventId"]=0
    
    if "value_Penalty" in df_expanded.columns:
        df_expanded['is_penalty'] = df_expanded.value_Penalty
    else:
        df_expanded['is_penalty']=0
    df_expanded['isGoal'] = np.where(df_expanded.typeId==16,1,0)
    
    
    if "value_Blocked" in df_expanded.columns:
        df_expanded['value_Blocked'] = df_expanded.value_Blocked
    else:
        df_expanded['value_Blocked']=0
    df_expanded['minute'] = df_expanded.timeMin
    df_expanded['second'] = df_expanded.timeSec
    
    
    df_expanded["oppositionTeamName"] = np.where(df_expanded.teamName==df_tm.teamName.values[0],
                                                 df_tm.teamName.values[1],
                                                 df_tm.teamName.values[0])
    #df_expanded["refName"] = get_plains(data['matchCentreData']["referee"],"name")
    df_expanded["time_seconds"]= df_expanded.timeMin*60 + df_expanded.timeSec
    df_pl=get_players(df_expanded)
    df_expanded= pd.merge(df_expanded,df_pl[['playerId','position','isFirstEleven']],
                          how='left',on='playerId')
    df_expanded= df_expanded.drop_duplicates(subset=['id','eventId','matchId'],keep='first')
    
    for col in df_expanded.columns:
        if "value_" in col or col.lower()[-1] in ['x', 'y']:
            df_expanded[col] = (
                df_expanded[col]
                .apply(pd.to_numeric, errors='coerce')  # convierte a número (NaN si falla)
            )
    
    return {"eventData":df_expanded,"matchData":df_match,"teamData":df_tm, "playerData":df_pl}

def procesar_fichero(ruta,fichero,output, export=1):
    try:
        print("\nParseando fichero de partido: {}".format(fichero))
        result = lectura_json(ruta, fichero)
        try:
            game_data = get_events(result)
            if export:
                for k in game_data:
                    game_data[k].to_csv("{}/{}_{}.csv".format(os.path.join(ruta,output),
                                                              get_plains(result['matchInfo'],'id'),
                                                              k
                                                              ),decimal=',',sep=';',index=False)
            #old_dir = os.path.join(ruta, "old")
            #os.makedirs(old_dir, exist_ok=True)  # Crear la carpeta si no existe
            #shutil.move(os.path.join(ruta, fichero), os.path.join(old_dir, fichero))
            print("Proceso Completado")
        except Exception as e:
            print(e)
            pass
    except Exception as e:
        print("ERROR - {}".format(e))

def procesar_ficheros_lista(ruta,subr):
    json_list=[f for f in os.listdir(ruta) if f.endswith(".json") and f.split("_")[-1].replace(".json",".csv").replace(".","_eventData.") not in os.listdir(os.path.join(ruta,subr))]
    counter=0
    counter_ok=0
    for json_file in json_list:
        counter+=1
        print("\n({}/{})".format(counter,len(json_list)))
        procesar_fichero(ruta,json_file,subr)
        counter_ok+=1
    print("\n\nFicheros Leidos: {}".format(counter))
    print("Ficheros Parseados: {}".format(counter_ok))
    
