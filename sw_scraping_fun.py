# -*- coding: utf-8 -*-
"""
Created on Sat May 31 18:10:24 2025

@author: aleex
"""

import requests
import re
from bs4 import BeautifulSoup
import os
import json
from datetime import datetime
import time
import random
import pandas as pd
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# url_competicion = "https://www.scoresway.com/en_GB/soccer/liga-profesional-argentina-2025/3l4bzc8syz1ea2dnv453kp89g/fixtures"
def obtener_sdapi_outlet_key(url_competicion):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5) Chrome/135.0 Mobile Safari/537.36'
    }

    try:
        response = requests.get(url_competicion, headers=headers)
        if response.status_code != 200:
            pass

        soup = BeautifulSoup(response.text, 'html.parser')

        for script in soup.find_all('script'):
            if script.text and "sdapi_outlet_key" in script.text:
                match = re.search(r'sdapi_outlet_key\s*:\s*"([^"]+)"', script.text)
                if match:
                    return match.group(1)

        # Si no se encontró nada en los <script>
        raise ValueError("Añadiendo token por defecto")

    except Exception as e:
        print(f"Error: {e}")
        print("Leyendo sdapi_outlet_key desde config/config.json...")
        try:
            with open("config/config.json", "r", encoding="utf-8") as f:
                config = json.load(f)
                return config['scoresway']['sdapi_outlet_key']
        except Exception as config_error:
            raise RuntimeError("No se pudo obtener sdapi_outlet_key ni desde la web ni desde el archivo config.json") from config_error

    
def obtener_fixture_json(sdapi_outlet_key, torneo_id, callback_id,url_competicion):
    fixture_url = (
        f"https://api.performfeeds.com/soccerdata/match/{sdapi_outlet_key}/"
        f"?_rt=c&tmcl={torneo_id}&live=yes&_pgSz=400&_lcl=en&_fmt=jsonp"
        f"&sps=widgets&_clbk={callback_id}"
    )

    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5) Chrome/135.0 Mobile Safari/537.36',
        'Referer': url_competicion
    }

    response = requests.get(fixture_url, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"No se pudo obtener el fixture JSON: {response.status_code}")

    # Limpiar JSONP y obtener JSON puro
    content = response.text
    json_start = content.find('(') + 1
    json_end = content.rfind(')')
    fixture_data = json.loads(content[json_start:json_end])

    return fixture_data



def generar_dataframe_desde_competicion(url_competicion, fixture_json,metadata):
    
    
    # Extraer competición y torneo ID de la URL de competición
    match_url = re.search(r'soccer/([^/]+)/([^/]+)/fixtures', url_competicion)
    if not match_url:
        raise ValueError("La URL proporcionada no tiene el formato esperado.")
    
    competicion = match_url.group(1)
    torneo_id = match_url.group(2)
    
    # Extraer información de partidos del fixture JSON
    partidos = fixture_json.get('match', [])
    datos_partidos = []
    
    for partido in partidos:
        match_info = partido.get('matchInfo', {})
        partido_id = match_info.get('id')
        fecha = match_info.get('date')
        equipo_local = match_info.get('contestant')[0].get('name')
        equipo_visitante = match_info.get('contestant')[1].get('name')
        estadio = match_info.get('venue', {}).get('name')

        # Generar URL dinámica a partir de la competición y torneo_id
        url = (f"https://www.scoresway.com/en_GB/soccer/{competicion}/{torneo_id}/match/view/{partido_id}/player-stats")
        
        datos_partidos.append({
            'competition':metadata[metadata.url==url_competicion].competicion_id.values[0],
            'season':metadata[metadata.url==url_competicion].season.values[0],
            'date': fecha,
            'home_team': equipo_local,
            'away_team': equipo_visitante,
            'stadium': estadio,
            'match_id': partido_id,
            'URL': url
        })
    datos_extra=[]
    for fi in ["fixtures","results"]:
        try:
            with open("fixtures/extra_{}_{}_{}.txt".format(torneo_id, 
                                                        metadata[metadata.url==url_competicion].season.values[0],
                                                        fi), "r", encoding="utf-8") as f:
                extra = f.read()
            extras = extra.split("data-match=")[1:]
            
            for e in extras:
                partido_id=e.split(" ")[0].replace('"','').strip()
                m = re.search(r'data-date="(\d+)"', e)
                fecha = None
                if m:
                    ts_ms = int(m.group(1))
                    fecha = datetime.utcfromtimestamp(ts_ms/1000).strftime("%Y-%m-%d")
                    dt = pd.to_datetime(ts_ms, unit="ms", utc=True)
                    time  = dt.strftime("%H:%M:%S")   # HH:MM:SS
                # equipo local (primer <td class="Opta-Team Opta-TeamName Opta-Home ..."> ... </a>)
                ht =  e.split("Opta-Team Opta-TeamName Opta-Home Opta-Team-")[1].split(" ")[0].strip()
                at =  e.split("Opta-Team Opta-Away Opta-TeamName Opta-Team-")[1].split(" ")[0].strip()
                url = (f"https://www.scoresway.com/en_GB/soccer/{competicion}/{torneo_id}/match/view/{partido_id}/player-stats")
                datos_extra.append({
                    'competition':metadata[metadata.url==url_competicion].competicion_id.values[0],
                    'season':metadata[metadata.url==url_competicion].season.values[0],
                    'home_team':ht,
                    'away_team':at,
                    'date':fecha,
                    'time':time,
                    'match_id': partido_id,
                    'URL': url
                })
        except:
            print("No existen datos extra: {}".format(fi))
    data = pd.concat([pd.DataFrame(datos_partidos), pd.DataFrame(datos_extra)])
    data=data.drop_duplicates(keep='first',subset="match_id")
    data['date'] = data['date'].fillna(data[data['date'].isna()==False]['date'].values[-1])
    return data

def scrape_fixtures(metadata,url_competicion):


    callback_id = "W3e14cbc3e4b2577e854bf210e5a3c7028c7409678" # la unica incognita si es siempre el mismo o no y si funciona o hay que cambiarlo siempre

    # Definimos los parámetros base
    # sdapi_outlet_key = "ft1tiv1inq7v1sk3y9tv12yh5"  # ya lo tenemos de la competición
    # callback_id = "W3dummycallbackid123456789abcdef"  # callback genérico, no validado
    # callback_id = "W3e14cbc3e4b2577e854bf210e5a3c7028c7409678"  # callback genérico, no validado
    # callback_id = "W3d28817e6ef50641737261ca4c1838b4c8a202e3e"
    
    # Dividimos la URL en partes
    partes = url_competicion.split("/")
    torneo_name = partes[5]  # liga-profesional-argentina-2025
    torneo_name = metadata[metadata.url==url_competicion].competicion_id.values[0] + "_" + metadata[metadata.url==url_competicion].season.values[0]
    torneo_id = partes[6]    # 3l4bzc8syz1ea2dnv453kp89g
    
    print("torneo_name:", torneo_name)
    print("torneo_id:", torneo_id)
    
    # Crear las carpetas si no existen
    os.makedirs("fixtures", exist_ok=True)
    #os.makedirs("partidos", exist_ok=True)
    
    print("✅ Estructura de carpetas creada (si no existía).")
    
    sdapi_outlet_key = obtener_sdapi_outlet_key(url_competicion)
    print(f"sdapi_outlet_key: {sdapi_outlet_key}")
    
    fixture_json = obtener_fixture_json(sdapi_outlet_key, torneo_id, callback_id,url_competicion)
    
    # Guardar para revisión
    with open("fixtures/fixture.json", "w", encoding='utf-8') as f:
        json.dump(fixture_json, f, ensure_ascii=False, indent=4)
    
    print("Fixture guardado exitosamente.")
    
    
    # Leer nuevamente el archivo JSON cargado anteriormente
    ruta_json = 'fixtures/fixture.json'
    with open(ruta_json, 'r', encoding='utf-8') as file:
        fixture_data = json.load(file)
    
    # Generar el DataFrame dinámicamente desde la URL
    df_partidos = generar_dataframe_desde_competicion(url_competicion, fixture_data,metadata)
    
    return df_partidos

def get_json_games(df_partidos,ruta_dest,url_competicion):

    # Crear la carpeta si no existe
    os.makedirs(ruta_dest, exist_ok=True)
    callback_id = "W3e14cbc3e4b2577e854bf210e5a3c7028c7409678" 
    # Headers del navegador
    headers = {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Mobile Safari/537.36','referer': 'https://www.scoresway.com/en_GB/soccer/liga-profesional-argentina-2025/3l4bzc8syz1ea2dnv453kp89g/match/view/1xefu7uwfmhf8q24rstfoh7v8/match-summary'
    }
    
    # Fecha actual
    hoy = datetime.today().date()
    
    # Recorremos el DataFrame
    for index, row in df_partidos.iterrows():
    # for index, row in df_partidos.head(3).iterrows():
    
        # Parsear fecha del partido (corrigiendo el formato ISO con Z al final)
        fecha_partido = datetime.fromisoformat(row["date"].replace("Z", "")).date()
        # Saltar si aún no se jugó
        if fecha_partido <= hoy or not pd.notna(row['home_team']):
            # print('Partido aún no se jugo.')
            #continue
        #else:
    
            partido_id = row["match_id"]
        
            fecha = row["date"]
            home = row["home_team"]
            away = row["away_team"]
            
            # Armar nombre del archivo
            file_name = f"{fecha}_{home}_{away}_{partido_id}.json".replace(":", "-")
            file_path = os.path.join(f"{ruta_dest}", file_name)
            sdapi_outlet_key = obtener_sdapi_outlet_key(url_competicion)
            # url = row["API JSON URL"]
            url = F'https://api.performfeeds.com/soccerdata/matchevent/{sdapi_outlet_key}/{partido_id}?_rt=c&_lcl=en&_fmt=jsonp&sps=widgets&_clbk={callback_id}'
        
            print(url)
        
            # Si el archivo existe, verificamos si el partido ya está marcado como Played
            mark = 0
            if os.path.exists(file_path):
                try: 
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data_existente = json.load(f)
                        match_status = data_existente.get("liveData", {}).get("matchDetails", {}).get("matchStatus", "")
                        if match_status == "Played":
                            print(f"✅ Ya descargado y jugado: {file_name}")
                            #continue
                        else:
                            mark+=1
                            print(f"📄 Archivo existe pero sin estado 'Played': {file_name}")
                except:
                    mark+=1
                    pass
            else:
                mark+=1
            
            max_retries = 3  # Número máximo de intentos

            if mark > 0:
                for attempt in range(1, max_retries + 1):
                    try:
                        espera = random.uniform(3, 8)
                        print(f"⏳ Esperando {espera:.2f} segundos antes de descargar (intento {attempt}/{max_retries})...")
                        time.sleep(espera)
            
                        response = requests.get(url, headers=headers)
            
                        if response.status_code == 200:
                            content = response.text
                            inicio_json = content.find('(') + 1
                            final_json = content.rfind(')')
                            json_data = json.loads(content[inicio_json:final_json])
            
                            file_name = file_name.replace("_nan_nan", "")
                            with open(f'{ruta_dest}/{file_name}', 'w', encoding='utf-8') as f:
                                json.dump(json_data, f, ensure_ascii=False, indent=4)
            
                            print(f"✅ Guardado: {file_name}")
                            break  # Éxito: salimos del bucle
            
                        else:
                            print(f"❌ Error HTTP {response.status_code} en {file_name}")
            
                    except Exception as e:
                        print(f"⚠️ Falló intento {attempt} para {file_name}: {e}")
            
                    # Si no es el último intento, esperamos un poco antes de reintentar
                    if attempt < max_retries:
                        espera_extra = random.uniform(2, 5)
                        print(f"🔁 Reintentando en {espera_extra:.2f} segundos...")
                        time.sleep(espera_extra)
                else:
                    print(f"❌ No se pudo descargar {file_name} tras {max_retries} intentos.")
            
            
def get_teamstats(df_partidos,ruta_dest):
    options = Options()
    options.headless = True
    os.makedirs(ruta_dest, exist_ok=True)
    callback_id = "W3e14cbc3e4b2577e854bf210e5a3c7028c7409678" 
    # Headers del navegador
    headers = {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Mobile Safari/537.36','referer': 'https://www.scoresway.com/en_GB/soccer/liga-profesional-argentina-2025/3l4bzc8syz1ea2dnv453kp89g/match/view/1xefu7uwfmhf8q24rstfoh7v8/match-summary'
    }
    
    # Fecha actual
    hoy = datetime.today().date()
    
    # Recorremos el DataFrame
    for index, row in df_partidos.iterrows():
    # for index, row in df_partidos.head(3).iterrows():
        try:
            # Parsear fecha del partido (corrigiendo el formato ISO con Z al final)
            fecha_partido = datetime.fromisoformat(row["date"].replace("Z", "")).date()
            # Saltar si aún no se jugó
            if fecha_partido <= hoy or not pd.notna(row['home_team']):
                # print('Partido aún no se jugo.')
                #continue
        
                partido_id = row["match_id"]
                url=row['URL']        
                file_name = f"{partido_id}_teamStats.csv".replace(":", "-")
                file_path = os.path.join(f"{ruta_dest}/output", file_name)
                #print(file_name)
                if not os.path.exists(file_path):
                    driver = webdriver.Chrome(options=options)
                    driver.get(url.replace("player-stats","match-stats"))
                            
                    
                    time.sleep(5)
                            
                            # Obtener el HTML renderizado
                    html = driver.page_source
                    soup = BeautifulSoup(html, 'html.parser')
                            
                            # Buscar el div
                    div_opta = soup.find('div', id='Opta_1')
                    
                    
                    data = []
                    
                    # Acceder a cada sección/categoría
                    sections = div_opta.select('ul.Opta-TabbedContent > li')
                    
                    for section in sections:
                        category = section.find('h3').get_text(strip=True)
                        rows = section.select('table.Opta-Stats-Bars tbody tr')
                        
                        # Cada métrica tiene dos filas: [nombre, datos]
                        for i in range(0, len(rows), 2):
                            try:
                                metric = rows[i].get_text(strip=True)
                                values = rows[i + 1].select('td')
                                home = values[0].get_text(strip=True)
                                away = values[2].get_text(strip=True)
                                
                                data.append({
                                    'category': category,
                                    'metric': metric,
                                    'home': home,
                                    'away': away
                                })
                            except Exception as e:
                                # Ignora pares incompletos
                                continue
                    
                    df_raw = pd.DataFrame(data)
                    driver.close()
                    # Pivotamos a formato deseado
                    df_pivot = df_raw.pivot_table(index='metric', values=['home', 'away'], aggfunc='first').T
                    df_pivot.index.name = 'field'
                    
                    df_pivot.reset_index(inplace=True)
                    for i in df_pivot.columns:
                        if "field" not in i:
                            if "%" in df_pivot[i].values[0]:
                                df_pivot[i]=df_pivot[i].str.replace("%","")
                                df_pivot[i]=pd.to_numeric(df_pivot[i])
                                df_pivot[i]=df_pivot[i]/100
                            else:
                                df_pivot[i]=pd.to_numeric(df_pivot[i])
                    df_pivot['matchId'] = partido_id
                    df_pivot.to_csv(file_path,index=False,decimal=",",sep=';')
                else:
                    df_pivot=pd.read_csv(file_path,decimal=",",sep=';')
                    #return df_pivot
        except Exception as e:
            print("Error {}".format(e))
    return df_pivot
        
            
def load_fixtures(con,ruta_df="config/metadata.xlsx",origen='scoresway'):
    metadata = pd.read_excel(ruta_df)
    metadata = metadata[metadata.origen==origen]
    local = pd.read_sql("""select distinct home_id,home_name from sw_match_data""",con)
    visitante = pd.read_sql("""select distinct away_id,away_name from sw_match_data""",con)
    data = pd.DataFrame()
    for i,j in metadata.iterrows():
        print(j['competicion_id'])
        print(j['season'])
        url_competicion = metadata[(metadata.season==j['season']) & (metadata.competicion_id == j['competicion_id'])].url.values[0]
        partes = url_competicion.split("/")
        torneo_name = partes[5]  # liga-profesional-argentina-2025
        torneo_name = metadata[metadata.url==url_competicion].competicion_id.values[0] + "_" + metadata[metadata.url==url_competicion].season.values[0]
        torneo_id = partes[6]    # 3l4bzc8syz1ea2dnv453kp89g
        match_url = re.search(r'soccer/([^/]+)/([^/]+)/fixtures', url_competicion)
        
        
        
        competicion = match_url.group(1)
        torneo_id = match_url.group(2)
        
        datos_extra=[]
        for fi in ["fixtures","results"]:
            try:
                with open("fixtures/extra_{}_{}_{}.txt".format(torneo_id, 
                                                            j['season'],
                                                            fi), "r", encoding="utf-8") as f:
                    extra = f.read()
                extras = extra.split("data-match=")[1:]
                
                for e in extras:
                    partido_id=e.split(" ")[0].replace('"','').strip()
                    m = re.search(r'data-date="(\d+)"', e)
                    fecha = None
                    if m:
                        ts_ms = int(m.group(1))
                        fecha = datetime.utcfromtimestamp(ts_ms/1000).strftime("%Y-%m-%d")
                        dt = pd.to_datetime(ts_ms, unit="ms", utc=True)
                        time  = dt.strftime("%H:%M:%S")   # HH:MM:SS
                    # equipo local (primer <td class="Opta-Team Opta-TeamName Opta-Home ..."> ... </a>)
                    ht =  e.split("Opta-Team Opta-TeamName Opta-Home Opta-Team-")[1].split(" ")[0].strip()
                    at =  e.split("Opta-Team Opta-Away Opta-TeamName Opta-Team-")[1].split(" ")[0].strip()
                    url = (f"https://www.scoresway.com/en_GB/soccer/{competicion}/{torneo_id}/match/view/{partido_id}/player-stats")
                    datos_extra.append({
                        'competition':j['competicion_id'],
                        'season':j['season'],
                        'home_team':ht,
                        'away_team':at,
                        'date':fecha,
                        'time':time,
                        'match_id': partido_id,
                        'URL': url
                    })
            except:
                print("No existen datos extra: {}".format(fi))
        if len(datos_extra)==0:
            print("Leyendo fichero excel de FIXTURES")
            try:
                datos_re = pd.read_excel("fixtures/matches_{}_{}.xlsx".format(j['competicion_id'],j['season']))
                datos_re = pd.merge(datos_re,local,left_on="home_team",right_on="home_name",how='left')
                datos_re = pd.merge(datos_re,visitante,left_on="away_team",right_on="away_name",how='left')
                for k,s in datos_re.iterrows():
                    fecha = s['date'].replace("Z","")
                    time  =None
                    ht=s["home_id"]
                    at = s["away_id"]
                    partido_id = s["match_id"]
                    url=s['URL']
                    datos_extra.append({
                    'competition':j['competicion_id'],
                    'season':j['season'],
                    'home_team':ht,
                    'away_team':at,
                    'date':fecha,
                    'time':time,
                    'match_id': partido_id,
                    'URL': url
                })
            except:
                print("No existe fichero de FIXTURES")

        data = pd.concat([data,pd.DataFrame(datos_extra)])
        data = data.drop_duplicates(subset="match_id",keep='first')
        if data.shape[0]>0:
            data.to_sql(name='dim_fixture', con=con, if_exists='replace', index=False)
            
def get_datos_partido(partido_id,fecha,home,away,token):
            callback_id = "W3e14cbc3e4b2577e854bf210e5a3c7028c7409678" 
            # Headers del navegador
            headers = {
                'Accept': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Mobile Safari/537.36','referer': 'https://www.scoresway.com/en_GB/soccer/liga-profesional-argentina-2025/3l4bzc8syz1ea2dnv453kp89g/match/view/1xefu7uwfmhf8q24rstfoh7v8/match-summary'
            }
            # Armar nombre del archivo
            file_name = f"{fecha}_{home}_{away}_{partido_id}.json".replace(":", "-")
            sdapi_outlet_key = token
            # url = row["API JSON URL"]
            url = F'https://api.performfeeds.com/soccerdata/matchevent/{sdapi_outlet_key}/{partido_id}?_rt=c&_lcl=en&_fmt=jsonp&sps=widgets&_clbk={callback_id}'
        
            print(url)
        

            
            max_retries = 3  # Número máximo de intentos

            for attempt in range(1, max_retries + 1):
                    try:
                        espera = random.uniform(3, 8)
                        print(f"⏳ Esperando {espera:.2f} segundos antes de descargar (intento {attempt}/{max_retries})...")
                        time.sleep(espera)
            
                        response = requests.get(url, headers=headers)
            
                        if response.status_code == 200:
                            content = response.text
                            inicio_json = content.find('(') + 1
                            final_json = content.rfind(')')
                            json_data = json.loads(content[inicio_json:final_json])
            
                            file_name = file_name.replace("_nan_nan", "")
                            #with open(f'{ruta_dest}/{file_name}', 'w', encoding='utf-8') as f:
                            #    json.dump(json_data, f, ensure_ascii=False, indent=4)
            
                            print(f"✅ Guardado: {file_name}")
                            break  # Éxito: salimos del bucle
            
                        else:
                            print(f"❌ Error HTTP {response.status_code} en {file_name}")
            
                    except Exception as e:
                        print(f"⚠️ Falló intento {attempt} para {file_name}: {e}")
            
                    # Si no es el último intento, esperamos un poco antes de reintentar
                    if attempt < max_retries:
                        espera_extra = random.uniform(2, 5)
                        print(f"🔁 Reintentando en {espera_extra:.2f} segundos...")
                        time.sleep(espera_extra)
                
            return json_data
        