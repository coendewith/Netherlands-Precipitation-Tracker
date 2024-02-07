import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import pandas as pd
import requests
import re
import itertools
import time
from progressbar import ProgressBar

secrets_data = {}

links = [
'https://www.meteoblue.com/en/weather/week/rotterdam_netherlands_2747891',
'https://www.meteoblue.com/en/weather/week/the-hague_netherlands_2747373',
'https://www.meteoblue.com/en/weather/week/utrecht_netherlands_2745912',
'https://www.meteoblue.com/en/weather/week/groningen_netherlands_2755251',
'https://www.meteoblue.com/en/weather/week/almere-stad_netherlands_2759879',
'https://www.meteoblue.com/en/weather/week/breda_netherlands_2758401',
'https://www.meteoblue.com/en/weather/week/delft_netherlands_2757345',
"https://www.meteoblue.com/en/weather/week/%27s-hertogenbosch_netherlands_2747351",
'https://www.meteoblue.com/en/weather/week/eindhoven_netherlands_2756253',
'https://www.meteoblue.com/en/weather/week/enschede_netherlands_2756071',
'https://www.meteoblue.com/en/weather/week/haarlem_netherlands_2755003',
'https://www.meteoblue.com/en/weather/week/leiden_netherlands_2751773',
'https://www.meteoblue.com/en/weather/week/nijmegen_netherlands_2750053',
'https://www.meteoblue.com/en/weather/week/tilburg_netherlands_2746301',
'https://www.meteoblue.com/en/weather/week/zwolle_netherlands_2743477',
'https://www.meteoblue.com/en/weather/week/zaandam_netherlands_2744118',
'https://www.meteoblue.com/en/weather/week/zoetermeer_netherlands_2743856',
'https://www.meteoblue.com/en/weather/week/arnhem_netherlands_2759661',
'https://www.meteoblue.com/en/weather/week/apeldoorn_netherlands_2759706',
'https://www.meteoblue.com/en/weather/week/alkmaar_netherlands_2759899',
'https://www.meteoblue.com/en/weather/week/dordrecht_netherlands_2756669',
'https://www.meteoblue.com/en/weather/week/amersfoort_netherlands_2759821',
'https://www.meteoblue.com/en/weather/week/hilversum_netherlands_2754064',
'https://www.meteoblue.com/en/weather/week/alphen-aan-den-rijn_netherlands_2759875',
'https://www.meteoblue.com/en/weather/week/den-helder_netherlands_2757220',
'https://www.meteoblue.com/en/weather/week/gouda_netherlands_2755420',
'https://www.meteoblue.com/en/weather/week/leeuwarden_netherlands_2751792',
'https://www.meteoblue.com/en/weather/week/nieuwegein_netherlands_2750325',
'https://www.meteoblue.com/en/weather/week/almelo_netherlands_2759887',
'https://www.meteoblue.com/en/weather/week/heerlen_netherlands_2754652',
'https://www.meteoblue.com/en/weather/week/kerkrade_netherlands_2752923',
'https://www.meteoblue.com/en/weather/week/veenendaal_netherlands_2745774',
'https://www.meteoblue.com/en/weather/week/ede_netherlands_2756429',
'https://www.meteoblue.com/en/weather/week/spijkenisse_netherlands_2746932',
'https://www.meteoblue.com/en/weather/week/maastricht_netherlands_2751283',
'https://www.meteoblue.com/en/weather/week/sittard_netherlands_2747203',
'https://www.meteoblue.com/en/weather/week/geleen_netherlands_2755616',
'https://www.meteoblue.com/en/weather/week/naarden_netherlands_2750521',
'https://www.meteoblue.com/en/weather/week/emmen_netherlands_2756136',
'https://www.meteoblue.com/en/weather/week/venlo_netherlands_2745641',
'https://www.meteoblue.com/en/weather/week/purmerend_netherlands_2748413',
'https://www.meteoblue.com/en/weather/week/heerhugowaard_netherlands_2754659',
'https://www.meteoblue.com/en/weather/week/hoorn_netherlands_2753638',
'https://www.meteoblue.com/en/weather/week/helmond_netherlands_2754447',
'https://www.meteoblue.com/en/weather/week/hengelo_netherlands_2754394',
'https://www.meteoblue.com/en/weather/week/roosendaal_netherlands_2747930',
'https://www.meteoblue.com/en/weather/week/assen_netherlands_2759633',
'https://www.meteoblue.com/en/weather/week/roermond_netherlands_2748000',
'https://www.meteoblue.com/en/weather/week/doetinchem_netherlands_2756767',
'https://www.meteoblue.com/en/weather/week/drachten_netherlands_2756644',
'https://www.meteoblue.com/en/weather/week/middelburg_netherlands_2750896',
'https://www.meteoblue.com/en/weather/week/harderwijk_netherlands_2754848',
'https://www.meteoblue.com/en/weather/week/bergen-op-zoom_netherlands_2759145',
'https://www.meteoblue.com/en/weather/week/hoofddorp_netherlands_2753801',
'https://www.meteoblue.com/en/weather/week/hardenberg_netherlands_2754861',
'https://www.meteoblue.com/en/weather/week/hoogeveen_netherlands_2753719',
'https://www.meteoblue.com/en/weather/week/oosterhout_netherlands_2749450',
'https://www.meteoblue.com/en/weather/week/kampen_netherlands_2753106',
'https://www.meteoblue.com/en/weather/week/woerden_netherlands_2744248',
'https://www.meteoblue.com/en/weather/week/ridderkerk_netherlands_2748172',
'https://www.meteoblue.com/en/weather/week/terneuzen_netherlands_2746420',
'https://www.meteoblue.com/en/weather/week/heerenveen_netherlands_2754669',
'https://www.meteoblue.com/en/weather/week/weert_netherlands_2744911',
'https://www.meteoblue.com/en/weather/week/schagen_netherlands_2747720',
'https://www.meteoblue.com/en/weather/week/hoofddorp_netherlands_2753801',
'https://www.meteoblue.com/en/weather/week/beverwijk_netherlands_2758998',
'https://www.meteoblue.com/en/weather/week/helmond_netherlands_2754447',
'https://www.meteoblue.com/en/weather/week/lelystad_netherlands_2751738',
'https://www.meteoblue.com/en/weather/week/goes_netherlands_2755476',
'https://www.meteoblue.com/en/weather/week/deventer_netherlands_2756987',
'https://www.meteoblue.com/en/weather/week/barendrecht_netherlands_2759426',
'https://www.meteoblue.com/en/weather/week/zutphen_netherlands_2743608'

]  

cities = [
'Amsterdam',
'Rotterdam',
'The Hague',
'Utrecht',
'Groningen',
'Almere',
'Breda',
'Delft',
'Den Bosch',
'Eindhoven',
'Enschede',
'Haarlem',
'Leiden',
'Nijmegen',
'Tilburg',
'Zwolle',
'Zaandam',
'Zoetermeer',
'Arnhem',
'Apeldoorn',
'Alkmaar',
'Dordrecht',
'Amersfoort',
'Hilversum',
'Alphen aan den Rijn',
'Den Helder',
'Gouda',
'Leeuwarden',
'Nieuwegein',
'Almelo',
'Heerlen', 
'Kerkrade',
'Veenendaal',
'Ede',
'Spijkenisse',
'Maastricht',
'Sittard',
'Geleen',
'Bussum Naarden',
'Emmen',
'Venlo',
'Purmerend',
'Heerhugowaard',
'Hoorn',
'Helmond',
'Hengelo',
'Roosendaal',
'Assen',
'Roermond',
'Doetinchem',
'Drachten',
'Middelburg',
'Harderwijk',
'Bergen op Zoom',
'Hoofdorp',
'Hardenberg',
'Hoogeveen',
'Oosterhout',
'Kampen',
'Woerden',
'Ridderkerk',
'Terneuzen',
'Heerenveen',
'Weert',
'Schagen',
'Hoofddorp',
'Beverwijk',
'Helmond',
'Lelystad',
'Goes',
'Deventer',
'Barendrecht',
'Zutphen'
]

pbar = ProgressBar()
time_of_day = []
precipitation_mm = []
precipitation_chance = []
time_of_day_single = []
for i in range(1,8):
    page = requests.get('https://www.meteoblue.com/en/weather/week/amsterdam_netherlands_2759794'+ '?day=' + str(i))
    soup = BeautifulSoup(page.content, 'html.parser')
    for item in soup.find_all('p',{'class':'precip-help'}):
        precipitation_chance.append(item.strong.get_text())    
    for item in soup.find_all('p',{'class':'precip-help'}): 
        time_of_day.append(re.findall('\d{2}:\d{2}\sto\s\d{2}:\d{2}',str(item)))
    for item in soup.find_all('p',{'class':'precip-help'}): 
        precipitation_mm.append(re.findall('[\d]+\.[\d]+\smm|[\d]+\smm',str(item)))

time_of_day = list(itertools.chain(*time_of_day))
time_of_day_single = [int(item[0:2]) for item in time_of_day]
precipitation_mm = list(itertools.chain(*precipitation_mm))
precipitation_mm_final = []
for item in precipitation_mm:
    precipitation_mm_final.append(item.split('\u2009mm')[0])
precipitation_chance_final = []
for item in precipitation_chance:
    precipitation_chance_final.append(int(item.split('%')[0])/100)
df = pd.DataFrame([precipitation_mm_final], columns = time_of_day_single)
df2 = pd.DataFrame([precipitation_chance_final], columns = time_of_day_single)


for link in pbar(links):
    time_of_day = []
    precipitation_mm = []
    precipitation_chance = []
    for i in range(1,8):
        page = requests.get(link + '?day=' +  str(i))
        soup = BeautifulSoup(page.content, 'html.parser')
        for item in soup.find_all('p',{'class':'precip-help'}):
            precipitation_chance.append(item.strong.get_text())    
        for item in soup.find_all('p',{'class':'precip-help'}): 
            time_of_day.append(re.findall('\d{2}:\d{2}\sto\s\d{2}:\d{2}',str(item)))
        for item in soup.find_all('p',{'class':'precip-help'}): 
            precipitation_mm.append(re.findall('[\d]+\.[\d]+\smm|[\d]+\smm',str(item)))
    time_of_day = list(itertools.chain(*time_of_day))
    time_of_day_single = [int(item[0:2]) for item in time_of_day]
    precipitation_mm = list(itertools.chain(*precipitation_mm))
    precipitation_mm_final = []
    for item in precipitation_mm:
        precipitation_mm_final.append(item.split('\u2009mm')[0])
    precipitation_chance_final = []
    for item in precipitation_chance:
        precipitation_chance_final.append(int(item.split('%')[0])/100)
    to_append = precipitation_mm_final
    df_length = len(df)
    df.loc[df_length] = to_append
    to_append2 = precipitation_chance_final
    df_length = len(df)
    df2.loc[df_length] = to_append2

df['City'] = cities
df = df.set_index('City')
df.to_csv('data.csv')
df = df.apply(pd.to_numeric)
df2 = df2.apply(pd.to_numeric)

scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

credentials = ServiceAccountCredentials.from_json_keyfile_dict(secrets_data, scope)
client = gspread.authorize(credentials)
credentials = ServiceAccountCredentials.from_json_keyfile_dict(secrets_data, scope)
spreadsheet = client.open('Weather Dashboard')
worksheet = spreadsheet.get_worksheet(0)
worksheet.update('B2',[df.columns.values.tolist()] + df.values.tolist())
worksheet.update('B76',[df2.columns.values.tolist()] + df2.values.tolist())