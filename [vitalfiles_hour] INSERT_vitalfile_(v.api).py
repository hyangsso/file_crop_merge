import vitaldb
import pymysql
import pandas as pd
import datetime
import os 

os.chdir('')

vitaldb.api.login('', '', '')

db = pymysql.connect(host='', user='', passwd='', db='', charset='')
cur = db.cursor()

df = pd.DataFrame(vitaldb.api.filelist(bedname='SICU', dtstart='2022-01-01'))
# df = pd.read_excel('재업로드_vitalfile.xlsx')
for idx, row in df.iterrows():
    filename = row['filename']

    cur = db.cursor()
    sql = f'SELECT moveid FROM vitalfiles_hour WHERE filename="{filename}"'
    cur.execute(sql)
    
    if cur.rowcount >= 1:
        print(f'vitalfile exist...{filename}')
        continue
    
    fileicu = filename.split('_')[0]
    filebed = filename.split('_')[1]
    
    try:
        vf = vitaldb.VitalFile(vitaldb.api.download(filename), track_names_only=True)
    except:
        print(f'vitalfile not load...{filename}')
        continue
    dtstart = str(datetime.datetime.fromtimestamp(int(vf.dtstart)))
    dtend = str(datetime.datetime.fromtimestamp(int(vf.dtend)))
    
    cur = db.cursor()
    # test: sql = f'SELECT moveid FROM bed_moves WHERE moveid IN (SELECT moveid FROM bed_moves WHERE "2021-12-01" <= bedin and bedout <= "2021-12-31" and icuid IN (SELECT icuid FROM admissions WHERE icuroom = "MICU")) and icubed = {icubed} and DATE_FORMAT(bedin, "%Y-%m-%d") <= "{date}" and "{date}" <= DATE_FORMAT(bedout, "%Y-%m-%d")  and icuid IN (SELECT icuid FROM admissions WHERE icuroom = "{fileicu}")'
    sql = f'SELECT moveid,icuid FROM bed_moves WHERE icubed = "{filebed}" and (((bedin <= "{dtstart}" and "{dtstart}" <= bedout)) or ((bedin <= "{dtend}" and "{dtend}" <= bedout))) and icuid IN (SELECT icuid FROM admissions WHERE icuroom = "{fileicu}")'
    cur.execute(sql)
    data = cur.fetchall()
    
    if len(data) >= 1:
        for row in data :
            moveid = row[0]
            icuid = row[1]
            cur = db.cursor()
            sql = f'SELECT hid, icuroom FROM admissions WHERE icuid = {icuid}'
            cur.execute(sql)
            data = cur.fetchall()
            hid = data[0][0]
            icuroom = data[0][1]
            # INSERT vitalfiles matching case
            cur = db.cursor()
            sql = f'INSERT IGNORE INTO vitalfiles_hour(filename, moveid, hid, icuroom) VALUES ("{filename}", {moveid}, {hid}, "{icuroom}")'
            cur.execute(sql)
            print(f'vitalfile upload...{filename}')

    db.commit()
db.close()