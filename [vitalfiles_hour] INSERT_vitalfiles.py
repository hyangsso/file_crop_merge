import pandas as pd
import numpy as np
import datetime
import vitaldb
import os
import pymysql
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

_BEGIN_AT = datetime.datetime.now()

# db address & connect
db = pymysql.connect(host='', user='', passwd='', db='', charset='')

# (변경) local 1day file address
path = 'F:/final_test/'

file_count = sum(len(files) for _, _, files in os.walk(path))

# 1day file - moveid matching
idlist = []
count = 0
dfmtch = pd.DataFrame()
dfnotmtch = pd.DataFrame()
dfnotread = pd.DataFrame()
for path, dir, files in os.walk(path):
    for file in files:
        if not file.split('.')[1] == 'vital':
            continue

        count += 1
        
        try:
            icuroom = file.split('_')[0]
            icubed = file.split('_')[1].zfill(2)
        except IndexError:
            continue
        
        if count % 100 == 0:
            print('matching...('+str(count)+'/'+str(file_count)+')...'+file)
    
        cur = db.cursor()
        sql = f'SELECT moveid FROM vitalfiles_test WHERE filename = "{file}"'
        cur.execute(sql)
        if cur.rowcount > 0:
            continue
        
        try:
            vf = vitaldb.VitalFile(os.path.join(path,file), track_names_only=True)
        except:
            dfnotread = dfnotread.append({'filename':file}, ignore_index=True)
        dtstart = str(datetime.datetime.fromtimestamp(vf.dtstart)).split('.')[0]
        dtend = str(datetime.datetime.fromtimestamp(vf.dtend)).split('.')[0]
    
        # date = datetime.datetime.strptime(file.split('.')[0].split('_')[2], '%y%m%d').date()

        cur = db.cursor()
        # test: sql = f'SELECT moveid FROM bed_moves WHERE moveid IN (SELECT moveid FROM bed_moves WHERE "2021-12-01" <= bedin and bedout <= "2021-12-31" and icuid IN (SELECT icuid FROM admissions WHERE icuroom = "MICU")) and icubed = {icubed} and DATE_FORMAT(bedin, "%Y-%m-%d") <= "{date}" and "{date}" <= DATE_FORMAT(bedout, "%Y-%m-%d")  and icuid IN (SELECT icuid FROM admissions WHERE icuroom = "{icuroom}")'
        sql = f'SELECT moveid,icuid FROM bed_moves WHERE icubed = "{icubed}" and (((bedin <= "{dtstart}" and "{dtstart}" <= bedout)) or ((bedin <= "{dtend}" and "{dtend}" <= bedout))) and icuid IN (SELECT icuid FROM admissions WHERE icuroom = "{icuroom}")'
        cur.execute(sql)
        data = cur.fetchall()
        
        if len(data) == 0:
            npath = os.path.join(path, file)
            filesize = os.path.getsize(npath)
            dfnotmtch = dfnotmtch.append({'filename':file, 'filesize':filesize}, ignore_index=True)
            continue 
            
        elif len(data) >= 1:
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
                sql = f'INSERT IGNORE INTO vitalfiles_test(filename, moveid, hid, icuroom) VALUES ("{file}", {moveid}, {hid}, "{icuroom}")'
                cur.execute(sql)
                
dfnotmtch.to_excel('not_readind_0524_new.xlsx', index=False)
dfnotmtch.to_excel('not_matching_0524_new.xlsx', index=False)
db.commit()
print('업로드 완료')

def DROP_DUPLICATED():
    # duplicated case
    dfdplct = dfmtch[dfmtch.filename.isin(dfmtch[dfmtch.duplicated('filename')].filename.to_list())]
    print(f'duplicated case: {len(dfdplct)} cases')

    dfresult = dfmtch.copy()
    for idx, row in dfdplct.iterrows():
        filename = row['filename']
        moveid = row['moveid']
        
        year = '20' + filename.split('.')[0].split('_')[2][:2]
        month = filename.split('.')[0].split('_')[2][2:4]
        bed = filename[:-13]
            
        sql = f'SELECT bedin, bedout FROM bed_moves WHERE moveid = {moveid}'
        cur.execute(sql)
        data = cur.fetchone()
        
        # vitalfile dtstart, dtend
        vf = vitaldb.VitalFile(filename)
        dtstart = datetime.datetime.fromtimestamp(int(vf.dtstart))
        dtend = datetime.datetime.fromtimestamp(int(vf.dtend))
        
        # moveid bedin, bedout
        bedin = data[0]
        bedout = data[1]
        
        print(f'cropping {filename}-{moveid}...', end='', flush=True)
        # crop duplicated case
        if dtstart <= bedin and bedin <= dtend :
            vf.crop(dtfrom=bedin.timestamp())
        if dtstart <= bedout and bedout <= dtend :
            vf.crop(dtend=bedout.timestamp())
        
        # save cropped case
        print('saving...', end='', flush=True)
        new_filename = filename.split('.')[0]+'_'+str(int(moveid))+'.vital'
        vf.to_vital(new_filename)
        
        # change filename
        dfresult.loc[dfresult.index==idx, 'filename'] = new_filename
        
        print('update...', end='', flush=True)
        cur = db.cursor()
        # UPDATE vitalfiles cropped case
        # sql = f'UPDATE vitalfiles SET filename = "{new_filename}" WHERE filename = "{filename}" and moveid = {moveid}'
        cur.execute(sql)
        print('done')
        
db.commit()
db.close()

_END_AT = datetime.datetime.now()
print("begin:", _BEGIN_AT)
print("end:", _END_AT)