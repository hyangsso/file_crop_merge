from gzip import BadGzipFile
from pickle import FALSE
import re
from tabnanny import check
import vitaldb
import pandas as pd
import os
import datetime
import time
from shutil import move
import parmap
import time
import copy
import parmap
import multiprocessing
import numpy as np 
from collections import Counter

_BEGIN_AT = datetime.datetime.now()

ONLY_MERGE = False

global datalist
datalist = []

# save path
savepath = ''

# raw path
global rawpath
rawpath = ''
os.chdir(rawpath)

def replaceVitalfile():
    for dirpath, dirname, files in os.walk(savepath):
        for filename in files:
            if filename.endswith('tmp'):
                path = os.path.join(dirpath,filename)
                move(path, path.replace('.tmp',''))

# count new file 
def countVitalfile():
    filelist = []
    savelist = sum([files for _, _, files in os.walk(savepath)],[])
    savelist = [file for file in savelist if file.endswith('vital')]
    mergelist = [file[:-17] for file in savelist if 'merge' in file]

    # 파일명을 기준으로 변경할 파일만 추출
    for path, dir, files in os.walk(rawpath):
        for file in files:
            if os.path.splitext(file)[-1] != '.vital':
                continue
            
            if file not in savelist and file[:-10] not in mergelist :
                filelist.append(os.path.join(path, file))

    return filelist, savelist

# 1시간 이내 파일인지 아닌지 확인
def checkVitalfile(file):
    for filepath in file:
        filename = filepath.split('\\')[-1]
        try:
            vf = vitaldb.VitalFile(filepath)
        except:
            continue
        format = '%Y/%m/%d %H:%M:%S'
        startTime = datetime.datetime.strptime(time.strftime(format, time.localtime(vf.dtstart)),format)
        startDateHour = startTime.replace(minute=0, second=0)
        endTime = datetime.datetime.strptime(time.strftime(format, time.localtime(vf.dtend)),format)
        endDateHour = endTime.replace(minute=0, second=0)
        if endDateHour - startDateHour != datetime.timedelta(0):
            cropVitalfile(filename, filepath, vf, startTime, startDateHour, endDateHour)
        else:
            newpath = savepath + filepath.strip(filename).split('/',2)[2]
            os.makedirs(newpath, exist_ok=True)
            try:
                vf.to_vital(newpath+filename+'.tmp')
            except KeyboardInterrupt:
                quit()
            except BadGzipFile:
                print(file)
                pass
            else:
                move(newpath+filename+'.tmp', newpath+filename)

# 1시간이상인 파일들은 정각 단위를 기준으로 crop
def cropVitalfile(file, filepath, vf, dtstart, startDateHour, endDateHour):
    global savepath
    print(f'trimming {file}', end='...', flush=True)
    timeCount = ((endDateHour - startDateHour)/3600).seconds
    for hour in range(timeCount+1):
        newvf = copy.deepcopy(vf)
        if hour == 0 :
            newdtend = time.mktime(dtstart.replace(minute=59, second=59).timetuple())
            newvf.crop(dtend=newdtend)
            filename = file

        elif 0 < hour and hour < timeCount:
            newdtstart = newdtend + 1
            newdtend = newdtend + 3600
            oldDatetime = file.split('/')[-1].split('_',2)[2].split('.')[0]
            newDate = str(re.sub(r'[^0-9]','',time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(newdtstart))))
            newDatetime = newDate[2:8]+'_'+newDate[8:]
            filename = file.replace(oldDatetime, newDatetime)
            newvf.crop(newdtstart, newdtend)
        
        elif hour == timeCount :
            newdtstart = newdtend + 1
            oldDatetime = file.split('/')[-1].split('_',2)[2].split('.')[0]
            newDate = str(re.sub(r'[^0-9]','',time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(newdtstart))))
            newDatetime = newDate[2:8]+'_'+newDate[8:]
            filename = file.replace(oldDatetime, newDatetime)
            newvf.crop(dtfrom=newdtstart)

        newpath = savepath + '\\'.join(filepath.split('/',2)[2].split('\\')[:-1]) + '\\'
        if hour > 0 :
            newpath = newpath.replace('20'+oldDatetime.split('_')[0][:4],newDate[:6])
            newpath = newpath.replace(oldDatetime.split('_')[0],newDate[2:8])
            
        os.makedirs(newpath, exist_ok=True)

        try:
            newvf.to_vital(os.path.join(newpath,filename)+'.tmp') 
        except KeyboardInterrupt:
            quit()
        except BadGzipFile:
            print('error file: {file}')
            pass
        else:
            move(os.path.join(newpath,filename)+'.tmp', os.path.join(newpath,filename))
    print('done')

# 1시간이내 파일이 여러개인지 확인
def findVitalfile(name, path):
    for dirpath, dirname, filename in os.walk(savepath):
        if name in filename:
            return str(os.path.join(dirpath, name))

# 1시간이내 파일이 여러개인 경우 merge
def mergeVitalfile():
    replaceVitalfile()
    savelist = sum([files for _, _, files in os.walk(savepath)],[])
    # mergelist = [file for file in savelist if not "merge" in file ] 
    cutlist = [file[:file.find(file.split('_')[3])+2] for file in savelist if file.endswith('vital')]
    count = 0
    for key, value in Counter(cutlist).items():
        filelist = []
        if value >= 2:
            matching = [s for s in savelist if key in s]
            for file in matching:
                filepath = findVitalfile(file, savepath)
                filelist.append(filepath)
            print(f'merging {key}', end='...', flush=True)

            filename = filelist[0].replace('.vital','_merged.vital')
            # name = filename.split('\\')[-1]
            try:
                print('validating '+filename.split("\\")[-1], end='...', flush=True)
                vf = vitaldb.VitalFile(filelist)
                vf.to_vital(filename+'.tmp')
                for filepath in filelist:
                    os.remove(filepath)
            except KeyboardInterrupt:
                quit()
            else:
                move(filename+'.tmp', filename)
                count += 1
            print('done')

    finalist = sum([files for _, _, files in os.walk(savepath)],[])
    
    return finalist, count 

def onlyMergeVitalfile():
    print(f'# only merging', end='...', flush=True)
    finalist, count = mergeVitalfile()
    print('done')
    print(f'# merging file counts: {count}')
    _END_AT = datetime.datetime.now()
    print("# begin:", _BEGIN_AT)
    print("# end:", _END_AT)
    quit()

if __name__ == '__main__':
    num_cores=multiprocessing.cpu_count()-1
    replaceVitalfile()
    fileList, savelist = countVitalfile()
    if ONLY_MERGE : 
        onlyMergeVitalfile()
    print(f'# scanning {len(fileList)} vitalfiles')
    splited_data =  np.array_split(fileList, num_cores)
    splited_data = [x.tolist() for x in splited_data]   
    result = parmap.map(checkVitalfile, splited_data, pm_pbar=True, pm_processes=num_cores)
    finalist, count = mergeVitalfile()

    _END_AT = datetime.datetime.now()
    print("# begin:", _BEGIN_AT)
    print("# end:", _END_AT)
    print(f'# original file counts: {len(fileList)}\n# result file counts: {len(set(finalist)-set(savelist))}')
