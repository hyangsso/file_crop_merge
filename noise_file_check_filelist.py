import pandas as pd
import os
import vitaldb
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

path = ''
df = pd.read_excel(path)

path = ''

result = pd.DataFrame()
count = 0
for path, dir, files in os.walk(path):
    for file in files:
        if not file.split('.')[1] == 'vital':
            continue
        
        if file in df.filename.to_list():
    
            filepath = os.path.join(path, file)
            filesize = os.path.getsize(filepath)
            
            vf = vitaldb.VitalFile(filepath, track_names_only=True)
            if ('Intellivue/PLETH_HR' in vf.get_track_names()) or ('Intellivue/PLETH_SAT_O2' in vf.get_track_names()) or ('Solar8000/HR' in vf.get_track_names()) or ('Solar8000/PLETH_HR' in vf.get_track_names()) or ('Solar8000/PLETH_SPO2' in vf.get_track_names()) or ('Bx50/HR' in vf.get_track_names()) or ('Bx50/PLETH_HR' in vf.get_track_names()) or ('Bx50/PLETH_SPO2' in vf.get_track_names()) :
                count += 1
                result = result.append({'filename':file, 'filesize':filesize}, ignore_index=True)
                if count % 100 == 0:
                    print(count)

result.to_excel('not_track_result_220324_error.xlsx', index=False)