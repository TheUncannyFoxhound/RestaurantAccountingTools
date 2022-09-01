#Modify dbase3 file to recast null date fields as a default date and reimport back into dbase3 file
import collections
import datetime
from typing import OrderedDict
import dbf as dbf1
from simpledbf import Dbf5
from dbfread import DBF, FieldParser
import pandas as pd
import numpy as np

#Default date to overwrite NaN values
blank_date = datetime.date(1900, 1, 1)

#Read in dbase file from Old Path and point to new Path
old_path = r"C:\ALTDBF\EMPFILE.DBF"
new_path = r"C:\ICIT\BackupALTDBF\EMPFILE.DBF"

#Establish 1st rule for resolving corrupted dates
class MyFieldParser(FieldParser):
    def parse(self, field, data):
        try:
            return FieldParser.parse(self, field, data)
        except ValueError:
            return blank_date

#Collect the original EMPFILE.DBF data while stepping over any errors
table = DBF(old_path, None, True, False, MyFieldParser, collections.OrderedDict, False, False, False,'ignore')

#Grab the Header Name, Old School Variable Format, and number of characters/length for each variable
dbfh = Dbf5(old_path, codec='utf-8')
headers = dbfh.fields
hdct = {x[0]: x[1:] for x in headers}
hdct.pop('DeletionFlag')
keys = hdct.keys()

#Position of Type and Length relative to field name
ftype = 0
characters = 1

# Reformat and join all old school DBF Header fields in required format
fields = list()

for key in keys:
    ftemp = hdct.get(key)
    k1 = str(key)
    res1 = ftemp[ftype]
    res2 = ftemp[characters]
    if k1 == "BASE_PAY":
        fields.append(k1 + " " + res1 + "(" + str(res2) + ",2)")
    elif res1 == 'N':
        fields.append(k1 + " " + res1 + "(" + str(res2) + ",0)")
    elif res1 == 'D':
        fields.append(k1 + " " + res1)
    elif res1 == 'L':
        fields.append(k1 + " " + res1)
    else: 
        fields.append(k1 + " " + res1 + "(" + str(res2) + ")")


addfields = '; '.join(str(f) for f in fields)

#load the records of the EMPFILE.dbf into a dataframe
df = pd.DataFrame(iter(table))

#go ham reformatting date fields to ensure they are in the correct format
df['DATE_HIRED'] = df['DATE_HIRED'].replace(np.nan, blank_date)
df['DATE_LEFT'] = df['DATE_LEFT'].replace(np.nan, blank_date)
df['DATE_BIRTH'] = df['DATE_BIRTH'].replace(np.nan, blank_date)
df['LAST_RAISE'] = df['LAST_RAISE'].replace(np.nan, blank_date)

df['DATE_HIRED'] = pd.to_datetime(df['DATE_HIRED'])
df['DATE_LEFT'] = pd.to_datetime(df['DATE_LEFT'])
df['DATE_BIRTH'] = pd.to_datetime(df['DATE_BIRTH'])
df['LAST_RAISE'] = pd.to_datetime(df['LAST_RAISE'])

# eliminate further errors in the dataframe
df = df.fillna('0')

#drop added "record index" field from dataframe
df.set_index('EMP_NUMBER', inplace=False)


#initialize defaulttdict and convert the dataframe into a .DBF appendable format
dd = collections.defaultdict(list)
records = df.to_dict('records',into=dd)

#create the new EMPFILE.DBF file
new_table = dbf1.Table(new_path, addfields)

#append the dataframe to the new EMPFILE.DBF file
new_table.open(mode=dbf1.READ_WRITE)

for record in records:
    new_table.append(record)

new_table.close()

