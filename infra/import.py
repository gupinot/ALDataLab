#!/usr/bin/env python3

# coding: utf-8
import sys
import getopt
import pandas as pd
import numpy as np
from influxdb import DataFrameClient
import re
from time import strptime

# Script globals
verbose=False
influxdbUrl='influxdb://root:root@localhost:8086/datalab'
influxdb='datalab'

def run(argv):
    global verbose,influxdbUrl,influxdb

    # Read options
    try:
        opts, args = getopt.getopt(argv,"vi:",["influxdb=","verbose"])
        if len(args)  == 0:
            raise "Missing arguments"
    except:
        print("import.py [-v|--verbose] [-i|--influxdb]=<influxdb url> <xlsUrl> ...")
        sys.exit(1)

    for opt,val in opts:
        if opt in ("-v","--verbose"):
            verbose=True
        elif opt in ("-i","--influxdb"):
            influxdbUrl=val

    client = DataFrameClient(database=influxdb)

    for file in args:
        if verbose:
            print("Processing ",file)
        try:
            base_input = pd.read_excel(file)
            base_tags=extract_tags_from_name(file)
            base_input.columns=['stream','date','value']
            input = base_input.groupby(['date','stream']).agg(np.mean).reset_index()
            input['table']=input['stream'].map(extract_table_and_tags)
            input['name']=input['stream'].map(extract_name)
            grouped = input[input['table'] != 'app'].groupby('table').apply(pivot).reset_index()
            index=pd.to_datetime(grouped['date'],format='%b %d %Y %I:%M%p')
            final=grouped.set_index(index)
            byTable = final.groupby('table')
            for key,df in byTable:
                (tablename,tags)= parse_key(key)
                tags.update(base_tags)
                nodatedf = df[df.columns - ['date','table','name']]
                client.write_points(nodatedf,tablename,tags=tags)
        except:
            print("Error encountered when processing ", file, " : ",sys.exc_info()[0])
            raise

def pivot(df):
    res = df[['date','name','value']].pivot(index='date',columns='name',values='value')
    return res

def extract_tags_from_name(url):
    basename = url.split('/')[-1]
    noext = basename[0:basename.rfind('.')]
    return {'server': noext.split('_')[0]}

def extract_table_and_tags(value):
    if value.startswith("PROCESSOR Utilization") or value.startswith("MemPhysUsage") \
        or value.startswith("MemVirtualUsage") or value.startswith("SwapUsage"):
        return "sysstat"
    elif value.startswith("Ldsk:"):
        return "iostat:"+extract_tags(value[4:])
    else:
        return "app"

def extract_name(value):
    matched = re.search(":?([^: ]+)\^\^(.*)",value)
    if matched:
        return (matched.group(1)+'_'+matched.group(2).replace('%','percent')).lower()
    else:
        return value.lower()

def extract_tags(value):
    matched = re.search("([^ ]+) ?\((.+)\):",value)
    if matched:
        return 'mount='+matched.group(1)+',device='+matched.group(2)
    else:
        return ""

def parse_key(value):
    (tablename,sep,tagString) = value.partition(':')
    tags={}
    if len(tagString) > 0:
        for tagkv in tagString.split(','):
            (tag,sep,value)=tagkv.partition('=')
            tags[tag]=value

    return [tablename,tags]

def convert_datetime(val):
    return strptime(val,'%b %d %Y %I:%M%p')

if __name__ == "__main__":
    run(sys.argv[1:])
