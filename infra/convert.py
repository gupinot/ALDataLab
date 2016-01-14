#!/usr/bin/env python2.7

# coding: utf-8
import sys
import getopt
import pandas as pd
import numpy as np
import re
from datetime import datetime
import time

# Script globals
verbose=False
output='serverusage_'+str(round(time.time()))+'.json'

def build_frame(file):
    tags=extract_tags_from_name(file)

    try:
        input = pd.read_excel(file,sheetname=0)
    except:
        print('Failed to read '+file)
        input = pd.DataFrame({
            'stream':pd.Series([np.nan],dtype='object'),
            'date':pd.Series([np.nan],dtype='object'),
            'value':pd.Series([np.nan],dtype='float')
        })

    input.columns=['stream','date','value']
    for tagname in tags.keys():
        input[tagname]=input['stream'].map(lambda x: tags[tagname])
    input['table']=input['stream'].astype('str').map(extract_table_and_tags)
    input['name']=input['stream'].astype('str').map(extract_name)
    input['date']=pd.to_datetime(input['date'])
    return input

def extract_tags_from_name(url):
    basename = url.split('/')[-1]
    noext = basename[0:basename.rfind('.')]
    tags = noext.split('_')
    if len(tags) > 1:
        importdt = datetime.strptime(tags[1],'%Y%m%d-%H%M%S').isoformat()
    else:
        importdt = None
    return {'server': tags[0].lower(), 'importdt': importdt}

def extract_table_and_tags(value):
    if value.startswith("PROCESSOR Utilization") or value.startswith("MemPhysUsage") \
        or value.startswith("MemVirtualUsage") or value.startswith("SwapUsage"):
        return "sysstat"
    elif value.startswith("Ldsk:") or value.startswith("Ldisk"):
        return "iostat:"+extract_tags(value[4:])
    else:
        return "app"

def extract_name(value):
    patterns = ['^L(disk[^ ]+).*\^\^(.*)',':?([^: ]+)\^\^(.*)']
    for pattern in patterns:
        matched = re.search(pattern,value)
        if matched:
            return (matched.group(1)+'_'+matched.group(2).replace('%','percent').replace('#','count')).lower()

    return value.lower()

def extract_tags(value):
    matched = re.search("([^ ]+) ?\((.+)\):",value)
    if matched:
        return 'mount='+matched.group(1)+',device='+matched.group(2)
    else:
        matched = re.search("\(?([A-Z]:)\)?",value)
        if matched:
            return 'mount='+matched.group(1)
    return ""

def run(argv):
    global verbose,output

    # Read options
    try:
        opts, args = getopt.getopt(argv,"vo:",["verbose","output"])
        if len(args)  == 0:
            raise "Missing arguments"
    except:
        print("convert.py [-v|--verbose] -o <output.csv> <xlsUrl> ...")
        sys.exit(1)

    for opt,val in opts:
        if opt in ("-v","--verbose"):
            verbose=True
        elif opt in ["-o","--output"]:
            output=val

    if verbose:
        print("Reading frames ")

    frames = [ build_frame(file) for file in args ]
    merged = pd.concat(frames)
    merged.to_json(output,orient='records')

if __name__ == "__main__":
    run(sys.argv[1:])
