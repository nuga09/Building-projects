# -*- coding: utf-8 -*-
"""
Created on Tue Dec 28 16:02:56 2021

@author: nugad
"""

#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Implements mondas/maskedtime data based routines for checking meta data

:copyright: Fraunhofer ISE - 2020 
"""
from io import BytesIO
from maskedtimedata.utils.metadata import MetaDictList, MetaDefinition
from tqdm import tqdm
import csv
import glob
import json
import logging
import os
import pandas as pd
import yaml


def load_meta(inpath, mdef, encoding="auto"):
    mlist = MetaDictList(meta_definition=mdef)

#     with open(inpath, "r", encoding="latin1") as f:
#         print(f.readline())
#         f.seek(0)
#         f = BytesIO(f.read().encode("utf-8"))

    if not encoding:
        import chardet
        with open(inpath, "rb") as f:
            rawdata = f.read()
        enc = chardet.detect(rawdata)
        encoding = enc["encoding"]
        print("Detected encoding:", encoding)

    mlist.read_csv(inpath, delimiter=";", translation_key="bpo" , encoding=encoding)
    return mlist


def load_meta_definition(inpath):
    with open(inpath, encoding="utf8") as f:
        mdef_dict = yaml.load(f, yaml.SafeLoader)
        if isinstance(mdef_dict, list):
            mdef_dict = {"metadata":mdef_dict,
                         "categories": []}
        mdef = MetaDefinition.from_dict(mdef_dict)

    return mdef


def check_meta(meta_path, meta_definition_path, raise_on_error=True):
    mdef = load_meta_definition(meta_definition_path)
    mlist = load_meta(meta_path, mdef=mdef)
    mlist.to_dict(no_classinfo=True)
    
    results = []
    
    for mdict in mlist:
        try:
            mlist.meta_definition.check_values(mdict)
        except ValueError as e:
            if raise_on_error:
                raise
            else:
                er_info = str(e)
                results.append(dict(error=er_info, **mdict))
        else:
            er_info = None
    
    return results
    

def check_all_meta(specpat="../Buildings/*/spec.yaml", meta_definition="data/mdef_2.yaml.yaml", outpath="../Analyses/meta_errors.json", raise_on_error=False):
    
    out = {}
    spec_progress = tqdm(glob.glob(specpat))
    for spec_path in spec_progress:
        building_dir = os.path.dirname(spec_path)
        spec_progress.set_description(desc=f"Checking spec of: {building_dir}")
        try:
            with open(spec_path) as f:
                spec = yaml.load(f)
                
            rel_meta_path = spec.get("meta", {}).get("path", "")
            meta_path = os.path.join(building_dir, rel_meta_path)
            print(meta_path)
            if rel_meta_path and os.path.isfile(meta_path):
                res = check_meta(meta_path, meta_definition, raise_on_error=raise_on_error)
                if res:
                    out[os.path.basename(building_dir)] = res
            else:
                logging.error("No path to meta data file defined in '%s'", spec_path)
        except:
            logging.exception("Got exception while processing building dir: '%s'", building_dir)
    
    if out:
        print(f"Errors in meta data files found --> see '{outpath}'")
    with open(outpath, "w") as f:
        json.dump(out, f, indent=4)
    
    df = pd.DataFrame(sum(([dict(site=k, **r) for r in results] for k,results in out.items()), []))
    df.to_csv(outpath.replace(".json", ".csv"), sep=";")

def convert_meta(meta_path, mdef_path, outpath=None, check=True, delimiter=";", lineterminator="\n"):
    outpath = outpath or os.path.splitext(meta_path)[0] + "_conv.csv"

    if check:
        check_meta(meta_path, mdef_path)

    mdef = load_meta_definition(mdef_path)
    mlist = load_meta(inpath=meta_path, mdef=mdef)

    with open(outpath, "w", encoding="utf-8") as f:
        vals = list(mdict for mdict in mlist)
        meta_labels = vals[0]._values.keys()

        csvwriter = csv.writer(f, delimiter=str(delimiter), quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator=lineterminator)

        # first line is commented with '#',  it's the headers
        f.write("#")
        csvwriter.writerow(['id'] + list(meta_labels))

        for v in vals:
            name = v.name
            values = v._values.values()

            row = [name] + list(values)
            csvwriter.writerow(row)

    print(f"Written converted meta file to: {outpath}")