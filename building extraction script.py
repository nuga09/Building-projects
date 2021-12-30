# -*- coding: utf-8 -*-
"""
Created on Tue Dec 28 16:01:37 2021

@author: nugad
"""

#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""Migration script for converting legacy DataStorage databases to pandas 
compatible hdf5 files.

:copyright: Fraunhofer ISE - 2020
"""

from datastorage.utils.dbcontext import dbcontext # @UnresolvedImport
import datastorage # @UnresolvedImport
import pandas as pd
import numpy as np

from fnmatch import fnmatch
import os, glob, logging, os.path
from tqdm import tqdm

import logging
import contextlib
import warnings
from pandas.errors import PerformanceWarning
logger = logging.getLogger()

import shutil


def fix_logging():
    '''Removes handlers from datastorage because of "MSG echo"
    '''
    handlers = [h for h in logger.handlers if not "datastorage" in str(h)]
    if not handlers:
        handlers.append(logging.StreamHandler())
    logger.handlers = handlers


def mtd_to_pd(mtd_array):
    return pd.Series(mtd_array, mtd_array.timearray.astype("M8[s]"))


def get_meta(node):

    meta_dicts = {}
    for sn, s in node.items():
        mkeys = s.getMetaKeys()
        meta_dict = dict(
                        (mk, s.getMetaData(mk).decode("latin1")# .encode("utf-8")
                         )
                    for mk in mkeys)
        meta_dicts[sn] = meta_dict

    meta = pd.DataFrame.from_dict(meta_dicts, orient="index")
    return meta


def db_to_pandas_h5(dbpath="h5", proj_pat="*", node_pat="*", basedir=None,
                    chunk_seconds=3600 * 24 * 30,
                    meta_sep=";", single_sensors=False):
    '''Stores sensor data in a DataStorage database to pandas compatible hdf5 files.
    
    Through `proj_pat` and `node_pat` the projects and sensor groups can be selected.
    By default all projects and sensor groups are converted. Each project will
    result in multiple h5 files to be created. Each h5 file will contain two tables:
    
    - data: The actual data
    - meta: The corresponding meta data 
    
    Parameters
    ----------
    
    dbpath: str (optional)
        Path to the database folder (default: "h5")
    proj_pat: str (optional)
        Patter for selecting the projects to be extracted, may include '*' wildcards.
    node_pat: str (optional)
        Patter for selecting the sensor groups to be extracted, may include '*' wildcards.
    basedir: str (optional)
        Base folder where to put output files (default: same as dbpath)
    chunk_seconds: int
        Length of time in seconds slices to be extracted one at a time
    single_sensors: bool (optional)
        Do store each sensor as a key on its own. This may be faster but greatly increases the file size. (default: False)
    '''
    basedir = basedir or os.path.dirname(dbpath)
    if not os.path.exists(basedir):
        os.makedirs(basedir)

    warnings.filterwarnings("ignore", category=PerformanceWarning) # ignore pandas PerformanceWarnings. These are only fired because of the spec entry.

    with dbcontext(dbpath, close=False, reuse=True) as db:
        fix_logging()

        root = db.getnode("/")
        for proj_name, proj in root.items():# iterate projects:
            logger.info("Processing project '%s'", proj_name)
            if not fnmatch(proj_name, proj_pat):
                continue

            for node_name, node in proj.items(): # iterate sensor or filter groups:
                force_single_sensors = False
                try:
                    if not fnmatch(node_name, node_pat):
                        continue
                    if isinstance(node, datastorage.eventgroup.EventGroup) and not single_sensors:
                        logger.info("EventGroups are not supported as single DataFrame --> node '%s' stored as single sensors.", node_name)
                        force_single_sensors = True

                    outpath = os.path.join(basedir, '{proj}-{node}.pandas.h5'.format(proj=proj_name,
                                                                                    node=node_name))

                    with contextlib.closing(pd.HDFStore(outpath, mode='w')) as store: # : :type store: pd.HDFStore
                        logger.info("\tProcessing sensor group '%s'", node_name)

                        # write meta data to csv:
                        meta = get_meta(node)
                        meta_outpath = outpath.replace(".pandas.h5", "-{node_name}.meta.csv".format(node_name=node_name))
                        meta.to_csv(meta_outpath, sep=meta_sep, encoding="utf8")

                        # encode DataFrame strings before storing to hdf5:
                        cols = [c for i, c in enumerate(meta.columns) if meta.dtypes[i] == "object"]
                        meta[cols] = meta[cols].apply(lambda x: x.str.encode("utf8"))
                        store.put("meta", meta, format="table")# , encoding="utf8")

                        # add spec dict:
                        spec_dict = {"step" : node.step,
                                "single_sensors" : single_sensors or force_single_sensors}
                        if single_sensors or force_single_sensors:
                            spec_dict["sensor_paths"] = dict((s.name, "/data/" + s.name) for s in node.itervalues())
                        spec = pd.Series(spec_dict)

                        store.put("spec", spec)

                        tg = node.timegrid

                        total_start = tg.start or min(s.start for s  in node.itervalues())
                        total_stop = tg.stop or max(s.stop for s in node.itervalues())

                        if not single_sensors and not force_single_sensors:
                            step = chunk_seconds
                            n_steps = int((total_stop - total_start) // step) + 1

                            for i, start in tqdm(enumerate(np.arange(total_start, total_stop, step)),
                                                desc="\tMigrating time slices...",
                                                total=n_steps):
                                time_slice = slice(start, start + step) # replace a None with a time tuple, e.g.: (2015,1,1) to get start and stop dates
                                sensor_data = ((s.name, mtd_to_pd(s[time_slice])) for s in node.itervalues())

                                df = pd.DataFrame.from_items(sensor_data) # store data to pandas data frame for easy access
                                store.append("data", df)

                        else: # iterate sensors
#                             time_slice = slice(total_start, total_stop) #does not make any difference here.
                            time_slice = slice(None, None)
                            for s in tqdm(node.itervalues(),
                                        desc="\tMigrating sensors...",
                                        total=len(node)):
                                sensor_data = mtd_to_pd(s[time_slice])
                                store.put("/data/" + s.name, sensor_data)

                        logging.info("Stored sensor group '%s' to '%s'", node_name, outpath)
                except:
                    logger.exception("Error while processing '%s'", node_name)


def main(bp):

    bglob = os.path.join(bp, '*')
    buid = glob.glob(bglob)
    # buid = ['../../Buildings/BZR_Dusseldorf']
    for x in buid:
        dirglb = os.path.join(x, 'data/h5')
        src = glob.glob(dirglb)
        dstglb = os.path.join(x, 'data/h5_bakup')
        dstbse = os.path.join(x, 'data/pandas.h5')
        for fil in src:

            print(fil)

            try:
                shutil.copytree(fil, dstglb)
            except OSError:
                print("Backup already existing:", dstglb)
            except Exception as error:
                logging.exception("Could not backup folder")
                continue

            db_to_pandas_h5(dbpath=fil,
        #                     chunk_seconds=100 * 86400,
                        basedir=dstbse,
        #                     single_sensors=True,
        #                     node_pat="event_data",
        #                     node_pat="sampled_data",
                        )


if __name__ == '__main__':
    main('../../Buildings')
