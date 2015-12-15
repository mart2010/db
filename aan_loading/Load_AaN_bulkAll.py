#! /usr/bin/env python
# This script populates A-a-N data model from the JSON JobReport input file.
# It relies on the PS adapter for Python : 'psycopg', which must be installed in the python distr:
# Distutils :
#   -> python setup.py install (or ->python setup.py install --user  (for 'user installation scheme')
# from the distribution root directory.  It requires 'pg_config' (in postgresql-devel, i.e. libpq-dev in Debian/Ubuntu)
#
# JobReport Format: "jobId; replicaId; uuid; outputTarFile; NumReplicas; slaveValidated;
#                   Time-> UserTime; Time-> "WallTime; Time-> SystemTime;
#                   BOINC_USERID; BOINC_USERNAME; BOINC_HOSTID;
# Files format are: "JobReport_55c38822-29ff-11e4-8a38-b8ca3aa02642_2014-05-20 13:13:10.json"
# The script takes as input a conf file :
###[Json Report]
###  rep_dir  = /dir.../
###  arch_dir = /dir../
###  file_pattern = JobReport_*.json
###[Db host]
###  dbhost     = 10.80.64.24
###  dbname     = an_rep
###  dbport     = 5432
###  dbuser     = rep
###  dbpwd      = rep
#[Email alert log]
###  recipient=martin.ouellet@epfl.ch
### However, any parameters can be overwritten through optional argument (ex. -dbhost newhost)
###
### This version first proceed with all JobReport files loading :
###    - Iterate over Json files
###         - bulk load all files
###
###    - Load all staging rows into target tables
###         - Load run table
###         - Load user table
###         - Load run table
###         - Load replica table
###         - Load user sat table
###
###    - Update stage table


import sys
import json
import cStringIO
import os
import fnmatch
import shutil
from datetime import datetime
import psycopg2
import logging
import inspect
import ConfigParser
import argparse


# Global attributes
MAX_EXP_DATE = datetime(3000,1,1)



def get_pending_files(repdir, pattern):
    if not os.path.lexists(repdir):
        raise EnvironmentError("Invalid directory defined :'" + repdir + "'")

    pendFiles = []
    for f in os.listdir(repdir):
        if fnmatch.fnmatch(f,pattern):
            pendFiles.append(f)
    return pendFiles


def move_loaded_files(fileList, repdir, archdir):
    log = logging.getLogger()
    for f in fileList:
        shutil.move(repdir + f, archdir)
        log.debug("File '%s' moved to archive", f)



def bulk_load_files(conn, repdir, pendingFiles, truncateStg):
    """
    :param pendingFiles:
    :param truncateStg:
    :return: All filenames correctly loaded
    """
    log = logging.getLogger()
    cur = conn.cursor()

    if truncateStg:
        cur.execute('truncate table stg_file')

    loadedFiles = []
    for f in pendingFiles:
        #this will raise ValueError if not found
        ji = f.index("JobReport_")+10

        runuuid = f[ji:ji+36]
        datetext = f[ji+37:ji+56]
        start_dts = datetime(int(datetext[0:4]),int(datetext[5:7]),int(datetext[8:10]), \
                            int(datetext[11:13]),int(datetext[14:16]),int(datetext[17:19]))

        jobFile = open(repdir + f,"r")
        jobs = {}
        try:
            content = json.load(jobFile)
            jobs = content.get('jobs')
            log.info("Process json file: %s", f)
        except ValueError:
            log.error("Skip invalid json file: %s", f)
            continue

        sio= cStringIO.StringIO()
        now = datetime.now()
        for j in jobs:
            jid, repno, repuuid = j.get('jobId'), j.get('replicaId'), j.get('uuid')
            slaveVal, userId, username =  j.get('slaveValidated'), j.get('BOINC_USERID'), j.get('BOINC_USERNAME')
            utime, wtime, stime = j.get('Time').get('UserTime'), j.get('Time').get('WallTime'), j.get('Time').get('SystemTime')

            sio.write(runuuid+"\t"+
                      jid+"\t"+
                      repno+"\t"+
                      repuuid+"\t"+
                      userId+"\t"+
                      username+"\t"+
                      utime+"\t"+
                      wtime+"\t"+
                      stime+"\t"+
                      str(start_dts)+"\t"+
                      str(now)+"\n" )
        sio.seek(0)

        cur.copy_from(sio, 'stg_file', sep='\t', \
                      columns=('run_uuid', 'job_no', 'replica_no', 'rep_uuid', 'boinc_userid', 'boinc_username',
                               'user_time', 'wall_time', 'system_time', 'time_start', 'load_dts'
                      ))
        loadedFiles.append(f)

        #freeing resource
        sio.close()
    cur.close()
    return loadedFiles


def load_run_table(conn):
    cur = conn.cursor()
    now = datetime.now()
    cur.execute("""
                insert
                into run_h2(run_uuid,time_start,load_dts)
                (select distinct cast(run_uuid as uuid)
                        ,time_start
                        ,%s
                from stg_file
                where process_dts is null); """, (now,))
    count = cur.rowcount
    cur.close()
    return count


def load_user_table(conn):
    cur = conn.cursor()
    now = datetime.now()
    cur.execute("""
                insert into user_h2(boinc_userid, load_dts)
                (select distinct s.boinc_userid, %s
                from stg_file s
                left outer join user_h2 u on (s.boinc_userid = u.boinc_userid)
                where
                s.process_dts is null
                and u.boinc_userid is null
                ); """, (now, ))
    count = cur.rowcount

    #TODO: validate username vs userid (assumption: many username possible for same userid)
    cur.execute("""
                insert into user_s2(boinc_userid, username, valid_from, valid_to, load_dts)
                (select s.boinc_userid, max(s.boinc_username), min(s.time_start), %s, %s
                from stg_file s
                left outer join user_s2 u on (s.boinc_userid = u.boinc_userid)
                where
                s.process_dts is null
                and u.boinc_userid is null
                group by s.boinc_userid
                ); """, (MAX_EXP_DATE, now))
    cur.close()
    return count

def load_update_user_sat(conn):
    cur = conn.cursor()
    now = datetime.now()
    cur.execute("""
                with sat_rec_insert as (
                insert into user_s2(boinc_userid, username, valid_from, valid_to, load_dts)
                (select new.boinc_userid, new.username, new.valid_from, %(max_date)s, %(now)s
                from
                    (select boinc_userid, max(boinc_username) username, max(time_start) valid_from
                    from stg_file
                    where process_dts is null
                    group by boinc_userid) new
                    inner join
                    (select boinc_userid, username, valid_from
                    from user_s2
                    where valid_to = %(max_date)s) prev
                    on (new.boinc_userid = prev.boinc_userid)
                    where
                    new.valid_from > prev.valid_from
                    and new.username != prev.username --OR ...all sat attr would be added here..
                )
                returning *
                )
                update user_s2 set valid_to = sat_rec_insert.valid_from
                from sat_rec_insert
                where user_s2.boinc_userid = sat_rec_insert.boinc_userid
                and user_s2.valid_to = %(max_date)s; """, {'max_date':MAX_EXP_DATE, 'now':now} )
    count = cur.rowcount
    cur.close()
    return count



def load_replica_table(conn):
    cur = conn.cursor()
    now = datetime.now()
    cur.execute("""
                insert into replica_l2(rep_uuid, boinc_userid, run_uuid, replica_no
                                       ,slave_validated, time_start, wall_time
                                       ,system_time, user_time, host_id, day_period
                                       ,job_no, load_dts)
                (select cast(rep_uuid as uuid), boinc_userid, cast(run_uuid as uuid), replica_no
                        ,slave_validated, time_start, wall_time
                        ,system_time, user_time, host_id, cast(to_char(time_start,'YYYYMMDD') as int)
                        ,job_no, %s
                from stg_file
                where process_dts is null)""", (now,))
    count = cur.rowcount
    cur.close()
    return count


def update_process_complete(conn):
    now = datetime.now()
    cur = conn.cursor()
    cur.execute("""
                update stg_file
                set process_dts = %s
                where process_dts is null;""", (now,))
    count = cur.rowcount
    cur.close()
    return count


# Read config file and overwrite parameter if optional arguments are set
# return all param values as Dictionary
def read_params():

    #####setup conf file parameters and optional arguments
    parser = argparse.ArgumentParser()
    ##Only mandatory argument is name of config file
    parser.add_argument("conf_file")
    ##All other are optinal arguments
    parser.add_argument("-d","--dbhost")
    parser.add_argument("-dp","--dbport")
    parser.add_argument("-dn","--dbname")
    parser.add_argument("-du","--dbuser")
    parser.add_argument("-dpw","--dbpwd")
    parser.add_argument("-fd","--filedir")
    parser.add_argument("-fa","--filearch")
    parser.add_argument("-fp","--filepattern")

    args = parser.parse_args()
    ##Read config file
    conf = ConfigParser.SafeConfigParser()
    conf.read(args.conf_file)

    params = {}
    value = args.dbhost if args.dbhost != None else conf.get('Db host','dbhost')
    params["dbhost"] = value
    value = args.dbname if args.dbname != None else conf.get('Db host','dbname')
    params["dbname"] = value
    value = args.dbport if args.dbport != None else conf.get('Db host','dbport')
    params["dbport"] = value
    value = args.dbuser if args.dbuser != None else conf.get('Db host','dbuser')
    params["dbuser"] = value
    value = args.dbpwd if args.dbpwd != None else conf.get('Db host','dbpwd')
    params["dbpwd"] = value

    value = args.filedir if args.filedir != None else conf.get('Json Report','filedir')
    params["filedir"] = value
    value = args.filearch if args.filearch != None else conf.get('Json Report','filearch')
    params["filearch"] = value
    value = args.filepattern if args.filepattern != None else conf.get('Json Report','filepattern')
    params["filepattern"] = value

    return params



def main():

    #####Get config parameters
    params = read_params()

    #####set up the logger
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    # add a consol handler (stdout) and file handler
    consolHandler = logging.StreamHandler()
    fileHandler = logging.FileHandler("Log_Load_AaN.log")
    # both handler uses same formatter
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s')
    consolHandler.setFormatter(formatter)
    fileHandler.setFormatter(formatter)
    # add the two handler (can (de)activate as needed for prod)
    log.addHandler(consolHandler)
    log.addHandler(fileHandler)



    ############ Starting Loading #############
    log.info("Starting Loading Process!")

    #### setup of DB connection
    dbconn = None
    try:
        dbconn = psycopg2.connect(host=params["dbhost"],
                                database=params["dbname"],
                                port=params["dbport"],
                                user=params["dbuser"],
                                password=params["dbpwd"])
    except Exception, msg:
        log.error("DB connection error: %s", msg)
        sys.exit(-1)


    #### Run Bulk Load as isolated transaction
    pendingFiles = get_pending_files(params["filedir"], params["filepattern"])

    if len(pendingFiles) > 0:
        try:
            filestoArchive = bulk_load_files(dbconn, params["filedir"], pendingFiles, False)
            dbconn.commit()
            #at this stage all files loaded, safe to archive
            move_loaded_files(filestoArchive, params["filedir"], params["filearch"])
            log.info(">>Bulk loaded %s Json files sucessfully", str(len(filestoArchive)))
        except Exception, msg:
            log.error("Bulk load error : %s", msg)
    else:
        pass
        #exit or continue with rest of processing ?  to be set as command-line param
        #for now conitnue

    ##### Run remaining transformation steps as unit
    try:
        step = '>>Load Run table'
        n = load_run_table(dbconn)
        log.info(step + ', %s rows inserted', str(n))

        step = '>>Load User (hub) table'
        n = load_user_table(dbconn)
        log.info(step + ', %s rows inserted', str(n))

        step = '>>Load Replica table'
        n = load_replica_table(dbconn)
        log.info(step + ', %s rows inserted', str(n))

        step = '>>Load/Update User (sat) table'
        n = load_update_user_sat(dbconn)
        log.info(step + ', %s rows inserted/updated', str(n))

        step = '>>Update stg table'
        n = update_process_complete(dbconn)
        log.info(step + ', %s rows updated', str(n))

        step = '>>commit'
        dbconn.commit()
        log.info(step)
    except Exception, msg:
        log.error("Step %s failed, must rollback all steps, error: %s", step, msg)
        dbconn.rollback()
    dbconn.close()
    log.info("Ending Loading Process!")





if __name__ == '__main__':
    main()
