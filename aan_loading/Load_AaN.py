#! /usr/bin/env python
# This script populates A-a-N data model from the JSON JobReport input file.
# It relies on the library PS adapter for Python : 'psycopg2' installed in site-packages:
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
###  dbhost     = ...
###  dbname     = ...
###  dbport     = ...
###  dbuser     = ...
###  dbpwd      = ..
#[Email alert log]
###  recipient=martin.ouellet@epfl.ch
###
### However, any parameters can be overwritten through optional argument (ex. -dbhost newhost)
###
### This version completes the full DB loading after each JobReport file loaded :
###    - Iterate over Json files
###         - Bulk load one File
###         - Load run table
###         - Load user table
###         - Load run table
###         - Load replica table
###         - Load user sat table
###         - Update stage table

import sys
import json
import cStringIO
import os
import fnmatch
import shutil
from datetime import datetime
import psycopg2
import logging
import ConfigParser
import argparse
import time
import smtplib


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


def treat_loaded_file(file, params):
    if params["delFile"]:
        os.remove(params["filedir"] + file)
    else:
        shutil.move(params["filedir"] + file, params["filearch"])


def bulk_load_file(conn, repdir, pendingFile, monitoringfile):
    """
    :param pendingFile:
    :param truncateStg:
    :return: filename correctly loaded
    """
    log = logging.getLogger()
    stepname = 'bulk_load_file'

    #raises ValueError if not found
    ji = pendingFile.index("JobReport_")+10

    runuuid = pendingFile[ji:ji+36]
    datetext = pendingFile[ji+37:ji+56]
    start_dts = datetime(int(datetext[0:4]),int(datetext[5:7]),int(datetext[8:10]), \
                        int(datetext[11:13]),int(datetext[14:16]),int(datetext[17:19]))

    jobFile = open(repdir + pendingFile, "r")
    jobs = []
    try:
        content = json.load(jobFile)
        jobs = content.get('jobs')
    except ValueError:
        log.error("Skip invalid json file: %s", pendingFile)
        return pendingFile

    sio= cStringIO.StringIO()
    now = datetime.now()
    for j in jobs:
        jid, repno, repuuid, tarfilename = j.get('jobId'), j.get('replicaId'), j.get('uuid'), j.get('outputTarFilename')
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
                  str(now)+"\t"+
                  tarfilename+"\n" )
    sio.seek(0)
    numrows = len(jobs)
    cur = conn.cursor()
    try:
        now = datetime.now()
        time1 = time.time()
        cur.copy_from(sio, 'stg_file', sep='\t', \
                      columns=('run_uuid', 'job_no', 'replica_no', 'rep_uuid', 'boinc_userid', 'boinc_username', \
                               'user_time', 'wall_time', 'system_time', 'time_start', 'load_dts', 'tar_filename'))
        elapse = time.time() - time1
        log.info(stepname + ' completed (ElapseSec= %.3f), file: %s' % (elapse, pendingFile))
        #monitoring metrics
        log_to_monitoring(monitoringfile, now, stepname, numrows, elapse, conn.dsn)
        # log to DB for stat analysis
        log_to_db(cur, now, stepname, numrows, elapse)
    except Exception, e:
        msg = "Bulk load failed quit loading process, due to error: %s" % e
        log.error(msg)
        send_alertmail(stepname, msg)
        conn.rollback()
        cur.close()
        conn.close()
        sys.exit(-1)
                 
    #freeing resource
    sio.close()
    cur.close()
    return pendingFile


def load_run_table(cur, now):
    cur.execute("""
                insert
                into run_h(run_uuid,time_start,load_dts)
                (select distinct cast(run_uuid as uuid)
                        ,time_start
                        ,%s
                from stg_file
                where process_dts is null); """, (now,))
    return cur.rowcount


def load_user_h_table(cur, now):
    cur.execute("""
                insert into user_h(boinc_userid, load_dts)
                (select distinct s.boinc_userid, %s
                from stg_file s
                left outer join user_h u on (s.boinc_userid = u.boinc_userid)
                where
                s.process_dts is null
                and u.boinc_userid is null
                ); """, (now, ))
    count = cur.rowcount

    #TODO: validate username vs userid (assumption: many username possible for same userid)
    cur.execute("""
                insert into user_s(boinc_userid, username, valid_from, valid_to, load_dts)
                (select s.boinc_userid, max(s.boinc_username), min(s.time_start), %s, %s
                from stg_file s
                left outer join user_s u on (s.boinc_userid = u.boinc_userid)
                where
                s.process_dts is null
                and u.boinc_userid is null
                group by s.boinc_userid
                ); """, (MAX_EXP_DATE, now))
    return count


def load_user_s_table(cur, now):
    cur.execute("""
                with sat_rec_insert as (
                insert into user_s(boinc_userid, username, valid_from, valid_to, load_dts)
                (select new.boinc_userid, new.username, new.valid_from, %(max_date)s, %(now)s
                from
                    (select boinc_userid, max(boinc_username) username, max(time_start) valid_from
                    from stg_file
                    where process_dts is null
                    group by boinc_userid) new
                    inner join
                    (select boinc_userid, username, valid_from
                    from user_s
                    where valid_to = %(max_date)s) prev
                    on (new.boinc_userid = prev.boinc_userid)
                    where
                    new.valid_from > prev.valid_from
                    and new.username != prev.username --OR ...all sat attr would be added here..
                )
                returning *
                )
                update user_s set valid_to = sat_rec_insert.valid_from
                from sat_rec_insert
                where user_s.boinc_userid = sat_rec_insert.boinc_userid
                and user_s.valid_to = %(max_date)s; """, {'max_date':MAX_EXP_DATE, 'now':now} )
    return cur.rowcount


def load_replica_table(cur, now):
    cur.execute("""
                insert into replica_l(rep_uuid, boinc_userid, run_uuid, replica_no
                                       ,slave_validated, time_start, wall_time
                                       ,system_time, user_time, host_id, day_period
                                       ,tar_filename, job_no, load_dts)
                (select cast(rep_uuid as uuid), boinc_userid, cast(run_uuid as uuid), replica_no
                        ,slave_validated, time_start, wall_time
                        ,system_time, user_time, host_id, cast(to_char(time_start,'YYYYMMDD') as int)
                        ,tar_filename, job_no, %s
                from stg_file
                where process_dts is null)""", (now,))
    return cur.rowcount


def update_stg_table(cur, now):
    cur.execute("""
                update stg_file
                set process_dts = %s
                where process_dts is null;""", (now,))
    return cur.rowcount



# Generic sql caller to avoid repeating same generic code for : logging, cursor and error handling, etc.
# Similar to a Java template 
def exec_sql_stmt(conn, stmt_fct, monitoringfile):
    log = logging.getLogger()
    cur = conn.cursor()
    stepname = stmt_fct.func_name
    try:
        now = datetime.now()
        time1 = time.time()
        n = stmt_fct(cur, now)
        elapse = time.time() - time1
        log.info(stepname + ' completed, %s rows processed (ElapseSec= %.3f)' % (n, elapse))
        #monitoring Metrics
        log_to_monitoring(monitoringfile, now, stepname, n, elapse, conn.dsn)
        #log to DB for stats (activate if needed)
        log_to_db(cur, now, stepname, n, elapse)
    except Exception, e:
        message = "Step %s failed, rollback all steps and quit loading process, error: %s" % (stepname, e)
        log.error(message)
        send_alertmail(stepname, message)
        conn.rollback()
        conn.close()
        sys.exit(-1)
    cur.close()    
    

def log_to_db(cursor, now, stepname, numrows, elapseSec):
    cursor.execute("""
                insert into log_info(log_time, step, numrows, elapse_sec)
                values(%s, %s, %s, %s)
                """, (now, stepname, numrows, elapseSec))
    

def log_to_monitoring(f, now, stepname, numrows, elapse, db_dsn):
        monitoring = {}
        ixhost = db_dsn.index("host=") + 5
        ixdbname = db_dsn.index("dbname=") + 7
        monitoring.update({"timestamp": str(now),
                           "metrics": {stepname + '-rows': numrows, stepname + '-elapse': elapse},
                           "metadata": {"db_host": db_dsn[ixhost:db_dsn.index(" ",ixhost)], 
                                        "db_name": db_dsn[ixdbname:db_dsn.index(" ",ixdbname)]} })
        #flush monitoring
        f.write(json.dumps(monitoring))
        f.write('\n')

        
#set in read_params(), not best but avoid having to pass around this list
EMAIL_RECIPIENT = ""

def send_alertmail(subject, msg):
    sender = "martin.ouellet@epfl.ch"
    smtp_addr = "mail.epfl.ch"
    recipients = EMAIL_RECIPIENT.split(",")
    
    message  = "From: DB Loader monitor <%s>\n" % sender
    message += "To: %s\n" % EMAIL_RECIPIENT
    message += "Subject: DB loading issue, %s\n\n" % subject 
    message += msg
    
    try:
        smtpObj = smtplib.SMTP(smtp_addr)
        smtpObj.sendmail(sender, recipients, message)
    except smtplib.SMTPException, e:
        print "Send email alert failed for message, due to error: %s" % e


# Read config file and overwrite parameter if optional arguments are set
# return all param values as Dictionary
def read_params():

    #####setup conf file parameters and optional arguments
    parser = argparse.ArgumentParser()
    ##Only mandatory argument is name of config file
    parser.add_argument("conf_file")
    ##All other are optinal arguments
    parser.add_argument("--delFile", action="store_true", help="Opt Argument for deleting JSON file after successful load")
    parser.add_argument("-d","--dbhost")
    parser.add_argument("-dp","--dbport")
    parser.add_argument("-dn","--dbname")
    parser.add_argument("-du","--dbuser")
    parser.add_argument("-dpw","--dbpwd")
    parser.add_argument("-fd","--filedir")
    parser.add_argument("-fa","--filearch")
    parser.add_argument("-fp","--filepattern")
    parser.add_argument("-mf","--monitoringFile", default="/tmp/myprog.mon")
    
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
    
    global EMAIL_RECIPIENT 
    EMAIL_RECIPIENT = conf.get('Email alert log','recipient')
    
    #params only settable as optional command-line argument
    params["delFile"] = args.delFile
    params["monitoringFile"] = args.monitoringFile
    
    return params



def main():
    
    #####Get config parameters
    params = read_params()
    
    #####set up the logger
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    # add a consol handler (stdout) and file handler
    consolHandler = logging.StreamHandler()
    fileHandler = logging.FileHandler("Log_Load_AaN.log") # os.path.dirname(__file__) 
    # both handler uses same formatter  [%(db)s]
    fstr = '%(asctime)s [%(levelname)s] [Db:' + params["dbname"] + '] - %(message)s'
    formatter = logging.Formatter(fstr)
    consolHandler.setFormatter(formatter)
    fileHandler.setFormatter(formatter)
    # add the two handler (can (de)activate as needed for prod)
    log.addHandler(consolHandler)
    log.addHandler(fileHandler)


    #### setup of DB connection
    dbconn = None
    try:
        dbconn = psycopg2.connect(host=params["dbhost"], database=params["dbname"], port=params["dbport"], 
                                  user=params["dbuser"], password=params["dbpwd"])
    except Exception, msg:
        m = "DB connection error: %s" % msg
        log.error(m)
        send_alertmail(params["dbhost"] + " connection error", m)
        sys.exit(-1)


    ############ Starting Load process #############
    pendingFiles = get_pending_files(params["filedir"], params["filepattern"])
    if len(pendingFiles) == 0:
        #exit (could force to continue through a command-line param)
        log.info("No JSON files to load, stop process")
        sys.exit(0)

    log.info("Starting Loading Process, found %d JSON files to load!" % len(pendingFiles))
    i = 0
    monitoringfile = open(params["monitoringFile"], "a")

    for pendingFile in pendingFiles:
        
        #### Run Bulk Load on one file
        filetoArchive = bulk_load_file(dbconn, params["filedir"], pendingFile, monitoringfile)
        
        exec_sql_stmt(dbconn, load_run_table, monitoringfile)

        exec_sql_stmt(dbconn, load_user_h_table, monitoringfile)

        exec_sql_stmt(dbconn, load_replica_table, monitoringfile)

        exec_sql_stmt(dbconn, load_user_s_table, monitoringfile)

        exec_sql_stmt(dbconn, update_stg_table, monitoringfile)

        #all data loaded for one file, safe to commit and archive
        dbconn.commit()
        treat_loaded_file(filetoArchive, params)
        i += 1
        
        log.info('Commit completed, archiving file#%d: %s' % (i ,filetoArchive))
        
        

    ############ Ending Load process #############
    dbconn.close()
    monitoringfile.close()
    log.info("Ending Loading Process!")
    


if __name__ == '__main__':
    main()
