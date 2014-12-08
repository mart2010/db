#! /usr/bin/env python 

# Re-written using DB-API to be more DB platform agnostic
import MySQLdb as dbapi
import argparse
import getpass
import sys

# IP-range for limiting access from inside BBP to all users created
# "128.178.187.%" (epfl and not geneva), "128.178.167.%", "10.80.0.%", "10.80.64.%", CSCS ranges: "148.187.76.%", "148.187.84.%", "148.187.85.%" 
# This is changed to firewall rule, so no longer restrict the access
ipranges =  () 


def get_params():
	parser = argparse.ArgumentParser()
	parser.add_argument("dbname", help="Define the name of new database and the new user")
	parser.add_argument("-ho", "--host", default="bbpdbsrv05.epfl.ch", help="Default host: bbpdbsrv05.epfl.ch")
	parser.add_argument("-p", "--port", default="3306", help="Default port: 3306" )
	parser.add_argument("-u", "--user", default="root", help="Used for connecting to database as superuser, default=root")
	parser.add_argument("--noReadOnly", action="store_true", default=False, help="Flag used to avoid creating a readonly user")
	
	args = parser.parse_args()
	params = {}
	params["dbname"] = args.dbname
	params["host"] = args.host
	params["port"] = int(args.port)
	params["user"] = args.user
	params["readonly"] = not args.noReadOnly
	return params


def main():
	params = get_params()
	print("\nThe script is launched with these config:" + str(params) )
	#### Connect to database as SuperUser
	ppwd = getpass.getpass("\nPlease enter the password of '%s' to connect as superuser> " % params["user"])
	dbconn = None
	cur = None

        try:
		step = "Connecting to MySQL host"
        	dbconn = dbapi.connect(host=params["host"],
                                port=params["port"],
                                user=params["user"],
                                passwd=ppwd)
		#make the connection auto-commit
		dbconn.autocommit(True)

	#### As SuperUser, create new user and new database
		upwd = getpass.getpass("\nCreating new user '%s', please enter its password> " %params["dbname"] )

		cur = dbconn.cursor()
		step = "Creating user"
		createUser1 = "create user '%s'@'%%' identified by '%s'" %(params["dbname"],upwd)
		cur.execute(createUser1)
		print("User %s created successfully!" % params["dbname"])
		step = "Creating database"
		createDb = "create database %s" % (params["dbname"]) #DB has default encoding='UTF8' 
		cur.execute(createDb)
	except Exception, msg:
		print("\nError during the execution of step '%s', error: %s" % (step, msg))
		print("\nPlease check if user '%s' and database '%s' were created"  % (params["dbname"],params["dbname"]))
		print("This script will not drop any objects in case of error!" )
		sys.exit(-1)
	finally:
		if cur != None:
			cur.close()
		if dbconn != None:
			dbconn.close()


	#### As SuperUser, connect to new database and grant all privilege to user on new database
	try:
        	print("\nNew database '%s' created successfully, now connecting to it..." % params["dbname"])
		step = "Connecting to new database"
		dbconn = dbapi.connect(host=params["host"],
                                db=params["dbname"],
                                port=params["port"],
                                user=params["user"],
                                passwd=ppwd)
		dbconn.autocommit(True)
		cur = dbconn.cursor()
		# grant access from all valid IP-ranges
		step = "Grant privilege to %s" % params["dbname"]
		for ip in ipranges:
			grant1 = "grant all privileges on %s.* to '%s'@'%s' identified by '%s'"\
					%(params["dbname"], params["dbname"],ip,upwd)
			cur.execute(grant1)

		print("Grant privileges to user %s successfully!" % params["dbname"]) 

		if params["readonly"]:
			userro = params["dbname"] + "_ro"
			upwd = getpass.getpass("\nCreating read-only user '%s', please enter its password> " % userro )
			step = "Creating read-only user"
			createUserRo = "create user '%s'@'%%' identified by '%s'" %(userro,upwd)
			cur.execute(createUserRo)
			print("Readonly User %s created successfully!" % params["dbname"])	
			step = "Grant read-only privilege to %s" % userro
			for ip in ipranges:
				grantRo = "grant select, show view on %s.* to '%s'@'%s'" %(params["dbname"], userro, ip )
				cur.execute(grantRo)
			print("Grant privileges to readonly user %s successfully!" % params["dbname"]) 
	except Exception, msg:
		print("Error during the execution of step %s, with error: %s" % (step, msg))
		print("\nPlease check privileges and role setting")
		print("This script does not drop any objects in case of error!" )
		sys.exit(-1)
	finally:
		if cur != None:
			cur.close()
		if dbconn != None:
			dbconn.close()

	print("Finished creating new database!")

if __name__ == "__main__":
	main()



