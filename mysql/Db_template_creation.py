#! /usr/bin/env python 

# Re-written using DB-API to be more DB platform agnostic
import MySQLdb as dbapi
import argparse
import getpass
import sys

def get_params():
	parser = argparse.ArgumentParser()
	parser.add_argument("dbname", help="Define the name of new database to be created.  A new user having same name will also be created")
	parser.add_argument("-ho", "--host", default="bbpdbsrv05.epfl.ch", help="Default host: bbpdbsrv05.epfl.ch")
	parser.add_argument("-p", "--port", default="3306", help="Default port: 3306" )
	parser.add_argument("-u", "--user", default="mysql", help="This is used for connecting to database (need superuser privileges), default= mysql")
	parser.add_argument("--noReadOnly", action="store_true", default=False, help="Use this flag to avoid creating a readonly user")
	
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
		step = "Creating database"
		createDb = "create database %s" % (params["dbname"]) #?should I add: character set UTF8
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

		grant1 = "grant all privileges on %s.* to '%s'@'%%'" %(params["dbname"], params["dbname"])
		step = "Grant privilege to %s" % params["dbname"]
		cur.execute(grant1)
		
		if params["readonly"]:
			userro = params["dbname"] + "_ro"
			upwd = getpass.getpass("\nCreating read-only user '%s', please enter its password> " % userro )
			step = "Creating read-only user"
			createUserRo = "create user '%s'@'%%' identified by '%s'" %(userro,upwd)
			cur.execute(createUserRo)
			
			grantRo = "grant select, show view on %s.* to '%s'@'%%'" %(params["dbname"], userro)
			step = "Grant read-only privilege to %s" % userro
			cur.execute(grantRo)
			

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



