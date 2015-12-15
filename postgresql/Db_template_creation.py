#! /usr/bin/env python 
import psycopg2
import argparse
import getpass
import sys
import string
import random

def get_params():
	parser = argparse.ArgumentParser()
	parser.add_argument("dbname", help="Define the name of new database to be created.  A new role (i.e. owner) is also created with same name")
	parser.add_argument("-ho", "--host", default="localhost", help="Default host: localhost")
	parser.add_argument("-p", "--port", default="5432", help="5432" )
	parser.add_argument("-u", "--user", default="postgres", help="This is used for connecting to database (need superuser privileges), default= postgres")
	parser.add_argument("--noReadOnly", action="store_true", default=False, help="Use this flag to avoid creating a readonly user")
	
	args = parser.parse_args()
	params = {}
	params["dbname"] = args.dbname
	params["host"] = args.host
	params["port"] = args.port
	params["user"] = args.user
	params["readonly"] = not args.noReadOnly
	return params

def gen_pwd(size):
	l = string.ascii_letters + string.digits 
	p = ''
	for i in range(size):
		p += random.choice(l)
	return p

def main():
	params = get_params()
	print("\nThe script is launched with these config:" + str(params) )
	#### Connect to database as SuperUser
	ppwd = getpass.getpass("\nPlease enter the password of '%s' to connect as superuser> " % params["user"])
	dbconn = None
	cur = None
        try:
		step = "Connecting to db"
        	dbconn = psycopg2.connect(host=params["host"],
                                database="postgres",
                                port=params["port"],
                                user=params["user"],
                                password=ppwd)
		#make the connection auto-commit
		dbconn.set_isolation_level(0)

	#### As SuperUser, create new role and new database
		upwd = gen_pwd(8) #getpass.getpass("\nCreating new role '%s', please enter its password> " %params["dbname"] )

		cur = dbconn.cursor()
		step = "Creating and altering new role"
		createUser1 = "create role %s with login password '%s'" %(params["dbname"],upwd)
		createUser2 = "alter role %s CREATEROLE" % params["dbname"]
		cur.execute(createUser1)
		cur.execute(createUser2)
		print("New user '%s' created successfully, password='%s'" % (params["dbname"],upwd))
		step = "Creating database"
		createDb = "create database %s with owner = %s" % (params["dbname"],params["dbname"])
		cur.execute(createDb)
	except Exception, msg:
		print("\nError during the execution of step '%s', error: %s" % (step, msg))
		print("\nPlease check if role '%s' and database '%s' were created"  % (params["dbname"],params["dbname"]))
		print("This script will not drop any of these are considered too risky!" )
		sys.exit(-1)
	finally:
		if cur != None:
			cur.close()
		if dbconn != None:
			dbconn.close()


	#### As SuperUser, connect to new database and revoke default privilege
	try:
        	print("\nNew database '%s' created successfully, now connecting to it..." % params["dbname"])
		step = "Connecting to new database"
		dbconn = psycopg2.connect(host=params["host"],
                                database=params["dbname"],
                                port=params["port"],
                                user=params["user"],
                                password=ppwd)
		dbconn.set_isolation_level(0)

		revoke1 = "revoke connect on database %s from public" %(params["dbname"])
		revoke2 = "revoke all on schema public from public"
		revoke3 = "grant all on schema public to %s" %(params["dbname"])
		
		cur = dbconn.cursor()
		step = "Revoking default privilege"
		cur.execute(revoke1)
		cur.execute(revoke2)
		cur.execute(revoke3)		
	#### As SuperUser, create optional read-only user
		if params["readonly"]:
			ro = params["dbname"] + "_ro"
			ropwd = gen_pwd(8)  #getpass.getpass("Creating new read-only role '%s', please enter its password> " % ro)

			createRo1 = "create role %s with login password '%s'" %(ro, ropwd)
			step = "Create read-only role"
			cur.execute(createRo1)

			grantRo1 = "grant connect on database %s to %s" %(params["dbname"], ro)
			grantRo2 = "grant usage on schema public to %s" % ro
			grantRo3 = "grant select on all tables in schema public to %s" % ro
			step = "Grant proper privileges to read-only user"
			cur.execute(grantRo1)
			cur.execute(grantRo2)
			cur.execute(grantRo3)
			print("New read-only user '%s' created successfully, password='%s'" % (ro,ropwd))
	except Exception, msg:
		print("Error during the execution of step %s, with error: %s" % (step, msg))
		print("\nPlease check privileges and role setting")
		print("This script does not rollback any changes as these are considered too risky!" )
		sys.exit(-1)
	finally:
		if cur != None:
			cur.close()
		if dbconn != None:
			dbconn.close()

	print("Finished creating new database!")

if __name__ == "__main__":
	main()


