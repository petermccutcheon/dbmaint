# -*- coding: utf-8 -*-
#
#TESTING MODULE
#
#######################################################################################
#####	Author:		Peter McCutcheon
#####	Created:	5/1/2017
#####	Modified:	5/5/2017	Initial Creation
#####				5/6/2017	Added/changed test numbering
#####							Added test for connect/object creation
#####							Added double print after each test to separate output.
#####-------------------------------------------------
#####Description:
#####
#####		This module implements the unit test suite
#####		for the DBMaint module/class.
#####
#######################################################################################

from dbmaint import DBMaint

#
#		Test the creation of the object and the connection to the database.
#		1.1. Test for a valid connection
#		1.1.1  For MySql
#		1.1.2  For SQLite
#		1.2. Test for an invalid connection
#		1.2.1  For MySql
#		1.2.2  For SQLite
#		1.3 Test closing connection
#		1.3.1  For MySql
#		1.3.2  For SQLite

#
#Test 1.1.1 and 1.3.1
#
db = DBMaint(dbtype='mysql', dbname='FlightData', dbuser='python', dbpass='python')
if (hasattr(db, 'cnx')):
	db.cnx.close()
	print("********************")
	print("Test 1.1.1, 1.3.1 -- Database object created ok. (TEST PASSED..)")
	print("********************")
else:
	print("********************")
	print("Test 1.1.1 -- Error creating database object! (TEST FAILED...)")
	print("********************")
	
print("")	# Print to separate the tests.
print("")

#
#Test 1.2.1
#
db = DBMaint(dbtype='mysql', dbname='FlightDate', dbuser='python', dbpass='python')  #dbname is incorrect s/b 'flightdata'
if (hasattr(db, 'cnx')):
	db.cnx.close()
	print("********************")
	print("Test 1.2.1 -- Database object created ok. (TEST FAILED...)")
	print("********************")
else:
	print("********************")
	print("Test 1.2.1 -- Database object not created. (TEST PASSED..)")
	print("********************")

print("")	# Print to separate the tests.
print("")

#
#Test 1.1.2 and 1.3.2
#
db = DBMaint(dbtype='SQLite', dbname='cars.db', dbuser='', dbpass='')
if (hasattr(db, 'cnx')):
	db.cnx.close()
	print("********************")
	print("Test 1.1.2, 1.3.2 -- Database object created ok. (TEST PASSED..)")
	print("********************")
else:
	print("********************")
	print("Test 1.1.2 -- Error creating database object! (TEST FAILED...)")
	print("********************")
	
print("")	# Print to separate the tests.
print("")

#
#Test 1.2.2
#
db = DBMaint(dbtype='mysql', dbname='..\..\mdasanalytics\data\car.db', dbuser='', dbpass='')  #dbname is incorrect s/b 'cars.db'
if (hasattr(db, 'cnx')):
	db.cnx.close()
	print("********************")
	print("Test 1.2.2 -- Database object created ok. (TEST FAILED..)")
	print("********************")
else:
	print("********************")
	print("Test 1.2.2 -- Database object not created. (TEST PASSED..)")
	print("********************")

print("")	# Print to separate the tests.
print("")

#
#		Test the insert method.  (.addTo)
#		2.1. Test to make sure we can insert.
#		2.1.1  For mysql
#		2.1.2  for Sqlite
#		2.2. Test for field names.
#		2.3. Test for field values.
#		2.4. Test for matching field names and field values.
#		2.5. Test for recovering from duplicate keys.
#		2.6. Test for not enough fields.
#		2.6.1  For MySql
#		2.6.2  For SQLite

#
#Test 2.1.1
#
db = DBMaint(dbtype='mysql', dbname='FlightData', dbuser='python', dbpass='python')
ret = db.addTo('airplane', ['model', 'seating', 'speed', 'company', 'manudate'], ['727', '140', '650', 'Boeing', '1965-10-31'])
if ret:
	print("********************")
	print("Test 2.1.1 -- Insert a row. (TEST PASSED..)")
	print("********************")
else:
	print("********************")
	print("Test 2.1.1 -- Insert a row. (TEST FAILED..)")
	print("********************")
ret = db.addTo('airplane', ['model', 'seating', 'speed', 'company', 'manudate'], ['A380', '300', '670', 'Airbus', '2012-04-01'])
if ret:
	print("********************")
	print("Test 2.1.1 -- Insert a row. (TEST PASSED..)")
	print("********************")
else:
	print("********************")
	print("Test 2.1.1 -- Insert a row. (TEST FAILED..)")
	print("********************")
db.cnx.close()

#
#Test 2.1.2
#
db = DBMaint(dbtype='sqlite', dbname='cars.db', dbuser='', dbpass='')
ret = db.addTo('fastcars', ['make', 'model', 'topspeed', 'acceleration', 'price'], ['Cheverolet', 'Z06 Corvette', '210', '29', '105000'])
if ret:
	print("********************")
	print("Test 2.1.2 -- Insert a row. (TEST PASSED...)")
	print("********************")
else:
	print("********************")
	print("Test 2.1.2 -- Insert a row. (TEST FAILED..)")
	print("********************")
ret = db.addTo('fastcars', ['make', 'model', 'topspeed', 'acceleration', 'price'], ['Dodge', 'Charger-Hellcat', '210', '30', '95000'])
if ret:
	print("********************")
	print("Test 2.1.2 -- Insert a row. (TEST PASSED...)")
	print("********************")
else:
	print("********************")
	print("Test 2.1.2 -- Insert a row. (TEST FAILED..)")
	print("********************")
db.cnx.close()

#
#Test 2.2
#
db = DBMaint(dbtype='sqlite', dbname='cars.db', dbuser='', dbpass='')
ret = db.addTo('fastcars', [], ['Cheverolet', 'Z06 Corvette', '210', '29', '105000'])
if ret:
	print("********************")
	print("********************")
	print("Test 2.2 -- Insert: Check for column names. (TEST FAILED...)")
else:
	print("********************")
	print("Test 2.2 -- Insert: Check for column names. (TEST PASSED..)")
	print("********************")
db.cnx.close()

#
#Test 2.3
#
db = DBMaint(dbtype='sqlite', dbname='cars.db', dbuser='', dbpass='')
ret = db.addTo('fastcars', ['make', 'model', 'topspeed', 'acceleration', 'price'], [])
if ret:
	print("********************")
	print("Test 2.3 -- Insert: Check for values. (TEST FAILED...)")
	print("********************")
else:
	print("********************")
	print("Test 2.3 -- Insert: Check for values. (TEST PASSED..)")
	print("********************")
db.cnx.close()

#
#Test 2.4
#
db = DBMaint(dbtype='sqlite', dbname='cars.db', dbuser='', dbpass='')
ret = db.addTo('fastcars', ['make', 'model', 'topspeed', 'acceleration'], ['Cheverolet', 'Z06 Corvette', '210', '29', '105000'])
if ret:
	print("********************")
	print("Test 2.4 -- Insert: Check for matching names and values. (TEST FAILED...)")
	print("********************")
else:
	print("********************")
	print("Test 2.4 -- Insert: Check for matching names and values. (TEST PASSED..)")
	print("********************")
ret = db.addTo('fastcars', ['make', 'model', 'topspeed', 'acceleration', 'price'], ['Z06 Corvette', '210', '29', '105000'])
if ret:
	print("********************")
	print("Test 2.4 -- Insert: Check for matching names and values. (TEST FAILED...)")
	print("********************")
else:
	print("********************")
	print("Test 2.4 -- Insert: Check for matching names and values. (TEST PASSED..)")
	print("********************")
db.cnx.close()

#
#Test 2.5
#
db = DBMaint(dbtype='mysql', dbname='FlightData', dbuser='python', dbpass='python')
ret = db.addTo('myplanes', ['planenumber', 'planetype', 'planename', 'location'], ['1', 'Jet', 'Lear-Jet LJ200', 'Boston'])
ret = db.addTo('myplanes', ['planenumber', 'planetype', 'planename', 'location'], ['2', 'Helio', 'Bell Huey', 'Boston'])
ret = db.addTo('myplanes', ['planenumber', 'planetype', 'planename', 'location'], ['2', 'WWII Fighter', 'P-51 Mustang', 'Los Angeles'])
if ret:
	print("********************")
	print("Test 2.5 -- Insert: Check for duplicate keys. (TEST FAILED...)")
	print("********************")
else:
	print("********************")
	print("Test 2.5 -- Insert: Check for duplicate keys. (TEST PASSED..)")
	print("********************")
db.cnx.close()

#
#Test 2.6
#
db = DBMaint(dbtype='sqlite', dbname='cars.db', dbuser='', dbpass='')
ret = db.addTo('fastcars', ['make', 'model', 'topspeed'], ['Cheverolet', 'Z06 Corvette', '210', '29'])
if ret:
	print("********************")
	print("Test 2.6.1 -- Insert: Check for not enough fields. (TEST FAILED...)")
	print("********************")
else:
	print("********************")
	print("Test 2.6.1 -- Insert: Check for not enough fields. (TEST PASSED..)")
	print("********************")
db.cnx.close()
db = DBMaint(dbtype='mysql', dbname='FlightData', dbuser='python', dbpass='python')
ret = db.addTo('myplanes', ['planenumber', 'planetype', 'planename'], ['1', 'Jet', 'Lear-Jet LJ200'])
if ret:
	print("********************")
	print("Test 2.6.2 -- Insert: Check for not enough fields. (TEST FAILED...)")
	print("********************")
else:
	print("********************")
	print("Test 2.6.2 -- Insert: Check for not enough fields. (TEST PASSED..)")
	print("********************")
db.cnx.close()

print("")	# Print to separate the tests.
print("")

#
#		Test the update method.	(.update)
#		3.1. Test to make sure we can update.
#		3.1.1  For MySql
#		3.1.2  For SQLite
#		3.2. Test for field names.
#		3.3. Test for field values.
#		3.4. Test for missing where clause.
#		3.5. Test for matching field names and field values.
#		3.6. Test for recovering from duplicate keys.
#		3.7. Test for row or rows not found.

#
#Test 3.1.1
#
db = DBMaint(dbtype='mysql', dbname='FlightData', dbuser='python', dbpass='python')
ret = db.update('myplanes', ['planename'], ['Lear-Jet LJ200'], "planenumber = '1'")
if ret:
	print("********************")
	print("Test 3.1.1 -- Update: Check to make sure we can update. (TEST PASSED...)")
	print("********************")
else:
	print("********************")
	print("Test 3.1.1 -- Update: Check to make sure we can update. (TEST FAILED..)")
	print("********************")
db.cnx.close()

#
#Test 3.1.2
#
db = DBMaint(dbtype='sqlite', dbname='cars.db', dbuser='', dbpass='')
ret = db.update('fastcars', ['price'], ['90000'], "make = 'Dodge' and model = 'Charger-Hellcat'")
if ret:
	print("********************")
	print("Test 3.1.2 -- Update: Check to make sure we can update. (TEST PASSED...)")
	print("********************")
else:
	print("********************")
	print("Test 3.1.2 -- Update: Check to make sure we can update. (TEST FAILED..)")
	print("********************")
db.cnx.close()

print("")	# Print to separate the tests.
print("")

#
#		Test the select method. (.retrieveFrom)
#		4.1. Test to make sure we can read a row or rows.
#		4.1.1  For MySql
#		4.1.2  For SQLite
#		4.2. Test for empty list of field names which is all columns
#		4.3. Test for no where clause which is all rows.
#

#
#Test 4.1.1
#
db = DBMaint(dbtype='mysql', dbname='FlightData', dbuser='python', dbpass='python')
results = db.retrieveFrom('airplane', ['model', 'speed', 'seating'], "model = 'A310'")
if ret:
	print("********************")
	print("Test 4.1.1 -- Retrieve: Select a single row. (TEST PASSED...)")
	print("********************")
else:
	print("********************")
	print("Test 4.1.1 -- Retrieve: Select a single row. (TEST FAILED..)")
	print("********************")
db.cnx.close()

#
#Test 4.1.2
#
db = DBMaint(dbtype='sqlite', dbname='cars.db', dbuser='', dbpass='')
results = db.retrieveFrom('fastcars', ['make', 'model', 'topspeed', 'price'], "make = 'Dodge'")
if ret:
	print("********************")
	print("Test 4.1.2 -- Retrieve: Select a single row. (TEST PASSED...)")
	print("********************")
else:
	print("********************")
	print("Test 4.1.2 -- Retrieve: Select a single row. (TEST FAILED..)")
	print("********************")
db.cnx.close()

#
#Test 4.2
#
db = DBMaint(dbtype='sqlite', dbname='cars.db', dbuser='', dbpass='')
results = db.retrieveFrom('fastcars', ['make', 'model', 'topspeed', 'price'], "make = 'Dodge'")
if ret:
	print("********************")
	print("Test 4.2 -- Retrieve: Select a single row. (TEST PASSED...)")
	print("********************")
else:
	print("********************")
	print("Test 4.2 -- Retrieve: Select a single row. (TEST FAILED..)")
	print("********************")
db.cnx.close()

#
#		Test the delete method. (.removeFrom)
#		5.1. Test to make sure we can delete.
#		5.1.1  For MySql
#		5.1.2  For SQLite
#		5.2. Test for row not found.
#		5.3. Test for no where clause (This will delete all rows).
#

#
#Test 5.1.1
#
db = DBMaint(dbtype='mysql', dbname='FlightData', dbuser='python', dbpass='python')
ret = db.removeFrom("airplane", "model = 'A380'")
if ret:
	print("********************")
	print("Test 5.1.1 -- Remove: Check to make sure we can remove a row. (TEST PASSED...)")
	print("********************")
else:
	print("********************")
	print("Test 5.1.1 -- Remove: Check to make sure we can remove a row. (TEST FAILED..)")
	print("********************")
db.cnx.close()

#
#Test 5.1.2
#
db = DBMaint(dbtype='sqlite', dbname='cars.db', dbuser='', dbpass='')
ret = db.removeFrom('fastcars', "make = 'Chevrolet' and model = 'Z06 Corvette'")
if ret:
	print("********************")
	print("Test 5.1.2 -- Remove: Check to make sure we can remove a row. (TEST PASSED...)")
	print("********************")
else:
	print("********************")
	print("Test 5.1.2 -- Remove: Check to make sure we can remove a row. (TEST FAILED..)")
	print("********************")
db.cnx.close()

#
#Test 5.2
#
db = DBMaint(dbtype='sqlite', dbname='cars.db', dbuser='', dbpass='')
ret = db.removeFrom('fastcars', "model = 'Camaro'")
if ret:
	print("********************")
	print("Test 5.2 -- Remove: Test for row not found. (TEST PASSED...)")
	print("********************")
else:
	print("********************")
	print("Test 5.2 -- Remove: Test for row not found. (TEST FAILED..)")
	print("********************")
db.cnx.close()

#
#Test 5.3
#
db = DBMaint(dbtype='sqlite', dbname='cars.db', dbuser='', dbpass='')
ret = db.removeFrom('fastcars', "")
if ret:
	print("********************")
	print("Test 5.3 -- Remove: Test for no where clause, all rows. (TEST PASSED...)")
	print("********************")
else:
	print("********************")
	print("Test 5.3 -- Remove: Test for no where clause, all rows. (TEST FAILED..)")
	print("********************")
db.cnx.close()

print("")	# Print to separate the tests.
print("")

#
#		Test the getColumns method.
#		6.1. Test valid table name.
#		6.1.1  For MySql
#		6.1.2  For SQLite
#		6.2. Test for invalid table name.
#		6.2.1  For MySql
#		6.2.2  For SQLite
#

#
#Test 6.1.1
#
db = DBMaint(dbtype='mysql', dbname='FlightData', dbuser='python', dbpass='python')
ret = db.getColumns("airplane")
if ret:
	print("********************")
	print("Test 6.1.1 -- getColumn: Get a list of all columns. (TEST PASSED...)")
	print("********************")
	print("--------------------")
	print("Returned columns:")
	print()
	print(ret)
	print("--------------------")
else:
	print("********************")
	print("Test 6.1.1 -- getColumn: Get a list of all columns. (TEST FAILED..)")
	print("********************")
db.cnx.close()

#
#Test 6.1.2
#
db = DBMaint(dbtype='sqlite', dbname='cars.db', dbuser='', dbpass='')
ret = db.getColumns("cars")
if ret:
	print("********************")
	print("Test 6.1.2 -- getColumn: Get a list of all columns. (TEST PASSED...)")
	print("********************")
	print("--------------------")
	print("Returned columns:")
	print()
	print(ret)
	print("--------------------")
else:
	print("********************")
	print("Test 6.1.2 -- getColumn: Get a list of all columns. (TEST FAILED..)")
	print("********************")
db.cnx.close()

#
#Test 6.2.1
#
db = DBMaint(dbtype='mysql', dbname='FlightData', dbuser='python', dbpass='python')
ret = db.getColumns("airlines")
if ret:
	print("********************")
	print("Test 6.2.1 -- getColumn: Get a list of all columns. (TEST FAILED...)")
	print("********************")
	print("--------------------")
	print("Returned columns:")
	print()
	print(ret)
	print("--------------------")
else:
	print("********************")
	print("Test 6.2.1 -- getColumn: Get a list of all columns. (TEST PASSED..)")
	print("********************")
db.cnx.close()

#
#Test 6.2.2
#
db = DBMaint(dbtype='sqlite', dbname='cars.db', dbuser='', dbpass='')
ret = db.getColumns("supercars")
if ret:
	print("********************")
	print("Test 6.2.2 -- getColumn: Get a list of all columns. (TEST FAILED...)")
	print("********************")
	print("--------------------")
	print("Returned columns:")
	print()
	print(ret)
	print("--------------------")
else:
	print("********************")
	print("Test 6.2.2 -- getColumn: Get a list of all columns. (TEST PASSED..)")
	print("********************")
db.cnx.close()

print("")
print("")