# -*- coding: utf-8 -*-
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#	THIS IS A CLASS DEFINITION MODULE.
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#************************************************************************************************************************************************
#***** Author:		Peter McCutcheon
#***** Created:		4/29/2017
#***** Modified		4/29/2017	Initial creation of code.
#***** 				5/6/2017	Change addTo for SQLite insert. Sqlite does not support set colname=value
#*****				5/17/2017	Added the method getColumns
#*****              6/16/2017   Added parameter msgMode
#*****
#------------------------------------------------------------------------------------------------------------------------------------------------
#***** Description:
#*****
#*****		This is a module that contains a class that implements a number of relational SQL based database manipulations. Presently this module only supports
#*****		MySql and SQLite3 databases.
#*****
#*************************************************************************************************************************************************

#=====================================================================================================
#==		Define our new class here.
#=====================================================================================================

__metaclass__ = type

class DBMaint:

    #-------------------------------------------------------------
    #----	Define the constructor function here.
    #----
    #----	This constructor will import necessary database modules
    #----	and open or connect to the database.
    #-------------------------------------------------------------
	
    def __init__(self, msgMode, dbtype, dbname, dbuser='', dbpass=''):
        #
        #	Grab the parameters here.
        #
        self.databaseType = dbtype
        self.databaseName = dbname
        self.databaseUser = dbuser
        self.databasePass = dbpass
        self.msgMode = msgMode
        #
        #   msgMode true indicates that you want error messages printed.
        #   msgMode false indicates that you want dont' want error messages printed.
        #
        returnData = True
		#
		#	See which type of database we have, import necessary modules, and open database.
		#
        if self.databaseType.lower() == 'mysql':
            import pymysql
            import pymysql.cursors
            try:
                self.cnx = pymysql.connect(user=self.databaseUser, password=self.databasePass, host='127.0.0.1', database=self.databaseName)
            except Exception as e:
                print("Ho we did get a connect error.")
                returnData = self.__errorHandler("Attempting to connect to database, ", self.databaseName, e)
                return
        elif self.databaseType.lower() == "sqlite":
            import sqlite3
            try:
                self.cnx = sqlite3.connect(self.databaseName)
            except Exception as e:
                returnData = self.__errorHandler("Attempting to connect to database, ", self.databaseName, e)
                return
        else:
            returnData = self.__errorHandler("Unsupported database type, please notify the developer.", self.databaseType, None)
            return
	
	#-------------------------------------------------------------
	#----	Define the table retreive method here.
	#----
	#----	This method will take a tablename, a list of fields, and 
	#----	a string where clause then retrieve the data from the 
	#----	specified table.  The method will return a two dimensional
	#----	list which will be rows by field columns.
	#-------------------------------------------------------------
	
    def retrieveFrom(self, tblname, fieldList=[], whereClause=''):
		#
		#	Find out which database type we are dealing with.
		#
        returnData = True
        if self.databaseType.lower() == "mysql":

            try:
                cursor = self.cnx.cursor()
            except Exception as e:
                returnData = self.__errorHandler("Attempting to create a cursor for, ", self.databaseName, e)
                return returnData

			#
			#	If no field list was provided then select all the fields using *
			#
            if not fieldList:
                fieldListStr = "*"
            else:
                fieldListStr = ', '.join(fieldList)

            selectStatemnt = "Select " + fieldListStr + " from " + tblname
            if whereClause != '':
                selectStatemnt += " where " + whereClause

            selectStatemnt += ';'
            try:
                cursor.execute(selectStatemnt)
            except  Exception as e:
                returnData = self.__errorHandler("Attempting retrieve data from, ", tblname, e)
                return returnData

            try:
                returnData = cursor.fetchall()
            except Exception as e:
                returnData = self.__errorHandler("Attempting fetch all rows, ", tblname, e)
                return returnData
			
            cursor.close()
        else:
			#
			#	The code here for SQLite is exactly the same as for MySql.
			#	At the creation of this code this is the case, at some later
			#	time this may change due to changes in either SQLite or MySql
			#	which is why the code has been separated.
			#
            if self.databaseType.lower() == "sqlite":
                try:
                    cursor = self.cnx.cursor()
                except Exception as e:
                    returnData = self.__errorHandler("Attempting to create a cursor for database, ", self.databaseName, e)
                    return returnData
				#
				#	If no field list was provided then select all the fields using *
				#
                if not fieldList:
                    fieldListStr = "*"
                else:
                    fieldListStr = ', '.join(fieldList)

                selectStatemnt = "Select " + fieldListStr + " from " + tblname
                if whereClause != '':
                    selectStatemnt += " where " + whereClause

                selectStatemnt += ';'
                try:
                    cursor.execute(selectStatemnt)
                except  Exception as e:
                    returnData = self.__errorHandler("Attempting to retrieve data from, ", self.tblname, e)
                    return returnData
                returnData = cursor.fetchall()
                cursor.close()

        return returnData
	
	#-------------------------------------------------------------
	#----	Define the table insert method here.
	#----
	#----	This method will take a tablename, a list of fields,
	#----	a list of values and insert a new row into the table.
	#----	The method returns either success or failure (Boolean).
	#-------------------------------------------------------------
	
    def addTo(self, tbl, fl, fv):
		
        returnData = True
        tablename = tbl
        fieldList = fl
        fieldValues = fv
        insertStatemnt = "INSERT INTO " + tablename
		#
		#	Check to see that we have a field list and field values for the insert.
		#	If not we have an error, need these to perform the insert.
		#
        if not fl:
            returnData = self.__errorHandler("No field list provided on update in DBMaint, contact IT or developer.", None, None)
            return returnData
        else:
            fieldlist = fl
            fieldlistLen = len(fieldlist)

        if not fv:
            returnData = self.__errorHandler("No field values provided on update in DBMaint, contact IT or developer.", None, None)
            return returnData
        else:
            fieldvalue = fv
            fieldvalueLen = len(fieldvalue)
			
        if fieldlistLen != fieldvalueLen:
            returnData = self.__errorHandler("Field names and field values don't match, contact IT or developer.", None, None)
            return returnData

        if self.databaseType.lower() == "mysql":
            insertStatemnt += " set "
            fieldlistvalueCombo = dict(zip(fieldlist,fieldvalue))
            for key, value in fieldlistvalueCombo.items():
                fld = key + "='" + value + "', "
                insertStatemnt += fld
			
            insertStatemnt = insertStatemnt[:-2]
            try:
                cursor = self.cnx.cursor()
            except  Exception as e:
                returnData = self.__errorHandler("Attempting to create a cursor for database, ", self.databaseName, e)
                return returnData
            try:
                cursor.execute(insertStatemnt)
                self.cnx.commit()
            except  Exception as e:
                returnData = self.__errorHandler("Attempting to insert rows into, ", tbl, e)
                return returnData
            cursor.close
        else:
			#
			#	SQLite does not have an insert with set 'colname=value' construct.
			#
            if self.databaseType.lower() == "sqlite":
                i = 0
                names = " ("
                values = "("
                while i < fieldlistLen:
                    names += "'" + fieldlist[i] + "', "
                    values += "'" + fieldvalue[i] + "', "
                    i += 1
					
                names = names[:-2] + ")"
                values = values[:-2] + ")"
                insertStatemnt += names + " values " + values + ";"
                try:
                    cursor = self.cnx.cursor()
                except  Exception as e:
                    returnData = self.__errorHandler("Attempting to create a cursor for database, ", self.databaseName, e)
                    return returnData
                try:
                    cursor.execute(insertStatemnt)
                    self.cnx.commit()
                except  Exception as e:
                    returnData = self.__errorHandler("Attempting to insert rows into, ", tbl, e)
                    return returnData
                cursor.close
				
        return returnData
		
	#-------------------------------------------------------------
	#----	Define the remove row from table method here.
	#----
	#----	This method will take a tablename and a string where
	#----	clause and remove a row from the table.  The method
	#----	returns success or failure (boolean).
	#-------------------------------------------------------------
	
    def removeFrom(self, tbl, wc):
		#
		#	Check to see if we have a table name, can't do anything without it.
		#	Also set up a couple of other variables we'll need.
		#
		#	NOTE: IF NO WHERE CLAUSE IS SENT TO THIS METHOD THEN THIS METHOD WILL
		#	DELETE ALL THE ROWS IN THE SPECIFIED TABLE!!!!!!  THIS IS BY DESIGN!!
		#
        returnData = True
        if not tbl:
            returnData = self.__errorHandler("No table name provided on delete in DBMaint, contact IT or developer. ", None, None)
            return returnData
        else:
            tablename = tbl
        whereClause = wc
        deleteStatemnt = "DELETE FROM " + tablename
		
        if self.databaseType.lower() == "mysql":
            try:
                cursor = self.cnx.cursor()
            except Exception as e:
                returnData = self.__errorHandler("Attempting to create a cursor for database, ", self.databaseName, e)
                return returnData
            if not whereClause:
                deleteStatemnt += ";"
            else:
                deleteStatemnt += " Where " + whereClause + ";"		
            try:
                cursor.execute(deleteStatemnt)
                self.cnx.commit()
            except Exception as e:
                returnData = self.__errorHandler("Attempting to delete row(s) from, ", tbl, e)
                return returnData
            cursor.close()
        else:
			#
			#	The code here for SQLite is exactly the same as for MySql.
			#	At the creation of this code this is the case, at some later
			#	time this may change due to changes in either SQLite or MySql
			#	which is why the code has been separated.
			#
            if self.databaseType.lower() == "sqlite":
                if not whereClause:
                    deleteStatemnt += ";"
                else:
                    deleteStatemnt += " Where " + whereClause + ";"		
                try:
                    cursor = self.cnx.cursor()
                except Exception as e:
                    returnData = self.__errorHandler("Attempting to create a cursor for database, ", self.databaseName, e)
                    return returnData
                try:
                    cursor.execute(deleteStatemnt)
                    self.cnx.commit()
                except Exception as e:
                    returnData = self.__errorHandler("Attempting to delete row(s) from, ", tbl, e)
                    return returnData
                cursor.close()

        return returnData

	#-------------------------------------------------------------
	#----	Define the update a row in table method here.
	#----
	#----	This method will takes a tablename, list of fields,
	#----	a list of field values, and a string where clause
	#----	and update the desired row in the table.  The method
	#----	returns success or failure (boolean).
	#-------------------------------------------------------------
	
    def update(self, tbl, fl, fv, wc):
		#
		#	Must have a table name, field list, and field values to
		#	perform the update.  Also set up other variables we will
		#	use.
		#
        returnData = True
        if not tbl:
            returnData = self.__errorHandler("No table name provided on update in DBMaint, contact IT or developer.", None, None)
            return returnData
        else:
            tablename = tbl

        if not fl:
            returnData = self.__errorHandler("No field list provided on update in DBMaint, contact IT or developer.", None, None)
            return returnData
        else:
            fieldlist = fl
            fieldlistLen = len(fieldlist)

        if not fv:
            returnData = self.__errorHandler("No field values provided on update in DBMaint, contact IT or developer.", None, None)
            return returnData
        else:
            fieldvalue = fv
            fieldvalueLen = len(fieldvalue)
			
        if fieldlistLen != fieldvalueLen:
            returnData = self.__errorHandler("Field names and field values don't match, contact IT or developer.", None, None)
            return returnData

        updateStatemnt = "UPDATE " + tablename + " set "
        fieldlistvalueCombo = dict(zip(fieldlist,fieldvalue))	
        for key, value in fieldlistvalueCombo.items():
            fld = key + "='" + value + "', "
            updateStatemnt += fld
			
        updateStatemnt = updateStatemnt[:-2]
        if not wc:
            whereClause = ""
        else:
            whereClause = " where " + wc

        updateStatemnt += whereClause + ";"
        if self.databaseType.lower() == "mysql":
            try:
                cursor = self.cnx.cursor()
            except Exception as e:
                returnData = self.__errorHandler("Attempting to create a cursor for database, ", self.databaseName, e)
                return returnData
            try:
                cursor.execute(updateStatemnt)
                self.cnx.commit()
            except Exception as e:
                returnData = self.__errorHandler("Attempting to update row(s) in, ", tbl, e)
                return returnData
            cursor.close()
        else:
			#
			#	The code here for SQLite is exactly the same as for MySql.
			#	At the creation of this code this is the case, at some later
			#	time this may change due to changes in either SQLite or MySql
			#	which is why the code has been separated.
			#
            if self.databaseType.lower() == "sqlite":
                try:
                    cursor = self.cnx.cursor()
                except Exception as e:
                    returnData = self.__errorHandler("Attempting to create a cursor for database, ", self.databaseName, e)
                    return returnData
                try:
                    cursor.execute(updateStatemnt)
                    self.cnx.commit()
                except Exception as e:
                    returnData = self.__errorHandler("Attempting to update row(s) in, ", tbl, e)
                    return returnData
                cursor.close()

        return returnData

	#-------------------------------------------------------------
	#----	Define the getColumns from table method here.
	#----
	#----	This method will take a tablename and generate the
	#----	list of columns defined for the table. It will
	#----	return this as a list or return false for an error.
	#-------------------------------------------------------------
	
    def getColumns(self, tbl):
		
		#
		#	Check that we have a table name.
		#
        if not tbl:
            returnData = self.__errorHandler("No table name provided for getColumns in DBMaint, contact IT or developer.", None, None)
            return returnData
        else:
            tablename = tbl
			
        if self.databaseType.lower() == "mysql":
            command = "describe " + tablename
            try:
                cursor = self.cnx.cursor()
                cursor.execute(command)
                data = cursor.fetchall()
                colnames = []
                for row in data:
                    colnames.append(row[0])
            except Exception as e:
                returnData = self.__errorHandler("Attempting to get columns for, ", tablename, e)
                return returnData
        else:
            if self.databaseType.lower() == "sqlite":
                command = "PRAGMA table_info(" + tablename + ")"
                try:
                    cursor = self.cnx.cursor()
                    cursor.execute(command)
                    data = cursor.fetchall()
                    colnames = []
                    for row in data:
                        colnames.append(row[1])
                except Exception as e:
                    returnData = self.__errorHandler("Attempting to get columns for, ", tablename, e)
                    return returnData
            else:
                returnData = self.__errorHandler("Invalid database type for getColumns in DBMaint, contact IT or developer.", None, None)
                return returnData
			
        cursor.close()
        return colnames
			
	#-------------------------------------------------------------
	#----	Define a standard internal error handler.
	#----
	#----	This method handles any internal errors generated while
	#----	manipulating the database.
	#-------------------------------------------------------------
	
    def __errorHandler(self, errText, errInfoOther, error):
		#
		#	For now we will just log the error to the standard output.
		#	Later this will change and a error log file will be created.
		#
        if self.msgMode:
            if errInfoOther:
                errText = "Error dbMaint: " + errText + errInfoOther
            else:
                errText = "Error dbMaint: " + errText
            print("")
            print(errText)
            print("")
		
        return False