#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""SQLite uses dynamic data structure, which converts 
the inserted data type to the following 5 categories:
1. NULL: empty value
2. Integer: signed int
3. Real: float
4. Text: string
5. Blob: binary
"""

import sqlite3 as sqlite
import pandas as pd
import numpy as np

import datetime
import time

import get_CSV_ready as get_CSV

class GetDatabase:
	def __init__(self):
		# Import data from .db to pandas
		self.csv = get_CSV.PrepareCSV()
		self.csv_data = self.csv.prepare_CSV()
		self.csv_data.fillna('', inplace=True)

		self.number_lines_read = 3
		self.reconnect = 5

		self.db_file = 'inventory.db'

	def sql_init(self):
		counter = 0
		while counter <= self.reconnect:
			try:
				con = sqlite.connect(self.db_file)
				break
			except Exception as Eor:
				counter += 1
				if counter == self.reconnect + 1:
					print("Database connection faliure")
					con = None

		return con

	def generate_cursor(self, db_engine):
		cursor = db_engine.cursor() # Active cursor
		return cursor

	def close_cursor(self, cursor):
		cursor.close() # Close cursor
		return None

	def sql_quit(self, db_engine):
		db_engine.close() # Close connection
		return None

	def sql_backup(self):
		"""Create timestamped database copy to a local folder
		Note that this approach may not be necessary since you 
		can manage the auto-backup in your server, but is another 
		safty treatment that you can use.
		Here is another way of doing this using the online API:
		https://gist.github.com/achimnol/3021995
		"""

		import os, errno
		import shutil

		backup_dir = os.getcwd() + '\DataBase_Backup'
		db_file = self.db_file

		if not os.path.exists(backup_dir):
			try:
				os.makedirs(backup_dir)
			except OSError as e:
				# This except is used in case the directory is created
				# in between the check and make directory commands
				if e.errno != errno.EEXIST:
					raise

		backup_file = os.path.join(backup_dir, os.path.basename(db_file) +
									time.strftime("-%Y%m%d-%H%M%S"))

		connection = self.sql_init()
		cursor = self.generate_cursor(connection)

		# Lock database before making a backup
		# BEGIN IMMEDIATE gets a reserved lock
		# where others can read but not write
		cursor.execute('BEGIN IMMEDIATE')

		# Make new backup file
		# for shutil check https://docs.python.org/2/library/shutil.html
		# for more details
		shutil.copyfile(db_file, backup_file)
		print("\nCreating {}...".format(backup_file))

		# Unlock database
		connection.rollback()

		self.close_cursor(cursor)
		self.sql_quit(connection)

		return None

	def diff_pd(self, df1, df2):
		"""Identify differences between two pandas DataFrames
		The methods only work when the two dataframes have 
		identical column names, otherwise an AssertionError
		will raise.
		"""
		assert (df1.columns == df2.columns).all(), \
			"DataFrame column names are different"
		
		# Assume that even if the two dataframes has only difference
		# in 'last_modified_time' (i.e. the content of the duplicated
		# rows are identical), we will see that needs to be updated.
		# Thus we do not ignore the 'last_modified_time' column
		ignore_columns = ['index', 'v_id', 'created_time']
		df1.drop(columns=ignore_columns, inplace=True)
		df2.drop(columns=ignore_columns, inplace=True)

		if df1.equals(df2):
			return None
		else:
			diff_mask = (df1 != df2)

			# Pivot a level of the (possibly hierarchical) column labels
			# to make a new dataframe
			ne_stacked = diff_mask.stack()
			changed = ne_stacked[ne_stacked]
			changed.index.names = ['id', 'col']

			# Give the difference column names
			difference_locations = np.where(diff_mask)
			changed_from = df1.values[difference_locations]
			changed_to = df2.values[difference_locations]

			result = pd.DataFrame({'from': changed_from, 'to': changed_to},
								index=changed.index)

			# Modify the index to use vin number as ID
			result.reset_index(inplace = True)
			for i in range(len(result['id'])):
				result.id[i] = df1.vin[result.id[i]]
			result.rename(columns = {'id':'vin'}, inplace=True)
			result.set_index(['vin', 'col'], inplace = True)

			return result

	def get_column_ordered(self):
		# Get the csv file to have the exact same column
		# orders as in database
		connection = self.sql_init()
		sql = "SELECT * from Inventory LIMIT 1"
		self.db_data_raw = pd.read_sql(sql, connection)

		column_order = list(self.db_data_raw)
		self.csv_data = self.csv_data[column_order]

	def update_frame(self, df, name=None, conn=None):
		"""Update SQLite with DataFrame"""
		status = False
		cursor = self.generate_cursor(conn)

		# Modify the dataframe structure for upload
		df.reset_index(inplace = True)
		column_order = ['col', 'to', 'vin']
		df = df[column_order]

		data = [list(x) for x in df.values]

		try:
			for i in range(len(data[0])):
				update_sql = 'UPDATE %s SET {} = ? WHERE vin = ?'.format(data[i][0]) % (name)
				cursor.execute(update_sql, data[i][1:])
				conn.commit()

			status = True
		except Exception as Eor:
			status = False
			print("Update database entry Failure")

		self.close_cursor(cursor)

		return status

	def write_frame(self, df, name=None, con=None):
		"""Write records stored in a DataFrame to SQLite"""
		    
		name_holder = ','.join(['?'] * len(df.columns))
		insert_sql = 'INSERT INTO %s VALUES (%s)' % (name, name_holder)
		# print('insert_sql:\n', insert_sql)
		# print('===========================')
		data = [tuple(x) for x in df.values]
		# print('data:\n', data)

		return insert_sql, data

	def update_database(self):
		# Prepare for dataframe comparison
		self.get_column_ordered()

		iteration = 0
		last_v_id_stored = 0
		start_timing = datetime.datetime.now()

		connection = self.sql_init()
		sql = "SELECT * from Inventory"
		self.sql_backup() # Backup sql before any operations

		# Setup chunksize so the program reads the databse 
		# a few lines at a time. This will prevent memory 
		# leaks when dealing with large database entries
		for db_data in pd.read_sql_query(sql, \
							connection, chunksize=self.number_lines_read):
			# Since read_sql_query gives NoneType when the cell is none
			# We have to fill in the none types
			db_data.fillna('', inplace=True)

			last_v_id_stored = db_data['v_id'][len(db_data['v_id'])-1]

			# Find duplicated rows
			db_dup_rows = db_data[
								db_data['vin'].isin(self.csv_data['vin'])
								].reset_index()
			# print(db_dup_rows['vin'])
			csv_dup_rows = self.csv_data[
								self.csv_data['vin'].isin(db_data['vin'])
								].reset_index()
			# print(csv_dup_rows['vin'])
			update_result = self.diff_pd(db_dup_rows, csv_dup_rows)
			dup_vin = csv_dup_rows['vin']
			print(update_result)
			
			# update the sql with the duplicated row
			if self.update_frame(df=update_result, name='inventory', conn=connection):
				print("DataBase entry updated...")
			else:
				print("DataBase entry not updated")

			# delete the duplicate row inside .csv file
			self.csv_data = self.csv_data[self.csv_data['vin'].isin(dup_vin) == False] 

			iteration += 1
			end_timing = datetime.datetime.now()
			print('%s seconds: completed %s rows\n' % 
				((end_timing - start_timing).seconds, 
					iteration*self.number_lines_read))

		# Prepare the csv file to be uploaded to database
		self.csv_data['v_id'] = range(last_v_id_stored + 1, \
						last_v_id_stored + 1 + len(self.csv_data['v_id']))
		# Clean out empty strings
		self.csv_data.replace(r'^\s*$', np.nan, regex=True, inplace = True)

		# Upload the rest of the .csv file
		self.csv_data.to_sql('inventory', connection, index=False, if_exists='append')

		self.sql_quit(connection)


if __name__ == "__main__":
	db = GetDatabase()
	db.update_database()