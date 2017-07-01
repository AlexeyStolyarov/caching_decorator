# -*- coding: utf-8 -*-
#############################################################################
#  Caching decorator for caching results returned by functions:
#  
#  Usage:
#   
#  @cashe_to_db(timedelta)
#  def function_that_you_want_to_cashe:
#		pass
#  
#---------------------------------------------------------------------------#
#  CopyRight by Alexey A Stolyarov aka mazilla. 
#############################################################################

import base64
import os
import os.path
import pickle
import sqlite3
import time
from datetime import timedelta, datetime

DB_NAME 		= './cached_data.sqlite3'
DB_TABLE 		= 'cashed_data'
# Maximum cashed values
DB_MAX_ITEMS 	= 10

DT_FORMAT 		= "%Y-%m-%d %H:%M:%S.%f"

DB_DATA_TTL 	= timedelta(days=0, hours=0, minutes=0, seconds=10)
DB_DATA_TTL_5M 	= timedelta(days=0, hours=0, minutes=5, seconds=0)
DB_DATA_TTL_1H	= timedelta(days=0, hours=1, minutes=0, seconds=0)
DB_DATA_TTL_1D	= timedelta(days=1, hours=0, minutes=0, seconds=0)
DB_DATA_TTL_7D	= timedelta(days=7, hours=0, minutes=0, seconds=0)


#
# creating database used by our decorator
#
def create_DB(name, reset=None):
	if reset and os.path.isfile(name):
		os.remove(name)
	if not os.path.isfile(name):
		conn 	= sqlite3.connect(name)
		c 		= conn.cursor()
		SQL 	= "CREATE TABLE [cashed_data] ( [func_name] CHAR(50), [func_args] VARCHAR(500), [func_kwargs] VARCHAR(500), [cashed_result] TEXT(2048), [cashed_date] DATETIME);"
		c.execute(SQL)
		conn.commit()
		c.close()
	
create_DB(DB_NAME)


#
#  Decorator definition
#
def cashe_to_db(data_ttl=DB_DATA_TTL_1H, debug=None):
	def decorator(func):
		def wrapper(*args, **kwargs):
			is_cashed = None
			is_cropped = None
			conn = sqlite3.connect(DB_NAME)
			c = conn.cursor()
			
			PICKLE_ARGS   = base64.encodestring(pickle.dumps(args[1:], pickle.HIGHEST_PROTOCOL))
			PICKLE_KWARGS = base64.encodestring(pickle.dumps(kwargs, pickle.HIGHEST_PROTOCOL))
		
			
			
			SQL_SELECT = "SELECT * FROM  %s WHERE func_name='%s' AND func_args='%s' AND func_kwargs ='%s'"  % (
							DB_TABLE, func.__name__, PICKLE_ARGS, PICKLE_KWARGS		
							)
		
			SQL_DELETE = "DELETE FROM  %s WHERE func_name='%s' AND func_args='%s' AND func_kwargs ='%s'"  % (
							DB_TABLE, func.__name__, PICKLE_ARGS, PICKLE_KWARGS		
							)	
			
			SQL_COUNT  = "SELECT count(*) FROM  %s WHERE func_name='%s'"  % (DB_TABLE, func.__name__)
			SQL_CROP   = "DELETE FROM  %s WHERE func_name='%s' AND cashed_date=(SELECT min(cashed_date) FROM '%s')"  % (DB_TABLE, func.__name__, DB_TABLE)	
			SQL_CROP_TTL = "DELETE FROM  %s WHERE func_name='%s' AND cashed_date<'%s'"  % (DB_TABLE, func.__name__, datetime.now() - data_ttl)
			
			dt =  datetime.now()
			
			def SQL_INSERT(arg):
				PIKLE_RES = base64.encodestring(pickle.dumps(arg, pickle.HIGHEST_PROTOCOL))
				return "INSERT INTO  %s (func_name, func_args, func_kwargs, cashed_result, cashed_date) VALUES ('%s','%s', '%s', '%s','%s')" % (
							DB_TABLE, func.__name__, PICKLE_ARGS, PICKLE_KWARGS, PIKLE_RES, dt)
		
			def UNPIKLE(arg):
				try:
					return pickle.loads(base64.decodestring(arg))
				except:
					return None

			# killing all items with old ttl
			c.execute(SQL_CROP_TTL)
					
			fetched_row = c.execute(SQL_SELECT).fetchone()
			if fetched_row:
				# return cached values
				output =  UNPIKLE(fetched_row[3])
				is_cashed = "CASHED"
			else: 
				# cashing	
				if c.execute(SQL_COUNT).fetchone()[0] >= DB_MAX_ITEMS:
					# if there no more place we killing oldest item
					is_cropped = "ITEMS IS CROPPED"
					c.execute(SQL_CROP)
					
				output = func(*args, **kwargs)
				c.execute(SQL_DELETE)
				c.execute(SQL_INSERT(output))	
				is_cashed = "NON CASHED"

			#================================================================================

			
			
			conn.commit()
			c.close()
			if debug:
				return output, is_cashed, is_cropped
			else:
				return output

		return wrapper
		
	return decorator
	
#=============================================================================

#
#  if we run this file directly then we start unittests for it.
#
if __name__ == '__main__':
	import unittest
			
	class my_class_test(unittest.TestCase):
		def setUp(self):
			pass

            # checking max intems
		def test_maxitems(self):
            # clearing database
			create_DB(DB_NAME,reset=True)
			
			class test_class:
				@cashe_to_db(timedelta(days=0, hours=0, minutes=20, seconds=0), debug=True)
				def test_f1(self, x, y, name):
					return "%s: %s: %s" % (x, y, name)

                    # testing eviction
			crop0 =  test_class().test_f1('f1_arg0', 'f1_arg0','f1_arg0')[2]
			assert crop0 == None
			
			crop_flag = None
			for number in range(DB_MAX_ITEMS-1):
				crop_flag =  test_class().test_f1(number, number, number)[2]
				
			assert crop_flag == None
			
			crop_flag = test_class().test_f1('f1_arg0', 'f1_arg0','f1_arg2340')[2]
			assert crop_flag == "ITEMS IS CROPPED"

            # testing items time to live
		def test_cashing(self):
			TEST_TIME=5
			
			class test_class:
				@cashe_to_db(timedelta(days=0, hours=0, minutes=0, seconds=TEST_TIME), debug=True)
				def test_f1(self, x, y, name):
					return "%s: %s: %s" % (x, y, name)

				@cashe_to_db(timedelta(days=0, hours=0, minutes=0, seconds=TEST_TIME+10), debug=True)
				def test_f2(self, x, y, name):
					return "%s: %s: %s" % (x, y, name)
					
			
			cashed1 =  test_class().test_f1('f1_arg1', 'f1_arg2','f1_arg3')[1]
			cashed2 =  test_class().test_f2('f2_arg1', 'f2_arg2','f2_arg3')[1]

            # 1st call. nothing is cached
			assert cashed1 == "NON CASHED"
			assert cashed2 == "NON CASHED"

			cashed1 =  test_class().test_f1('f1_arg1', 'f1_arg2','f1_arg3')[1]
			cashed2 =  test_class().test_f2('f2_arg1', 'f2_arg2','f2_arg3')[1]

            # 2st call. it should be cashed now
			assert cashed1 == "CASHED"
			assert cashed2 == "CASHED"
			
			time.sleep(TEST_TIME+1)
			cashed1 =  test_class().test_f1('f1_arg1', 'f1_arg2','f1_arg3')[1]
			cashed2 =  test_class().test_f2('f2_arg1', 'f2_arg2','f2_arg3')[1]

            # after TEST_TIME+1, 1st assert should be non cached
			assert cashed1 == "NON CASHED"
			assert cashed2 == "CASHED"
			
				
			
			
	unittest.main()


