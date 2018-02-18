"""
	Tools to use in Jupyter notebooks.

	Mostly for sanity checking small data structures.
"""


from IPython.core.display import display, HTML
import pandas as pd
import pyspark.sql
import numpy as np


def setup_notebook():

	display(HTML("<style>.container { width:99% !important; }</style>"))

	pd.set_option('display.max_columns', None)

	pd.options.display.max_columns = None

	pd.options.display.max_rows = None

	pd.options.display.max_colwidth = 1009


def peek(obj, n=3):

	if type(obj) == pd.core.frame.DataFrame:

		return peek_pandas(obj, n)

	elif type(obj) == list:

		return peek_list(obj, n)

	elif type(obj) == dict:

		return peek_dict(obj, n)

	else:

		return peek_spark(obj, n)


def peek_pandas(df, n=3):

	return df.head(n)


def peek_spark(df, n=3):

	return df.limit(n).toPandas()


def peek_dict(dictionary, n=3):

	return pd.DataFrame(
		[(key, val) for key,val in dictionary.iteritems()],
		columns=['key', 'value'])


def peek_list(array, n=3):

	return array[:n]


def count(df):

	if type(df) == pd.core.frame.DataFrame:

		return len(df)

	else:

		return df.count()


def size(obj):

	if type(obj) == pd.core.frame.DataFrame:

		print '{} rows, {} columns'.format(obj.shape[0], obj.shape[1])

	elif type(obj) == pyspark.sql.dataframe.DataFrame:

		print obj.count()

	elif type(obj) == dict:

		print 'There are {} keys'.format(len(obj))

	elif type(obj) == list:

		print len(obj)

	elif type(obj) == np.ndarray:

		print '{} rows, {} columns'.format(obj.shape[0], obj.shape[1])