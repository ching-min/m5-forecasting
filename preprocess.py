import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import pickle
def time_feature(df, calendar):
	# input feature: calendar information
	# output: sale values of each item in each store
	# df is sales_train_evaluation.csv (pd.DataFrame)
	# calendar is calendar.csv (pd.DataFrame)
	
	df=df.transpose()
	df.columns=[df.loc["item_id"][i]+"_"+df.loc["store_id"][i] for i in range(len(df.columns))]
	df_lst=[]
	for i in range(len(calendar)):
		temp=[calendar["year"][i],calendar["month"][i],float(calendar["date"][i][-2:]),
		float(calendar["d"][i].split("_")[-1]),calendar["wday"][i],calendar["event_name_1"][i],	
		calendar["event_type_1"][i],calendar["event_name_2"][i],calendar["event_type_2"][i],
		calendar["snap_CA"][i],calendar["snap_TX"][i],calendar["snap_WI"][i]]
		try:
			temp.extend(list(df.loc[calendar["d"][i],:]))
			df_lst.append(temp)
		except:
			continu
	df=	pd.DataFrame(df_lst)
	df.columns=list(range(len(df.columns)))
	event={v:i+1 for i,v in enumerate(set(df[5]))}
	event.update({v:float(i+1) for i,v in enumerate(set(df[7]))})
	event[0.0]=0.0
	type={v:i+1 for i,v in enumerate(set(df[6]))}
	type.update({v:float(i+1) for i,v in enumerate(set(df[8]))})
	type[0.0]=0.0
	df.fillna(0.0)
	for i in range(len(df)):
		df[5][i]=event[df[5][i]]
		df[7][i]=event[df[7][i]]
		df[6][i]=type[df[6][i]]
		df[8][i]=type[df[8][i]]
	df.astype(float)
	df.to_csv("df_time.csv",index=False)	
	return df

def statistic_feature(df,seq,column):
	# df: (pd.DataFrame)
	# seq: (int) how many days of data use in statistic_%s
	# column: (str) which column use for stastistic
	df=df.transpose()
	df.columns=[df.loc["item_id"][i]+"_"+df.loc["store_id"][i] for i in range(len(df.columns))]
	df=df.iloc[6:]
	df.index=list(range(len(df)))
	try:
		d=df[[column]]
	except:
		raise "column error %s is not in data".format(column)
	min_value=[np.nan]*(seq)
	max_value=[np.nan]*(seq)
	mean_value=[np.nan]*(seq)
	for i in range(seq,len(df)):
		temp=df[column][i-seq:i]
		min_value.append(min(temp))
		max_value.append(max(temp))
		mean_value.append(sum(temp)/seq)
	d["min_value"]=min_value
	d["max_value"]=max_value
	d["mean_value"]=mean_value
	d.to_csv("statistic_{}.csv".format(column),index=False)
	return d	

def seq(data,seq,offset,column=None):
	# data (pd.DataFrame)
	# length (int) how many days of data use in statistic
	# offset (int) start from which day (refer to d_n in calendar, if start from d_n, offset=n-1)
	# column (list of str): specific columns. Default is None, means whole columns
	data=data.transpose()
	data.columns=[data.loc["item_id"][i]+"_"+data.loc["store_id"][i] for i in range(len(data.columns))]
	data=data.iloc[6:]
	data.index=list(range(len(data)))
	X=[]
	y=[]
	if column is None:
		for i in range(offset,len(data)-seq):
			X.append(np.array(data.iloc[i:i+seq,:]))
			y.append(np.array(data.iloc[i+seq,:]))
	else:
		d=data[column]
		for i in range(offset,len(data)-seq):
			X.append(np.array(d.iloc[i:i+seq]))
			y.append(np.array(d.iloc[i+seq]))	
	del data
	if column==None: column="all"
	with open('seq_{}_X.pickle'.format(column), 'wb') as fp:
		pickle.dump(X, fp)
		fp.close()
	with open('seq_{}_y.pickle'.format(column), 'wb') as fp:
		pickle.dump(y, fp)
		fp.close()
	return np.array(X),np.array(y)

def statistic_feature_multiple_days(df,seq,column):
	# df: (pd.DataFrame)
	# seq: (int) how many days of data use in statistic
	# column: (str) which column use for stastistic
	df=df.transpose()
	df.columns=[df.loc["item_id"][i]+"_"+df.loc["store_id"][i] for i in range(len(df.columns))]
	df=df.iloc[6:]
	df.index=list(range(len(df)))
	if column not in df:
		raise "column error %s is not in data".format(column)
	d=pd.DataFrame()
	min_value=[]#[np.nan]*(seq)
	max_value=[]#[np.nan]*(seq)
	mean_value=[]#[np.nan]*(seq)
	sales=[]#[np.nan]
	for i in range(seq,len(df)):
		temp=df[column][i-seq:i]
		min_value.append(min(temp))
		max_value.append(max(temp))
		mean_value.append(sum(temp)/seq)
		sales.append(list(df[column][i:i+28]))
	d["min_value"]=min_value
	d["max_value"]=max_value
	d["mean_value"]=mean_value
	d.to_csv("stat_{}_{}.csv".format(seq,column),index=False)
	with open('stat_sales_{}.pickle'.format(column), 'wb') as fp:
		pickle.dump(sales, fp)
		fp.close()
	return d, sales


def seq_multiple_days(data,seq,offset,column=None):
	# data (pd.DataFrame)
	# length (int) how many days of data use in statistic
	# offset (int) start from which day (refer to d_n in calendar, if start from d_n, offset=n-1)
	# column (list of str): specific columns. Default is None, means whole columns
	X=[]
	y=[]
	data=data.transpose()
	data.columns=[data.loc["item_id"][i]+"_"+data.loc["store_id"][i] for i in range(len(data.columns))]
	data=data.iloc[6:]
	data.index=list(range(len(data)))
	if column is None:
		
		for i in range(offset,len(data)-seq-28):
			X.append(np.array(data.iloc[i:i+seq,:]))
			y.append(np.array(data.iloc[i+seq:i+seq+28,:]))
	else:
		d=data[[column]]
		for i in range(offset,len(data)-seq-28):
			X.append(np.array(d.iloc[i:i+seq,:]))
			y.append(np.array(d.iloc[i+seq:i+seq+28,:]))	
	del data
	if column==None: column="all"
	with open('seq_m_{}_X.pickle'.format(column), 'wb') as fp:
		pickle.dump(X, fp)
		fp.close()
	with open('seq_m_{}_y.pickle'.format(column), 'wb') as fp:
		pickle.dump(y, fp)
		fp.close()
	return np.array(X),np.array(y)

def slice(data,length=28,feature_columns=[0],prefix=""): # to 3d
    X=[]
    y=[] 
    if type(data) is not tuple:
        for i in range(0,len(data)-length):
        	X.append(np.array(data.iloc[i:i+length,feature_columns]))
        	y.append(np.array(data.iloc[i+length,-1:]))
    else:
        for i in range(0,len(data[0])-length):
        	a=data[0]
        	b=data[1]			
        	X.append(np.array(data[0][i:i+length]))
        	y.append(np.array(data[1][i+length]))
    del data
    with open('{}_X.pickle'.format(prefix), 'wb') as fp:
        pickle.dump(X, fp)
        fp.close()
    with open('{}_y.pickle'.format(prefix), 'wb') as fp:
        pickle.dump(y, fp)
        fp.close()
    return np.array(X),np.array(y)

def split(df,valid,test):
	# df (pd.DataFrame)
	# valid (int or float): (int) num of row for valid dataset (float) proportion of df, must<=1.0
	# test (int or float): (int) num of row for test dataset (float) proportion of df, must<=1.0
	
	if (type(valid)!=int or type(valid)!=float):
		raise "valid must be int or float"
	if type(valid)==float and valid>1:
		raise "the float valid must <=1.0"
		
	if (type(test)!=int or type(test)!=float):
		raise "test must be int or float"
	if type(test)==float and test>1:
		raise "the float test must <=1.0"
	
	testset=None
	valset=None
	
	if type(test)==float: testset=df.iloc[int(len(df)*(1-test)):]
	else: testset=df.iloc[len(df)-test:]
	testset.reset_index(inplace=True)
	
	if type(valid)==float: valset=df.iloc[len(df)-len(test)-int(len(df)*valid):len(df)-len(test)]
	else: valset=df.iloc[len(df)-len(test)-valid:len(df)-len(test)]
	valset.reset_index(inplace=True)
	
	return [df.iloc[:len(df)-len(valset)-len(testset)]]

	
