import datetime
import os
import io
import numpy 
import pandas
import csv

datadir = './data'

fieldnames = ['Node ID', 'Sensor ID', 'Type', 'Value']	#We will assign these labels to each field.
sensordict = {	#For the main fluid DAQ data. Translate the plain field identifier to a human readable name.
	'1:0:3': 'RTD1',
	'1:1:3': 'RTD2',
	'1:2:3': 'RTD3',
	'1:3:3': 'RTD4',
	
	'1:4:3': 'TK3LO',
	'1:5:3': 'TK3HI',
	
	'1:6:3': 'PT1',
	'1:7:3': 'PT2',
	'1:8:3': 'PT3',
	'1:9:3': 'PT4',
	'1:10:3': 'PT5',
	'1:11:3': 'PT6',
	
	'1:12:3': 'FQ3',
	'1:13:3': 'FQ2',
	'1:14:3': 'FQ1',
	
	'1:15:3': 'POT1'
}

def getmaindata():
	print(f'======== Loading main controls data')
	if os.path.exists(datadir) and os.path.isdir(datadir):
		fileanddirlist = os.listdir(datadir)

		filelist = []
		for item in fileanddirlist:	#Separate the contents of the data directory. Disregard directories, keep files in list
			if not os.path.isdir(os.path.join(datadir, item)):
				filelist.append(item)
	else:
		print("Data directory does not exist.")
		return None
		
		

	datastr = ''	#Holds all data direct from file

	#filequerylist = []
	#querydate = startdate

	#while(querydate < enddate):
	#	filequerylist.append(querydate.strftime("%Y-%m-%dT%H.csv"))
	#	querydate = querydate + datetime.timedelta(hours=1)

	#print(f'{len(filequerylist)} files of interest:')
	#print(filequerylist)

	print(f'{len(filelist)} files available:')
	#print(filelist)

	for file in filelist:
		#if file in filequerylist:
		path = os.path.join(datadir, file)
		print(f'Loading: {path}')
		datastr += open(path, 'r').read()


	data = []	#Array to hold all the data, in proper order. A dataframe will be made from this later on, but constructing the data as a simple array first is fastest.


	#print('Preparing fields')
	
	
	f = io.StringIO(datastr)
	reader = csv.reader(f, delimiter=',')
	headerfound = False
	datacolumns = 0	#Number of columns found in the header row. Check against data rows.

	header = None	#Header row for pandas dataframe


	rowindex = 0
	
	for row in reader:
		#print(f'Parsing: {row}')
		
		if len(row) < 3:
			rowindex+=1
			continue
			
		if not headerfound:
			if row[1] == 'HEADER':
			
				print(f'Header found at {rowindex}')
				
				#Header row
				headerfound = True
				
				header = ['Time']
				
				for i in range(2, len(row)):
					if len(row[i]) < 1:
						break
						
					indices = row[i].split(':')
					#print(indices)
					nodeid = 1	#node ID not implemented in header row yet.
					sensorid = int(indices[0])
					fieldname = int(indices[1])
					
					headercolumnstring = f'{nodeid}:{sensorid}:{fieldname}'	#Field identifier (plain format, just numbers)
					
					if headercolumnstring in sensordict:
						header.append(f'{sensordict[headercolumnstring]}')	#Append the name from the dictionary instead, not the plain identifier above.
					
					else:
						header.append(headercolumnstring)	#Append the plain identifier string if there is no human-readable name for this field.
				
				datacolumns = len(header)
				print(f'Header: {header}')
				print(f'{datacolumns} columns.')
				
			rowindex+=1
			continue	#Ignore rows before first header line
		
			
			
		if not row[1] == 'DATA':
			rowindex+=1
			continue
		
		date = None
		try:
			datestr = row[0]
			hundreths = datestr[20:]
			microseconds = int(hundreths)*10000
			datestr = datestr[:20]
			datestr += f'{microseconds:06d}'
			date = datetime.datetime.strptime(datestr, "%Y-%m-%dT%H-%M-%S_%f")
		except:
			print("Couldn't parse timestamp:")
			print(row[0])
			rowindex+=1
			continue
			
		#if date < startdate:
		#	rowindex+=1
		#	continue
			
		#if not enddate is None and date > enddate:
		#	break
			
		#print(date)
		
		datarow = [date]
		
		for i in range(2, len(row)):
			if len(row[i]) < 1:
				rowindex+=1
				continue
				
			if row[i] == '' or row[i] is None:
				rowindex+=1
				continue
				
				
			value = row[i]
			if value.isnumeric():
				value = int(value)
				
			elif value == 'err':
				value = None
				
			#print(f'Including elemnt {i} ({row[i]})')
				
			datarow.append(value)
			
		if not len(datarow) == datacolumns:
			#print(f'Data row invalid:')
			#print(datarow)
			pass
		
		else:
			data.append(datarow)
			
			
	print(f'Loaded {len(data)} rows')
		
	rowindex+=1
	#return header, data
	
	#Build dataframe
	
	df = pandas.DataFrame(data)

	#Set header
	df.columns = header

	#print('Sorting...')

	#df['Time'] = df['Time'].astype('datetime64[ms]')
		 
	df = df.set_index('Time').sort_index()


	#Apply sensor calibrations
	df['RTD1'] = df['RTD1']*.1221-24.908
	df['RTD2'] = df['RTD2']*.1221-24.908
	df['RTD3'] = df['RTD3']*.1221-24.908
	df['RTD4'] = df['RTD4']*.1221-24.908

	df['PT1'] = df['PT1']*.061050-12.45
	df['PT2'] = df['PT2']*.061050-12.45
	df['PT3'] = df['PT3']*.061050-12.45
	df['PT4'] = df['PT4']*.061050-12.45
	df['PT5'] = df['PT5']*.061050-12.45
	df['PT6'] = df['PT6']*.061050-12.45

	df['FQ1'] = df[f'FQ1']*3.663-747.3
	df['FQ2'] = df[f'FQ2']*3.663-747.3
	df['FQ3'] = df[f'FQ3']*3.663-747.3

	df['POT1'] = df['POT1']/1023

	return df