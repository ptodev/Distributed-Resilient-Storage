#!/usr/bin/env python
'''
--input_file could be /gridpp/ptodev/_Penguins.jpg/ or just Penguins.jpg
$ python get-ec.py --temporary_directory /home/paulin/Distributed-Resilient-Storage/fec/ 
                   --output_directory /home/paulin/Desktop 
                   --input_file /gridpp/ptodev/_Penguins.jpg/ 
                   --processes 1
'''

import zfec, sys, os, glob, multiprocessing, time, itertools, math
from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script

class Params:
	def __init__(self):
		self.TempDir = '/tmp/'
		self.Processes = int(math.ceil(multiprocessing.cpu_count()/2.0))

	def setTempDir(self, value):
		self.TempDir = value
		return S_OK()
	def getTempDir(self):
		return self.TempDir

	def setOutDir(self, value):
		self.OutDir = value
		return S_OK()
	def getOutDir(self):
		return self.OutDir

	def setInputFile(self, value):
		self.InputFile = value
		return S_OK()
	def getInputFile(self):
		return self.InputFile

	def setProcesses(self, value):
		self.Processes = value
		return S_OK()
	def getProcesses(self):
		return self.Processes

# Instantiate the params class
cliParams = Params()

# Register accepted switches and their callbacks
Script.registerSwitch("td:", "temporary_directory=", "Direcory where the files will be downloaded.", cliParams.setTempDir)
Script.registerSwitch("od:", "output_directory=", "Location of the reconstructed file.", cliParams.setOutDir)
Script.registerSwitch("i:", "input_file=", "Name/LFN of the file to be downloaded.", cliParams.setInputFile)
Script.registerSwitch("pr:", "processes=", "Number of processes to run concurrently.", cliParams.setProcesses)

# Parse the command line and initialize DIRAC
Script.parseCommandLine(ignoreErrors = False)
switches = dict(Script.getUnprocessedSwitches())

# Get the list of services
servicesList = Script.getPositionalArgs()

import DIRAC.Interfaces.API.Dirac as dirac_api
import DIRAC.Resources.Catalog.FileCatalogClient as FCC

class Counter(object):
	# A counter class for easier incrementing
	def __init__(self, initval, manager):
		self.val = manager.Value('i', initval)
		self.lock = manager.Lock()

	def increment(self):
		 with self.lock:
			self.val.value += 1
			return self.val.value

	def value(self):
		with self.lock:
			return self.val.value

def getFileCC(ec_file, local_dir):
	# A function to download files via LFNs
	dirac = dirac_api.Dirac()
	time1 = time.time()

	output = dirac.getFile(ec_file, local_dir)

	time2 = time.time()
	local_counter = counter.increment()
	
	result_queue.put([local_counter, ec_file, str(time2-time1), output])

def getFileCC_func(args):
	# A function needed to unpack the two arguments
	return getFileCC(*args)

def sanitize_directory(input_str):
	# Add a / to the end of the string if there isn't one
	if(input_str[-1] != '/'):
		input_str = input_str + '/'
	return input_str

def sanitize_remote_directory(input_str):
	if(input_str[0] == '/' and input_str[-1] != '/'):
		input_str = input_str + '/'
	return input_str

def sanitizeProcesses(processes):
	if(processes <= 0):
		return 1
	return processes

if __name__ == '__main__':
	input_str = cliParams.getInputFile()
	local_dir = cliParams.getTempDir()
	output_dir = cliParams.getOutDir()
	processes = int(cliParams.getProcesses())

	ec_files = []
	fc = FCC.FileCatalogClient()

	#######################################################################
	########################### INPUT SANITIZING ##########################
	#######################################################################
	input_str = sanitize_remote_directory(input_str)
	local_dir = sanitize_directory(local_dir)
	output_dir = sanitize_directory(output_dir)
	processes = sanitizeProcesses(processes)

	#######################################################################
	############### GENERATE A LIST WITH ERASURE CODED FILES ##############
	#######################################################################
	print 'Generating a list with LFNs... ',
	# The input is a directory if it starts with /
	# In that case use an LFN
	if(input_str[0] == '/'):
		output = fc.listDirectory(input_str)
		if(not output['Value']['Successful']):
			print 'ERROR: ' + output['Value']['Failed'][input_str]
			sys.exit()
		ec_files = output['Value']['Successful'][input_str[:-1]]['Files'].keys()
	# If the input does not start with / it is assumed to be a filename
	# In that case search for the file using metadata
	else:
		output = fc.findFilesByMetadata({'EC_FILE': input_str}, '/')
		if(output['Value'] == []):
			print 'No such file was found!'
			sys.exit()
		ec_files = output['Value']
	
	# Check if the metadata has the right filename
	ec_files_tmp = []
	for ec_file in ec_files:
		# Check if the metadata of the file matches the given filename
		output = fc.getFileUserMetadata(ec_file)
		if(output['Value']['EC_FILE'] == ec_file.split('/')[-2][1:]):
			ec_files_tmp.append(ec_file)
	ec_files = ec_files_tmp

	print 'done!'

	#######################################################################
	#################### FIND THE NUMBER OF NEEDED FILES ##################
	#######################################################################
	output = fc.getFileUserMetadata(ec_files[0])
	if(output['OK']):
		number_of_needed_files = int(output['Value']['SPLIT'])
	else:
		print 'ERROR: Unable to exctract SPLIT metadata value for file ' + ec_files[0]
		sys.exit()
	print 'Number of files needed for reconstruction: ' + str(number_of_needed_files)

	#######################################################################
	################### START A MULTIPROCESSING DOWNLOAD ##################
	#######################################################################
	sharefiles = []
	print 'Downloading files to ' + local_dir
	successful_downloads = 0

	if(processes == 1):
		dirac = dirac_api.Dirac()
		# A counter showing the number of the current downloded file
		i = 1
		
		time1 = time.time()

		for ec_file in ec_files:
			# Counstruct a counter, i.e. [2/15] which
			# shows which file i being downloaded
			counter = '[' + str(i) + '/' + str(number_of_needed_files) + '] '
			i += 1

			print counter +'Downloading ' + ec_file + '... ',
			# Download the file from the grid
			time_file_1 = time.time()
			output = dirac.getFile(ec_file, local_dir)
			time_file_2 = time.time()
			# See if the download was successful
			if(not output['OK']):
				print ''
				print 'ERROR: ' + output['Message']
			else:
				successful_downloads += 1
				print 'done in ' + str(time_file_2-time_file_1)[:4] + ' seconds!'
				# Get the local adress of the file from the dirac output to be decoded later
				sharefiles.append(output['Value']['Successful'][ec_file])

			if(successful_downloads == number_of_needed_files):
				print 'The number of necessary files has been reached.'
				print 'Terminating download... ',
				break
		print 'done!'
		time2 = time.time()
		print "Total time for download: " + str(time2-time1)[:4] + ' seconds!'
	else:
		manager = multiprocessing.Manager()
		# The counter counts how many downloads have been completed
		counter = Counter(0, manager)
		# The queue is for the threads to give real time information
		# to the main function about the downloads
		result_queue = manager.Queue()
		# The Pool() class takes an intit argument for how many
		# processes th pool should have
		pool = multiprocessing.Pool(4)
		# This is the timer for all the downloads
		time1 = time.time()
		# map_async() is different from map(), because it doesn't
		# block the main function
		pool.map_async(getFileCC_func, itertools.izip(ec_files, itertools.repeat(local_dir)))
		# queue_results contains lists like:
		# [number_of_download, name_of_file, elapsed_time]
		queue_results = []

		# Get results from the queue
		while(True):
			queue_results.append(result_queue.get())
			print '[' + str(queue_results[-1][0]) + '/' + str(number_of_needed_files) + ']',
			print 'Downloaded ' + queue_results[-1][1] + ' in ' + str(queue_results[-1][2])[:4] + ' seconds!'
			
			# See if the download was successful
			if(not queue_results[-1][3]['OK']):
				print ''
				print 'ERROR: ' + output['Message']
			else:
				successful_downloads += 1
				# Get the local adress of the file from the dirac output to be decoded later
				ec_file = queue_results[-1][1]
				sharefiles.append(queue_results[-1][3]['Value']['Successful'][ec_file])

			if(successful_downloads == number_of_needed_files):
				print 'The number of necessary files has been reached.'
				print 'Terminating download... ',
				# terminate() has to be followed by join() in order to give the background machinery
				# time to update the status of the object to reflect the termination.
				pool.terminate()
				pool.join()
				print 'done!'
				break
		time2 = time.time()
		print 'Total time for download: ' + str(time2-time1)[:4] + ' seconds!'

	#######################################################################
	###################### GENERATE THE ORIGINAL FILE #####################
	#######################################################################
	print 'Beginning decoding... ',
	# Check if the input is a directory or a file name
	if(input_str[0] == '/'):
		# This code could convert input_str = "/gridpp/ptodev/_Penguins.jpg/"
		# with input_str.split('/')[-2][1:] into Penguins.jpg (The filename)
		zfec_ouput_file_name = output_dir + input_str.split('/')[-2][1:]
	else:
		# If the input string does not start with / we assume that it is the filename
		zfec_ouput_file_name = output_dir + input_str

	# Create a file object where the original will be recreated
	zfec_ouput_file = open(zfec_ouput_file_name, 'wb')

	# Craete file objects for the erasure coded segments
	zfec_input_files = []
	for sharefile in sharefiles:
	    zfec_input_files.append(open(sharefile, 'rb'))

	# Decode the file
	zfec.filefec.decode_from_files(zfec_ouput_file, zfec_input_files)
	print 'done!'

	print 'The restored original file has been is located at: ' + zfec_ouput_file_name

	#######################################################################
	################# CLEAN THE LOCAL ERASURE CODED FILES #################
	#######################################################################
	print "Cleaning up the local EC files... ",
	files = glob.glob(local_dir + '*')
	for f in files:
	    os.remove(f)
	print "done!"