#!/usr/bin/env python
'''
Example:
$ python add-ec.py --total 15 
                   --split 10 
                   --input_file Penguins.jpg 
                   --temporary_directory /home/paulin/Distributed-Resilient-Storage/fec/ 
                   --remote_directory /gridpp/ptodev/ 
                   --se_list se_list.txt
                   --processes 4
'''

#######################################################################
######################### GET INPUT AGRUMENTS #########################
#######################################################################
import zfec, sys, os, glob, multiprocessing, time, itertools, math, subprocess
from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script

class Params:
    def __init__(self):
        self.SEList = ''
        self.TempDir = '/tmp/'
        self.Processes = int(math.ceil(multiprocessing.cpu_count()/2.0))

    def setSplit(self, value):
        self.Split = value
        return S_OK()
    def getSplit(self):
        return self.Split

    def setTotal(self, value):
        self.Total = value
        return S_OK()
    def getTotal(self):
        return self.Total

    def setRemDir(self, value):
        self.RemDir = value
        return S_OK()
    def getRemDir(self):
        return self.RemDir

    def setTempDir(self, value):
        self.TempDir = value
        return S_OK()
    def getTempDir(self):
        return self.TempDir

    def setInputFile(self, value):
        self.InputFile = value
        return S_OK()
    def getInputFile(self):
        return self.InputFile

    def setSEList(self, value):
        self.SEList = value
        return S_OK()
    def getSEList(self):
        return self.SEList

    def setProcesses(self, value):
        self.Processes = value
        return S_OK()
    def getProcesses(self):
        return self.Processes

# Instantiate the params class
cliParams = Params()

# Register accepted switches and their callbacks
Script.registerSwitch("sp:", "split=", "Number of files the original will be split into.", cliParams.setSplit)
Script.registerSwitch("t:", "total=", "Total number of files (split + EC generated ones).", cliParams.setTotal)
Script.registerSwitch("rd:", "remote_directory=", "Direcory where the files will be uploaded.", cliParams.setRemDir)
Script.registerSwitch("td:", "temporary_directory=", "Location of the temporary files until they are uploaded.", cliParams.setTempDir)
Script.registerSwitch("i:", "input_file=", "Location of the file to be uploaded.", cliParams.setInputFile)
Script.registerSwitch("se:", "se_list=", "A file with names of usable SEs.", cliParams.setSEList)
Script.registerSwitch("pr:", "processes=", "Number of processes to run concurrently.", cliParams.setProcesses)

# Parse the command line and initialize DIRAC
Script.parseCommandLine(ignoreErrors = False)
switches = dict(Script.getUnprocessedSwitches())

# Get the list of services
servicesList = Script.getPositionalArgs()

import DIRAC.Interfaces.API.Dirac as dirac_api
import DIRAC.Resources.Catalog.FileCatalogClient as FCC

def get_se_status(testdir):
    # A function that tests the SEs visible by dirac-dms-show-se-status
    # by adding a file to them to see if they work.

    # Name of the file on the SE
    testfile_remote = '1'

    # Create a file to upload for testing
    testfile_local = '1'
    while(True):
        if(os.path.isfile(testfile_local)):
            testfile_local = str(int(testfile_local)+1)
        else:
            break
    local_file = open(testfile_local, 'w')
    local_file.write('A file for testing whether an SE works.')
    local_file.close()

    ####### GET A LIST OF THE SEs #########
    se_stat = subprocess.Popen("dirac-dms-show-se-status", shell=True, stdout=subprocess.PIPE).stdout.read()

    # Split into lines
    se_stat = se_stat.split('\n')

    # Clean unnecessary lines
    se_stat = se_stat[2:-1]

    # Split each line into strings
    for se_index in range(len(se_stat)):
        se_stat[se_index] = se_stat[se_index].split()

    # Create a list with the names of the SEs
    ses = []
    for se in se_stat:
        ses.append(se[0])

    ######### TEST WHICH SEs WORK #########
    dirac = dirac_api.Dirac()

    ses_not_working = []
    ses_working = []
    small_log = ''
    existing_file_error = "{'Message': 'putAndRegister: \
    This file GUID already exists for another file. \
    Please remove it and try again. True', 'OK': False}"

    # This is for surpressing any print statements from dirac.addFile()
    old_stdout = sys.stdout
    sys.stdout = open(os.devnull, 'w')

    for se in ses:
        while (True):
            # Try adding a test file
            output = dirac.addFile(testdir+testfile_remote, testfile_local, se)
            if(str(output) == existing_file_error):
                testfile_remote = str(int(testfile_remote)+1)
            else:
                # Remove the test file
                dirac.removeFile(testdir+testfile_remote)
                break
        if(not output['OK']):
            ses_not_working.append(se)
        else:
            ses_working.append(se)

    sys.stdout.close()
    sys.stdout = old_stdout

    try:
        os.remove(testfile_local)
    except OSError as e:
        print "Failed to remove file" + testfile_local + " with:", e.strerror
        print "Error code:", e.code

    return ses_working, ses_not_working


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

def addFileCC(remote_path, local_path, se):
    # A function to upload files via LFNs
    dirac = dirac_api.Dirac()
    time1 = time.time()

    output = dirac.addFile(remote_path, local_path, se)

    time2 = time.time()
    local_counter = counter.increment()

    result_queue.put([local_counter, remote_path, se, str(time2-time1), output])

def addFileCC_func(args):
    # A function needed to unpack the two arguments
    return addFileCC(*args)

def get_concurrent_parameters(total, loc_dir, rem_ec_dir, ses_working):
    # Set a counter for the current SE
    i = 0
    # Set a counter for the current file
    j = 0

    local_paths = []
    remote_paths = []
    ses = []

    while(True):
        # Break the loop when all the arrays are completed
        if(j == total):
            break
        # Go back to the first SE when you run out of them
        if(i == len(ses_working)):
            i = 0
        # Get the file numbering right, like "Penguins.jpg.01_15.fec" ...
        if(j < 10):
            num = '0'+str(j)
        # ...and for such as "Penguins.jpg.13_15.fec"
        else:
            num = str(j)

        local_path = loc_dir + ec_file.name + "." + num + "_" + str(total) + ".fec"
        local_paths.append(local_path)

        remote_path = rem_ec_dir + ec_file.name + "." + num + "_" + str(total) + ".fec"
        remote_paths.append(remote_path)

        ses.append(ses_working[i])

        i += 1
        j += 1

    return (remote_paths, local_paths, ses)

def sanitize_directory(input_str):
    # Add a / to the end of the string if there isn't one
    if(input_str[-1] != '/'):
        input_str = input_str + '/'
    return input_str

def sanitizeProcesses(processes):
    if(processes <= 0):
        return 1
    return processes

if __name__ == '__main__':
    split = int(cliParams.getSplit())
    total = int(cliParams.getTotal())
    rem_dir = cliParams.getRemDir()
    loc_dir = cliParams.getLocDir()
    ec_file = open(cliParams.getInputFile(), 'r')
    if(cliParams.getSEList() != ''):
        se_list = open(cliParams.getSEList())
    else:
        se_list = cliParams.getSEList()
    processes = int(cliParams.getProcesses())

    #######################################################################
    ########################### INPUT SANITIZING ##########################
    #######################################################################
    rem_dir = sanitize_directory(rem_dir)
    loc_dir = sanitize_directory(loc_dir)
    processes = sanitizeProcesses(processes)

    #######################################################################
    ######################## CHECK SE AVAILABILITY ########################
    #######################################################################
    # Import a file with SE names if it is given
    if(se_list):
        print "Importing a list with SEs...",
        ses_working = se_list.readlines()
        for i in range(len(ses_working)):
            ses_working[i] = ses_working[i].strip()
    # If there is no list with SEs, check which ones are available with the se_check.py script
    else:
        print "Checking SE availability...",
        res = get_se_status(rem_dir)
        ses_working = res[0]
        ses_not_working = res[1]

    # Make sure the list with working SEs is not empty
    if not ses_working:
        print "None of the SEs work!"
        sys.exit()

    print 'done!'

    #######################################################################
    ######################### DO ERASURE ENCODING #########################
    #######################################################################
    # Clean the local fec folder
    print "Cleaning up the ./fec/ folder... ",
    files = glob.glob(loc_dir + '*')
    for f in files:
        os.remove(f)
    print "done!"

    print "Starting erasure encoding...",
    # Find the size of the file
    ec_file.seek(0, 2)
    fsize = ec_file.tell()
    ec_file.seek(0, 0)
    # Split the file and do erasure encosing
    zfec.filefec.encode_to_files(ec_file, fsize, loc_dir, ec_file.name, split, total, overwrite=False, verbose=False)

    print 'done!'

    #######################################################################
    ########################### CREATE DIRECTORY ##########################
    #######################################################################
    fc = FCC.FileCatalogClient()
    rem_ec_dir = rem_dir + '_' + ec_file.name + '/'

    # See if the directory exists
    output = fc.isDirectory(rem_ec_dir)
    # Here we need the [:-1], because DIRAC return the name without a "/"
    if(output['Value']['Successful'][rem_ec_dir[:-1]] == True):
        print "ERROR! The remote directory already exists!"
        while(True):
            prompt = raw_input("Would you like to remove " + rem_ec_dir + "? (yes/no):")

            if(prompt == 'yes'):
                print 'Removing directory' + rem_ec_dir + '... ',
                
                # List the files in the directory
                output_ls = fc.listDirectory(rem_ec_dir)
                
                # Check if the directory has subdirectories - if it does, it cannot be deleted
                if(output_ls['Value']['Successful'][rem_ec_dir[:-1]]['SubDirs'] != {}):
                    print 'ERROR: Cannot delete a directory if it has subdirectories!'
                    sys.exit()
                
                # loop and delete with fc.removeFile()
                output_ls = output_ls['Value']['Successful'][rem_ec_dir[:-1]]['Files'].keys()
                for file_to_be_deleted in output_ls:
                    fc.removeFile(file_to_be_deleted)
                
                # Remove the empty directory
                output_rm = fc.removeDirectory(rem_ec_dir)
                # Check if the directory has been removed successfully
                if(not output_rm['Value']['Successful']):
                    print 'ERROR: ' + output_rm['Value']['Failed'][rem_ec_dir[:-1]]
                else:
                    print 'done!'
                    break

            elif(prompt == 'no'):
                print 'The directory was not removed. Exiting program...'
                sys.exit()
            
            else:
                print 'Please type "yes" or "no"!'

    # Create the directory
    print 'Creating a directory "' + rem_ec_dir + '"... ',
    fc.createDirectory(rem_ec_dir)
    print 'done!'

    #######################################################################
    ############################# UPLOAD FILES ############################
    #######################################################################
    (remote_paths, local_paths, ses) = get_concurrent_parameters(total, loc_dir, rem_ec_dir, ses_working)
    print 'Uploading files to ' + rem_ec_dir
    
    if(processes == 1):
        print 'A serial download method will be used.'
        time1 = time.time()
        dirac = dirac_api.Dirac()

        for i in range(len(ses)):
            # Print a counter showing the number of the
            # downloaded file, like [3/10]
            print '[' + str(i+1) + '/' + str(total) + ']',

            print 'Uploading ' + local_paths[i] + ' on ' + ses[i] + '...',
            time_single_1 = time.time()
            # Upload the files on the grid
            output = dirac.addFile(remote_paths[i], local_paths[i], ses[i])
            # See if the upload was successful
            time_single_2 = time.time()
            if(output['OK'] == True):
                print 'done in ' + str(time_single_2-time_single_1)[:4] + ' seconds!'
            else:
                print 'ERROR DETECTED:'
                print 'ERROR: ' + output['Message']
        
        time2 = time.time()
        print 'Total time for upload: ' + str(time2-time1)[:4]
    else:
        print 'A multiprocessing download method will be used!'
        print 'Number of cores on the computer: ' + str(multiprocessing.cpu_count())
        print 'Number of processes used: ' + str(processes)

        # The Pool() class takes an intit argument for how many
        # processes the pool should have
        manager = multiprocessing.Manager()
        result_queue = manager.Queue()
        counter = Counter(0, manager)

        pool = multiprocessing.Pool(4)

        time1 = time.time()
        pool.map_async(addFileCC_func, itertools.izip(remote_paths, local_paths, ses))

        queue_results = []
        
        # Get results from the queue
        while(True):
            queue_results.append(result_queue.get())
            # queue_results[-1][4] contains the output from dirac.addFile()
            if(queue_results[-1][4]['OK'] == False):
                print 'ERROR in uploading file ' + queue_results[-1][1] + ':'
                print queue_results[-1][4]['Message']
            else:
                print '[' + str(queue_results[-1][0]) + '/' + str(len(ses)) + ']',
                print 'File ' + queue_results[-1][1] + ' uploaded on ' + queue_results[-1][2] + ' in ' + str(queue_results[-1][3])[:4] + ' seconds!'
            if(len(queue_results) == len(ses)):
                pool.close()
                pool.join()
                break

        time2 = time.time()
        print 'Total time for upload: ' + str(time2-time1)[:4]

    #######################################################################
    ################# CLEAN THE LOCAL ERASURE CODED FILES #################
    #######################################################################
    print "Cleaning up the local EC files... ",
    files = glob.glob(loc_dir + '*')
    for f in files:
        os.remove(f)
    print "done!"

    #######################################################################
    ############################# ADD METADATA ############################
    #######################################################################
    print 'Adding metadata... ',
    
    metadata = (('EC_FILE', ec_file.name), ('EC_VERSION', '0.1'), ('TOTAL', total), ('SPLIT', split))
    
    for i in range(len(remote_paths)):
        for j in range(len(metadata)):
            output = fc.setMetadata(remote_paths[i], {metadata[j][0]: metadata[j][1]})
            if(output['OK'] == False):
                print 'ERROR! Could not add metadata for ' + remote_paths[i] + ':'
                print output['Message']

    print 'done!'