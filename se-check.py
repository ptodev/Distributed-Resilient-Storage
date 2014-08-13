#!/usr/bin/env python
'''
A program to test chich SEs given in dirac-dms-show-se-status are available for writing.

Example:
$ python se-check-cli.py --remote_directory /gridpp/ptodev/ --verbose
'''
from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script

class Params:
    def __init__(self):
        self.Verbose = False

    def setRemDir(self, value):
        self.RemDir = value
        return S_OK()
    def getRemDir(self):
        return self.RemDir

    def setVerbose(self, value):
        self.Verbose = True
        return S_OK()
    def getVerbose(self):
        return self.Verbose

# Instantiate the params class
cliParams = Params()

# Register accepted switches and their callbacks
Script.registerSwitch("rd:", "remote_directory=", "Directory on the catalogue where a test will be uploaded.", cliParams.setRemDir)
Script.registerSwitch("v", "verbose", "Verbose mode - prints more information on the screen.", cliParams.setVerbose)

# Parse the command line and initialize DIRAC
Script.parseCommandLine(ignoreErrors = False)
switches = dict(Script.getUnprocessedSwitches())

# Get the list of services
servicesList = Script.getPositionalArgs()

import DIRAC.Interfaces.API.Dirac as dirac_api
import subprocess as sp
import sys, os

# Get command line arguments
testdir = cliParams.getRemDir()
verbose = cliParams.getVerbose()

dirac = dirac_api.Dirac()

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

############################## GET A LIST OF THE SEs ###################################
print 'Getting a list of the SEs...'
se_stat = sp.Popen("dirac-dms-show-se-status", shell=True, stdout=sp.PIPE).stdout.read()

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

# Print the SEs
if(verbose):
    print 'Found SEs:'
    for se in ses:
        print se

############################### TEST WHICH SEs WORK ####################################
print ''
print 'Testing the SEs...'

ses_not_working = []
ses_working = []
small_log = ''
existing_file_error = "{'Message': 'putAndRegister: This file GUID already exists for another file. Please remove it and try again. True', 'OK': False}"

if(not verbose):
    old_stdout = sys.stdout
    sys.stdout = open(os.devnull, 'w')
else:
   print '**************  BEGIN LOG  **************'

for se in ses:
    print '**************  TESTING ' + se + '  **************'
    while (True):
        # Try adding a test file
        output = dirac.addFile(testdir+testfile_remote, testfile_local, se, printOutput=False)
        if(str(output) == existing_file_error):
            testfile_remote = str(int(testfile_remote)+1)
        else:
            # Remove the test file
            dirac.removeFile(testdir+testfile_remote)
            break
    # For verbose mode only
    if(verbose):
        print '\n' + se + ':   ' + str(output) + '\n'
    if(not output['OK']):
        ses_not_working.append(se)
    else:
        ses_working.append(se)

os.remove(testfile_local)

if(not verbose):
    sys.stdout.close()
    sys.stdout = old_stdout

######################## PRINT THE WORKING & NONWORKING SEs ############################
print '***********  NOT WORKING SEs  ***********'
for se in ses_not_working:
    print se
print ''

print '*************  WORKING SEs  *************'
for se in ses_working:
    print se
print ''
