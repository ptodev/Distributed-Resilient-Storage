Distributed-Resilient-Storage
=============================

The aim of this project is to provide easy distributed storage capability
for the DIRAC file catalog using Reed-Solomon erasure coding.

add-ec.py: Uploads a single file to a location on the catalogue.
           If the file is named test.txt it will be turned into 
           multiple smaller files whilch will then be uploaded to 
           a directory called _test.txt in the user defined directory, ie /gridpp/username

get-ec.py: Downloads the erasure coded files. The input could be:
           1) A directory, ie /gridpp/username/_test.txt
           2) The original filename, ie test.txt - then get-ec.py uses metadata to search for the file
