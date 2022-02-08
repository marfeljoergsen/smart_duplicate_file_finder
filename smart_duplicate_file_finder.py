#!/usr/bin/env python3

import sys
import subprocess
import inspect
from os import path, SEEK_SET, SEEK_END
import math
import argparse
import hashlib
import numpy as np  # for random number generation


# File to compare if md5sum is the same for similar files on 8 vs 12 TB disk...

class Data:
    def __init__(self):
        print('RUNNING: ' + inspect.stack()[0][3] + '()')
        self.line1 = 0
        self.line2 = 0
        self.fileNotExistCount = 0
        #self.fileNotExistLimit = 10
        self.fileNotExistLimit = 9e99
        if True:
        #if False:
            print(" *** TESTING (on laptop) ***")
            self.file1 = "random_data_RO_SORTED.txt_"
            self.file2 = "8tb_sdd_from_syn_mdadm_decrypted_SORTED.txt_"
            self.file1root = "/home/martin/Downloads/DELETE_THIS/ejdmægler-RAW/"
            self.file2root = "/home/martin/Downloads/DELETE_THIS/ejdmægler-RAW2/"
        else:
            print(" *** REAL PRODUCTION (on server) ***")
            self.file1 = "random_data_RO_SORTED.txt"
            self.file2 = "8tb_sdd_from_syn_mdadm_decrypted_SORTED.txt"
            self.file1root = "/mnt/hugeData"
            self.file2root = "/mnt/hugeData"

        # Open file 1 first, then file 2 - read data into buffers:
        try:
            with open(self.file1, 'r') as f:
                self.data1 = f.read().splitlines()  # Read whole file into buffer
        except FileNotFoundError as e:
            print("ERROR: " + e.strerror + ": \"" + e.filename + "\" (please fix: self.file1)")
            sys.exit(1)

        try:
            with open(self.file2, 'r') as f:
                self.data2 = f.read().splitlines()  # Read whole file into buffer
        except FileNotFoundError as e:
            print("ERROR: " + e.strerror + ": \"" + e.filename + "\" (please fix: self.file2)")
            sys.exit(1)

        # Print a bit of data:
        self.chunkData = (-1, '/dev/null')
        print("File 1 name and size: \"" + self.file1 + "\" (size: " + str(len(self.data1)) + " lines )")
        print("File 2 name and size: \"" + self.file2 + "\" (size: " + str(len(self.data2)) + " lines )")
        print(" ------------------------------------ ")
        print(" ")


    def findDuplicatesInSingleBuffer(self, b, *args, **kwargs):
        if len(args) == 0:
            minSz = -1  # not 0, use -1 to also get files with size 0...
        else:
            minSz = args[0]
        # for ar in args:
        #    print(ar)
        #       minSz):
        print('RUNNING: ' + inspect.stack()[0][3] + '()')
        print(' ')
        if b == 1:
            data = self.data1
            filelist = self.file1
            runMD5path = self.file1root
        elif b == 2:
            data = self.data2
            filelist = self.file2
            runMD5path = self.file2root
        else:
            print("Invalid buffer! Program cannot continue")
            sys.exit(1)

        print("Searching for duplicate file-sizes in: " + filelist)
        print("  ==> Path to use for file-list: " + runMD5path)
        self.singleBufferMD5sumComparison(data, minSz, runMD5path)
        print(" ")


    def singleBufferMD5sumComparison(self, buf, minSize, runMD5path=""):
        # this does *NOT* compare buffer 1 with buffer 2 - instead it only check a single buffer for duplicates!
        disableMD5 = False  # True
        lastSz = []
        lastFileOrDir = []
        minSzCounter = 0
        nonNumCounter = 0
        numDuplicates = 0
        numLinesTotal = len(buf)
        l = 1  # current line number indicator - as shown in the text-file
        for line in buf:
            words = line.split()
            if len(words) <= 1:
                if len(line) != 0:
                    print("WARNING: Need size + file/directory, but unexpected string found in line: " + str(l))
            else:
                # Split words into [0]:size and [1]:file or directory:
                sz = words[0]
                fileOrDir = " ".join(words[1:])
                if not sz.isnumeric():
                    print("WARNING: First column is not numeric in line: " + str(l) + " (" + fileOrDir + ")")
                    nonNumCounter = nonNumCounter + 1
                else:
                    if len(lastSz) == 0:  # meaning: The first time a new file-size occurs
                        if (sz == 0):  # ignore files with size 0, cannot do md5sum or anything on them!
                            print("Filesize: 0 ==> Ignoring: " + str(fileOrDir))
                        else:
                            lastSz.append(int(sz))
                            lastFileOrDir.append(fileOrDir)
                            lastSz_lineNums = []
                            lastSz_lineNums.append(l)

                    elif lastSz[-1] == int(sz):  # append when more files, with same size occurs
                        lastSz.append(int(sz))
                        lastFileOrDir.append(fileOrDir)
                        lastSz_lineNums.append(l)
                    else:
                        # Don't do the comparison, until we have all of similar filesizes in lastSz+lastFileOrDir:
                        if len(lastSz) > 1:  # must be more than 1, for duplicates to exist...
                            # Must be > minSize, in order to be processed:
                            if lastSz[-1] <= minSize:
                                minSzCounter = minSzCounter + 1
                            else:
                                print(" ")
                                if len(lastSz_lineNums)<20:
                                    print("*** Duplicate size (=" + str(lastSz[-1]) + ") in lines: " + \
                                        str(lastSz_lineNums))
                                else:
                                    print("*** Duplicate size (=" + str(lastSz[-1]) + ") in lines: " + \
                                        str(lastSz_lineNums[0:9]) + "... (skipping, too many) ..." + \
                                        str(lastSz_lineNums[-9:]))

                                numDuplicates = numDuplicates + len(lastSz)

                                # Do the heavy lifting:
                                self.doMD5sum(lastSz, runMD5path, lastFileOrDir, disableMD5)

                        # --- Reset, prepare for next block of same filesize-comparisons:
                        lastSz = []
                        lastFileOrDir = []
                        lastSz.append(int(sz))
                        lastFileOrDir.append(fileOrDir)
            l = l + 1
            if l % 10000 == 0:
                print("Line: " + str(l) + "/" + str(numLinesTotal) + \
                      " (" + str(round(100*l/numLinesTotal)) + "% done)")


        # ---=== Just checking if there are "un-processed" files to do MD5-sum on ===---
        if len(lastSz) != 0:  # this should've been reset, if we're done...
            if lastSz > 0:  # ignore files with size 0...
                if len(lastSz) > 1:  # must be more than 1, for duplicates to exist...
                    # Must be > minSize, in order to be processed:
                    if lastSz[-1] <= minSize:
                        minSzCounter = minSzCounter + 1
                    else:
                        print(" ")
                        print("*** Duplicate size (=" + str(lastSz[-1]) + ") in lines: " + \
                              str(lastSz_lineNums[0:9]) + "... (skipping, too many) ..." + \
                              str(lastSz_lineNums[-9:]))
                       # print("*** Duplicate size in lines: " + str(lastSz_lineNums) \
                       #       + ". Size: " + str(lastSz[-1]) + " ***")
                        numDuplicates = numDuplicates + len(lastSz)

                        # Do the heavy lifting:
                        self.doMD5sum(lastSz, runMD5path, lastFileOrDir, disableMD5)

            # --- Reset, prepare for next block of same filesize-comparisons:
            lastSz = []
            lastFileOrDir = []


        # === And now we're done: ===
        if numDuplicates>0:
            print(" ")
            print("All done, lines processed: " + str(l - 1))
            print("Number of duplicates found: " + str(numDuplicates))
        else:
            print("All done, no duplicates found. Lines processed: " + str(l - 1))

        if minSzCounter > 0:
            print("Files discarded due to minimum-file size requirement: " + str(minSzCounter) + \
                  " (minSize=" + str(minSize) + ")")
        if nonNumCounter > 0:
            print("Files discarded due to non-numeric-file size: " + str(nonNumCounter))
        print(" ")
        print(" ")


    def printBufLine(self, b, l):
        return "Buffer: " + str(b) + ": Line: " + str(l + 1) + ": "


    def doMD5sum(self, lastSz, runMD5path, lastFileOrDir, disableMD5):
        storedMD5vals = []
        fileNotExisting = False

        # === Run md5sum on each of the duplicate files: ===
        for z in range(0, len(lastSz)):
            if self.fileNotExistCount >= self.fileNotExistLimit:
                print("ERROR: Too many files did not exist - exiting now...")
                sys.exit(1)

            fullPath = path.join(runMD5path, lastFileOrDir[z])
            if not path.exists(fullPath):
                print("\"" + fullPath + "\": does not exist: Cannot do md5sum... Skipping...")
                self.fileNotExistCount = self.fileNotExistCount + 1
                fileNotExisting = True
                continue

            if path.isdir(fullPath):
                print("\"" + fullPath + "\": is a directory... Skipping...")
            elif path.isfile(fullPath):
                if len(runMD5path) == 0:
                    print("\"" + fullPath + "\": MD5sum was not requested, only printing...")
                else:
                    if disableMD5:
                        print("\"" + fullPath + "\": MD5sum is disabled, only printing...")
                    else:
                        if False:  # preferably never use this, REALLY slow for LARGE files...
                            cmd = "md5sum " + fullPath
                            list_files = subprocess.getstatusoutput(cmd)
                        else:
                            list_files = self.ownMD5sum(fullPath)

                        # Print result to screen
                        if not list_files[0] == 0:
                            print("ERROR: md5sum exit code was: %d" % list_files[0])
                            print(list_files[1])
                            sys.exit(1)
                        else:
                            # Save md5sum, to make a conclusion in the end...
                            storedMD5vals.append(list_files[1].split()[0])  # save md5-value
                            print(list_files[1])
            else:
                print("\"" + fullPath + "\": is neither a dir/file, probably a special file, skipping md5sum!")

        # Write out conclusion: If all files are the same or not...
        if fileNotExisting:
            print(" *** ERROR: One or more files was not found - please fix this (need ALL files)!")
        else:
            if (all(x == storedMD5vals[0] for x in storedMD5vals)):
                print(" *** All these files are the same - seems you should remove duplicates!")
            else:
                duplicate_dict = {}  # a dictionary to store each of them.
                for i in storedMD5vals:  # loop through them.
                    duplicate_dict[i] = storedMD5vals.count(i)
                print(" *** WARNING: All these files are not exactly the same - try removing duplicates:")
                print(duplicate_dict)
        print(" ")


    def ownMD5sum(self, fpath):
        md5_hash = hashlib.md5()
        chunksize = 1024 ** 2
        #maxChunkNum = 2000 # md5sum for a 2 GB file on SSD takes around 5 seconds -> 2000 seems appropriate
        maxChunkNum = 1000 # md5sum for a 2 GB file on SSD takes around 30 seconds on mech HDD...
        #maxChunkNum = 2 # TEST REMOVE THIS!
        f = open(fpath, "rb")  # read, binary
        f.seek(0, SEEK_END)  # get the cursor positioned at end
        fsize = f.tell()  # get the current position of cursor, equivalent to size of file

        if not self.chunkData[0] == fsize:
            #print("Initializing chunk data...")
            self.chunkData = (fsize, fpath)

            self.chunks_needed = math.ceil(fsize / chunksize)
            # print("Size of file is :", fsize, "bytes")
            # print("# of 1MB chunks: ", self.chunks_needed)

            # Define the new "chunkOrder:"
            if self.chunks_needed == 0:  # read 1 chunk
                self.chunkOrder = range(1)
            elif self.chunks_needed <= maxChunkNum: # use correct order - and *ALL* chunks
                self.chunkOrder = range(self.chunks_needed)
            else:  # use only maxChunkNum random (increasing ordered) chunks...
                self.chunkOrder = np.sort(np.random.permutation(self.chunks_needed)[:maxChunkNum])
                print(" *** WARNING: Only doing md5sum on a part of the file (" + str(len(self.chunkOrder)) + \
                    "/" + str(self.chunks_needed) + " chunks are used)! ***")

            self.chunks = len(self.chunkOrder)
        #else:
            #print("Same file size: Using same chunk data...")

        # Begin calculating the md5sum, using chunkOrder:
        if not isinstance(self.chunkOrder, np.ndarray):
            f.seek(0, SEEK_SET)  # read from beginning, https://python-reference.readthedocs.io/en/latest/docs/file/seek.html
            for i in self.chunkOrder:  # for i in range(chunks):
                #print("Reading chunk: " + str(i + 1) + " of " + str(self.chunks))
                data = f.read(chunksize)  # https: // python - reference.readthedocs.io / en / latest / docs / file / read.html
                md5_hash.update(data)
        else:
            for i in self.chunkOrder:  # for i in range(chunks):
                #print("(WARNING: " + str(self.chunks) + "/" + str(self.chunks_needed) + " chunks are used)" \
                #    + ": Reading RANDOM (ordered) chunk: " + str(i + 1))
                f.seek(i*chunksize, SEEK_SET)  # read from beginning, https://python-reference.readthedocs.io/en/latest/docs/file/seek.html
                data = f.read(chunksize)  # https: // python - reference.readthedocs.io / en / latest / docs / file / read.html
                md5_hash.update(data)
        f.close()

        # Print result and return:
        digest = md5_hash.hexdigest()  # should return the same as the "md5sum"-command
        retStr = digest + ' ' + fpath
        retTuple = (0, retStr)
        return retTuple


    def runMD5onFile(self, b):
        if b == 1:
            l = self.line1
            fname = self.fullPath1
        elif b == 2:
            l = self.line2
            fname = self.fullPath2
        else:
            print("Invalid buffer! Program cannot continue")
            sys.exit(1)

        if not path.exists(fname):
            print("Error: File does not exist:", fname)
            sys.exit(1)
        cmd = "md5sum " + fname
        list_files = subprocess.getstatusoutput(cmd)
        if not list_files[0] == 0:
            print("ERROR: md5sum exit code was: %d" % list_files[0])
            print(list_files[1])
            sys.exit(1)
        else:
            print(self.printBufLine(b, l) + list_files[1])


    def twoBufferMD5comparison(self):  # this was meant to compare buffer 1 with buffer 2 (*NOT* within the same buffer)
        print("RUNNING: " + inspect.stack()[0][3] + '()')
        print(" ")
        # Init:
        sz = [0, 0]
        lines = [0, 0]  # will be incremented by "findNext"
        f = ['', '']
        sz[0] = self.findNext(1)
        sz[1] = self.findNext(2)

        while True:
            if (sz[0] == sz[1]):
                print(" --- Same size, running md5sum: ---")
                self.runMD5onFile(1)
                self.runMD5onFile(2)
                # Increment current line+filesize and filename once:
                sz[0] = self.findNext(1)
                sz[1] = self.findNext(2)
            else:
                # Search/increment line counters until a match is found or until done
                if int(sz[0]) > int(sz[1]):
                    sz[0] = self.findNext(1, sz[1])
                else:
                    sz[1] = self.findNext(2, sz[0])
                continue
            # print("sz=" + (str(sz)))
            if any(int(x) < 0 for x in sz):
                print("One buffer is complete, nothing more to compare then...")
                break
        print("All is done.")


    def findNext(self, b, searchSz="-1"):
        if b == 1:
            buf = self.data1
            l = self.line1
        elif b == 2:
            buf = self.data2
            l = self.line2
        else:
            print("Invalid buffer! Program cannot continue")
            sys.exit(1)

        # print("l=" + str(l) + ", len(buf)=" + str(len(buf)))
        if l < len(buf):
            l = l + 1  # find NEXT index, so increment - this "l" is linenumber in textfile (index from 1->)
            for i in range(l - 1, len(buf)):
                # print(str(i+1) + ": " + buf[i])
                words = buf[i].split()
                if (len(words) <= 1):
                    if len(line) != 0:
                        print("WARNING: Need size + file/directory, but unexpected string found in line: " + str(l))
                else:
                    sz = words[0]
                    fileOrDir = " ".join(words[1:])
                    if path.isdir(fileOrDir):
                        print(self.printBufLine(b, i) + \
                              "Skipping directory: \"" + fileOrDir + "\"")
                        continue
                    if sz.isnumeric():
                        if int(searchSz) < 0:
                            break
                        else:
                            if int(sz) <= int(searchSz):
                                break
                            else:
                                continue
            if b == 1:
                self.fullPath1 = path.join(self.file1root, fileOrDir)
                self.line1 = i + 1  # increment, human-line numbering (indexing from 1, not 0)
            elif b == 2:
                self.fullPath2 = path.join(self.file2root, fileOrDir)
                self.line2 = i + 1
        else:
            sz = "-1"
            if b == 1:
                print("Buffer 1: Nothing more to do...")
                self.fullPath1 = ''
            elif b == 2:
                print("Buffer 2: Nothing more to do...")
                self.fullPath2 = ''

        return sz


def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        usage="%(prog)s buffer",
        description="Buffer is an integer, 1 or 2."
    )
    parser.add_argument('buffer', nargs='*')
    return parser


if __name__ == '__main__':

    parser = init_argparse()
    args = parser.parse_args()

    if not args.buffer:
        print("ERROR: Choose buffer 1 or buffer 2 (as input arguments to this script)!")
        sys.exit(1)
    else:
        buf = int(args.buffer[0])

    cmp = Data()  # init
    if True:
        cmp.findDuplicatesInSingleBuffer(buf)

    else:
        # minsize: To prevent WAYY to many matches
        # Consider adding md5sum to those small files, to get rid of all the
        # false positives (same file size, but not the same contents!)

        minSize = 5000  # --- Testing on Asus-laptop: ---
        # minSize = 100000000 # === Real production: ===

        cmp.findDuplicatesInSingleBuffer(buf, minSize)  # buffer 1

    print("========================")
    print(
        "WARNING: Run only md5 on beginning+end of file, see: https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file")
    # Compare both large files using md5sum, across disks...
    # cmp.twoBufferMD5comparison(..)