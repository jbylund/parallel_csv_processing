#!/usr/bin/python
import csv
import math
import multiprocessing
import os
import sys
import tempfile

READ_BUFFER = 2**13


def split(infilename, num_cpus=multiprocessing.cpu_count()):
    total_file_size = os.path.getsize(infilename)
    files = list()
    with open(infilename) as infile:
        header = infile.readline()
        reader = csv.DictReader(infile)
        for i in xrange(num_cpus):
            files.append(tempfile.TemporaryFile())
            files[-1].write(header)  # write the header to each subfile so you can use a dictReader if desired
            this_file_size = 0
            while this_file_size < 1.0 * total_file_size / num_cpus:
                files[-1].write(infile.read(READ_BUFFER))
                this_file_size += READ_BUFFER
            files[-1].write(infile.readline())  # get the possible remainder
            files[-1].seek(0, 0)
    return files


def process_one_file(infile, outfile, function):
    reader = csv.DictReader(infile)
    fake_row = dict([[fieldname, 0] for fieldname in reader.fieldnames])
    function(fake_row)
    outkeys = fake_row.keys()
    sorted_outkeys = [x for x in reader.fieldnames if x in outkeys]
    sorted_outkeys += [x for x in outkeys if x not in reader.fieldnames]
    writer = csv.DictWriter(outfile, sorted_outkeys)
    writer.writeheader()
    for row in reader:
        function(row)
        writer.writerow(row)


def identity(x):
    return x


def add_a_column(x):
    x["new_column"] = "default_value"


class CSVProcessor(multiprocessing.Process):
    def __init__(self, infile, proc_num, results_queue, function):
        self.infile = infile
        self.proc_num = proc_num
        self.results_queue = results_queue
        self.function = function
        super(CSVProcessor, self).__init__()

    def run(self):
        self.outfile = tempfile.NamedTemporaryFile(delete=False)
        process_one_file(self.infile, self.outfile, self.function)
        self.outfile.close()
        self.results_queue.put((self.proc_num, self.outfile.name))


def merge(outfilename, rlist):
    with open(outfilename, "w") as outfile:
        header = open(rlist[0]).readline()
        outfile.write(header)  # write the header once
        for subfile in rlist:
            with open(subfile) as ifile:
                ifile.readline()  # skip past the header
                while True:
                    data = ifile.read(READ_BUFFER)
                    if not data:
                        break
                    outfile.write(data)
            os.unlink(subfile)


def process_csv(infilename, outfilename, function=identity, max_procs=max(6, multiprocessing.cpu_count()), outfields=None):
    # first split the file up into a bunch of smaller files
    files = split(infilename)

    # then process the pieces
    processes = list()
    results = multiprocessing.Queue()
    for proc_num, ifile in enumerate(files):
        this_process = CSVProcessor(ifile, proc_num, results, function)
        processes.append(this_process)
        this_process.start()  # start the processes

    # get the bits back
    rlist = list()
    for process in processes:
        process.join()  # wait to finish
        rlist.append(results.get())  # get the path to the resulting piece
    rlist.sort()  # sort so that things are in the same order

    merge(outfilename, [x[1] for x in rlist])  # finally merge it all back together


def main():
    process_csv("input_example.csv", "output_example.csv", function=add_a_column)


if "__main__" == __name__:
    main()
