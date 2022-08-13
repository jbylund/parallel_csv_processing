"""Demo for how you can parallelize processing of a csv file"""
import argparse
import csv
import logging
import multiprocessing
import os
import tempfile

READ_BUFFER = 2**13

logger = logging.getLogger(__name__)


def split(infilename, num_chunks=multiprocessing.cpu_count()):
    """Split a large csv into multiple smaller chunks"""
    total_file_size = os.path.getsize(infilename)
    target_size = total_file_size / num_chunks
    logger.info(
        "Splitting %s (%d bytes) into %s chunks (of ~%d bytes each)...", infilename, total_file_size, num_chunks, target_size
    )

    files = []
    with open(infilename) as infile:
        header = infile.readline()
        reader = csv.DictReader(infile)
        for _ in range(num_chunks):
            files.append(tempfile.TemporaryFile(mode="w+"))
            files[-1].write(header)  # write the header to each subfile so you can use a dictReader if desired
            this_file_size = 0
            while this_file_size < 1.0 * total_file_size / num_chunks:
                files[-1].write(infile.read(READ_BUFFER))
                this_file_size += READ_BUFFER
            files[-1].write(infile.readline())  # get the possible remainder
            files[-1].seek(0, 0)
    return files


def process_one_file(infile, outfile, function):
    """Apply function to each line of infile and write result to outfile"""
    reader = csv.DictReader(infile)
    fake_row = {fieldname: 0 for fieldname in reader.fieldnames}
    function(fake_row)
    outkeys = fake_row.keys()
    sorted_outkeys = [x for x in reader.fieldnames if x in outkeys]
    sorted_outkeys += [x for x in outkeys if x not in reader.fieldnames]
    writer = csv.DictWriter(outfile, sorted_outkeys)
    writer.writeheader()
    for row in reader:
        function(row)
        writer.writerow(row)


def identity(row):
    """Useful identity function"""
    return row


def add_a_column(row):
    """Simple operation to demonstrate applying transformation to csv"""
    row["new_column"] = "default_value"


class CSVProcessor(multiprocessing.Process):
    """CSV processing helper process"""

    def __init__(self, infile, proc_num, results_queue, function):
        """Initialize a csv processing helper"""
        self.infile = infile
        self.proc_num = proc_num
        self.results_queue = results_queue
        self.function = function
        super().__init__()

    def run(self):
        """Apply transformation to a slice (which is just a smaller csv)"""
        with tempfile.NamedTemporaryFile(delete=False, mode="w+") as outfile:
            process_one_file(self.infile, outfile, self.function)
        self.results_queue.put((self.proc_num, outfile.name))


def merge(outfilename, rlist):
    """Merge a bunch of csv files back together"""
    logger.info("Merging %d files into %s...", len(rlist), outfilename)
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
    logger.info("Finished merging...")


def process_csv(*, infilename=None, outfilename=None, function=identity, max_procs=max(6, multiprocessing.cpu_count())):
    """Apply some transformation function to a csv"""
    # first split the file up into a bunch of smaller files
    logger.info("Will apply %s to %s in %d processes...", function.__name__, infilename, max_procs)

    files = split(infilename)

    logger.info("Applying %s to all %d pieces...", function.__name__, len(files))
    # then process the pieces
    processes = []
    results = multiprocessing.Queue()
    for proc_num, ifile in enumerate(files):
        this_process = CSVProcessor(ifile, proc_num, results, function)
        processes.append(this_process)
        this_process.start()  # start the processes

    logger.info("Collecting the results...")
    # get the bits back
    rlist = []
    for process in processes:
        process.join()  # wait to finish
        rlist.append(results.get())  # get the path to the resulting piece
    rlist.sort()  # sort so that things are in the same order

    merge(outfilename, [x[1] for x in rlist])  # finally merge it all back together

    # hash of resulting file should be 30ead4730052bd651d685774bc37d694 for
    # https://raw.githubusercontent.com/MicrosoftLearning/dp-090-databricks-ml/master/data/nyc-taxi.csv
    # applying add_column


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--infilename", required=True)
    parser.add_argument("--outfilename", default="output.csv")
    return vars(parser.parse_args())


def main():
    """Main entry point for parallel csv processing demo"""
    logging.basicConfig(level=logging.INFO)
    args = get_args()
    process_csv(function=add_a_column, **args)


if "__main__" == __name__:
    main()
