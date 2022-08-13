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
    files = []
    with open(infilename) as infile:
        header = infile.readline()
        data_size = os.path.getsize(infilename) - len(header)
        target_size = len(header) + data_size / num_chunks
        logger.info(
            "Splitting %s (%d bytes) into %s chunks (of ~%d bytes each)...",
            infilename,
            os.path.getsize(infilename),
            num_chunks,
            target_size,
        )
        for _ in range(num_chunks):
            files.append(tempfile.NamedTemporaryFile(mode="w+", delete=False))
            files[-1].write(header)  # write the header to each subfile so you can use a dictReader if desired
            while files[-1].tell() < target_size:
                chunk = infile.read(READ_BUFFER)
                if chunk:
                    files[-1].write(chunk)
                else:
                    break
            files[-1].write(infile.readline())  # get the possible remainder of the line
    return [f.name for f in files]


def identity(row):
    """Useful identity function"""
    return row


def add_a_column(row):
    """Simple operation to demonstrate applying transformation to csv"""
    row["new_column"] = "default_value"


def apply_function_to_piece(args):
    """Apply a function to every row of a csv"""
    idx, function, piecename = args
    with open(piecename) as infile_fh:
        reader = csv.DictReader(infile_fh)
        with tempfile.NamedTemporaryFile(delete=False, mode="w+") as outfile_fh:
            fake_row = {fieldname: "" for fieldname in reader.fieldnames}
            function(fake_row)
            sorted_outkeys = [x for x in reader.fieldnames if x in fake_row]  # fields from input csv
            sorted_outkeys += [x for x in fake_row if x not in reader.fieldnames]  # new fields in output csv
            writer = csv.DictWriter(outfile_fh, sorted_outkeys)
            writer.writeheader()
            for row in reader:
                function(row)  # apply the transformation to the row
                writer.writerow(row)  # write the output row
    os.remove(piecename)  # remove the processing artifact
    return idx, outfile_fh.name


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
    logger.info("Will apply %s to %s in %d processes...", function.__name__, infilename, max_procs)

    files = split(infilename)

    rlist = []
    logger.info("Applying %s to all %d pieces...", function.__name__, len(files))
    with multiprocessing.Pool(processes=max_procs) as wp:
        for res in wp.imap_unordered(apply_function_to_piece, [(idx, function, ifile) for idx, ifile in enumerate(files)]):
            rlist.append(res)

    rlist.sort()  # sort so that things are in the same order

    merge(outfilename, [x[1] for x in rlist])  # finally merge it all back together

    # hash of resulting file should be 30ead4730052bd651d685774bc37d694 for
    # https://raw.githubusercontent.com/MicrosoftLearning/dp-090-databricks-ml/master/data/nyc-taxi.csv
    # applying add_column


def get_args():
    """Argument parsing"""
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
