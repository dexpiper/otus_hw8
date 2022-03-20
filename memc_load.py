#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
import subprocess
import uuid
from optparse import OptionParser
from threading import Thread
from queue import Queue

from pymemcache.client.base import Client
from pymemcache.client.retrying import RetryingClient
from pymemcache.exceptions import MemcacheUnexpectedCloseError

import appsinstalled_pb2


MAX_FEATURES = 1000
NORMAL_ERR_RATE = 0.01
DEFAULT_DIRECTORY = os.environ.get('DIRLOCATION', '/data/appsinstalled')
AppsInstalled = collections.namedtuple(
    "AppsInstalled",
    [
        "dev_type", "dev_id",
        "lat", "lon", "apps"
    ]
)


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc_addr, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    try:
        if dry_run:
            logging.debug(
                "%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
        else:
            base_client = Client(memc_addr, connect_timeout=0.5, timeout=0.3)
            memc = RetryingClient(
                base_client,
                attempts=3,
                retry_delay=0.01,
                retry_for=[MemcacheUnexpectedCloseError]
            )
            memc.set(key, packed)
    except Exception as e:
        logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
        return False
    return True


def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isdigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


class Writer(Thread):
    """
    Consumes task from task queue and writes results in answer_queue.
    """
    def __init__(self, task_queue, answer_queue, handler, dry=False,
                 _id=str(uuid.uuid4())[:4]):
        Thread.__init__(self, daemon=True)
        self.task_queue = task_queue
        self.answer_queue = answer_queue
        self.handler = handler
        self.dry = dry
        self.__id = _id
        self.__poison_pill = False

    def terminate(self):
        logging.debug(f'Writer {self.__id} got terminate command')
        self.__poison_pill = True

    def run(self):
        logging.debug(f'Writer {self.__id} started')
        while not self.__poison_pill:
            memc_addr, appsinstalled = self.task_queue.get()
            result = self.handler(memc_addr, appsinstalled, self.dry)
            self.answer_queue.put(result)
            self.task_queue.task_done()

    def join(self, *args) -> tuple:
        logging.debug(f'Shutting down Writer {self.__id}...')
        Thread.join(self, timeout=0.1)


class Reader(Thread):
    '''
    Consumes results from answer_queue and returns general result.
    '''
    def __init__(self, answer_queue, _id=str(uuid.uuid4())[:4]):
        Thread.__init__(self, daemon=True)
        self.answer_queue = answer_queue
        self.processed = self.errors = 0
        self.__id = _id
        self.__poison_pill = False

    def terminate(self):
        logging.debug(f'Reader {self.__id} got terminate command')
        self.__poison_pill = True

    def run(self):
        logging.debug(f'Reader {self.__id} started')
        while not self.__poison_pill:
            result_ok = self.answer_queue.get()
            if result_ok:
                self.processed += 1
            else:
                self.errors += 1
            self.answer_queue.task_done()

    def join(self, *args) -> tuple:
        logging.debug(f'Shutting down Reader {self.__id}...')
        Thread.join(self, timeout=0.1)
        return self.processed, self.errors


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    for fn in glob.iglob(options.pattern):
        task_queue = Queue(maxsize=1000)
        answer_queue = Queue()
        reader_pool = [
            Reader(answer_queue, _id=n) for n in range(options.maxworkers)
        ]
        assert len(reader_pool) == options.maxworkers
        [reader.start() for reader in reader_pool]
        reader = Reader(answer_queue)
        reader.start()
        writer_pool = [
            Writer(task_queue, answer_queue,
                   insert_appsinstalled,
                   options.dry, _id=n) for n in range(options.maxworkers)
        ]
        assert len(writer_pool) == options.maxworkers
        [writer.start() for writer in writer_pool]

        processed = errors = 0
        logging.info('Processing %s' % fn)
        fd = gzip.open(fn)
        for line in fd:
            line = line.strip().decode('utf-8')
            if not line:
                continue
            appsinstalled = parse_appsinstalled(line)
            if not appsinstalled:
                errors += 1
                continue
            memc_addr = device_memc.get(appsinstalled.dev_type)
            if not memc_addr:
                errors += 1
                logging.error(
                    "Unknown device type: %s" % appsinstalled.dev_type
                )
                continue
            logging.debug('Putting task to queue')
            task_queue.put((memc_addr, appsinstalled))

        task_queue.join()
        answer_queue.join()
        [writer.terminate() for writer in writer_pool]
        [writer.join() for writer in writer_pool]
        [reader.terminate() for reader in reader_pool]
        results_tuple = [reader.join() for reader in reader_pool]
        for result in results_tuple:
            processed += result[0]
            errors += result[1]

        if not processed:
            logging.info(
                'Closing and renaming file %s, no processed lines' %
                fn.split('/')[-1]
            )
            fd.close()
            # dot_rename(fn)
            continue

        logging.info('Errors: %s, Processed: %s' % (errors, processed))
        err_rate = float(errors) / processed
        if err_rate < NORMAL_ERR_RATE:
            logging.info(
                "Acceptable error rate (%s). Successfull load" % err_rate)
        else:
            logging.error(
                "High error rate (%s > %s). Failed load" % (
                    err_rate, NORMAL_ERR_RATE
                    )
                )
        fd.close()
        # dot_rename(fn)


def runtest():
    command = ['python', '-m', 'unittest', '-v']
    subprocess.run(command)


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--maxworkers", action="store", default=3)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store",
                  default=f"{DEFAULT_DIRECTORY}/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log,
                        level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s',
                        datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        runtest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
