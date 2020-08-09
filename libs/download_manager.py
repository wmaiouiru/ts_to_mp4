import os
import time
import json
import traceback
import requests
from os.path import basename
import urllib.request
from multiprocessing import Pool
from enum import Enum
import itertools
import random


import libs.file_util as file_util

processing_chunks_size = 100

class DOWNLOAD_STATUS(Enum):
    INITIAL = 0
    SUCCESS = 200 # ok
    SERVER_ERROR = 500
    REQUEST_ERROR = 400
    OTHER_ERROR = 600
class PROCESSING_STATUS(Enum):
    INITIAL = 0
    SUCCESS = 1
    ERROR = -1
    DOWNLOAD_ERROR = -2

class DownloadManager(object):
    """
        Download Manager to handle download
    """
    def __init__(self, input_url: str, working_dir: str, out_file: str = None):
        self.input_url = input_url
        self.working_dir = working_dir
        self.out_file = out_file
        if not self.out_file:
            self.out_file = os.path.join(working_dir, basename(input_url))
        self.resp_status = DOWNLOAD_STATUS.INITIAL
        self.status = PROCESSING_STATUS.INITIAL

    def parse_response(self, resp):
        """
            Handle Response
        """
        if resp.ok:
            self.resp_status = DOWNLOAD_STATUS.SUCCESS
            return resp.content
        elif resp.status_code == 503:
            print("503 Service Temporarily Unavailable: {}".format(self.input_url))
            self.resp_status = DOWNLOAD_STATUS.SERVER_ERROR
            self.status = PROCESSING_STATUS.DOWNLOAD_ERROR
            return None
        elif resp.status_code >= 500 and resp.status_code < 600:
            print(resp.text)
            self.resp_status = DOWNLOAD_STATUS.SERVER_ERROR
            self.status = PROCESSING_STATUS.DOWNLOAD_ERROR
            return None
        elif resp.status_code >= 400 and resp.status_code < 500:
            self.resp_status = DOWNLOAD_STATUS.REQUEST_ERROR
            self.status = PROCESSING_STATUS.DOWNLOAD_ERROR
            return None
        else:
            print(resp.text)
            self.resp_status = DOWNLOAD_STATUS.OTHER_ERROR
            self.status = PROCESSING_STATUS.DOWNLOAD_ERROR
            return None

    def download_file(self):
        resp = requests.get(self.input_url)
        content = self.parse_response(resp)
        if self.resp_status == DOWNLOAD_STATUS.SUCCESS:
            f = open(self.out_file, 'wb')
            f.write(content)
            f.close()
            self.status = PROCESSING_STATUS.SUCCESS
        else:
            self.status = PROCESSING_STATUS.ERROR
            return None

def download_ts_file(args):
    """
        multiprocessing function to download ts file

        exponential backoff: https://cloud.google.com/iot/docs/how-tos/exponential-backoff
    """
    retry_limit = 5
    full_link = args[0]
    working_dir = args[1]
    dm = DownloadManager(full_link, working_dir)
    if os.path.isfile(dm.out_file):
        print("using cached: {}".format(dm.out_file))
        return dm.out_file
    for index in range(0, retry_limit+1):
        dm.download_file()
        if dm.status == PROCESSING_STATUS.SUCCESS:
            return dm.out_file
        elif dm.resp_status == DOWNLOAD_STATUS.SERVER_ERROR:
            # exponential backoff
            time.sleep(float(2^index) + random.random())
        elif dm.resp_status == DOWNLOAD_STATUS.REQUEST_ERROR:
            print("request failed: {}".format(full_link))
            return None
        elif dm.resp_status == DOWNLOAD_STATUS.OTHER_ERROR:
            print("download failed: {}".format(full_link))
            return None
    print("download failed: {}".format(full_link))

def print_ts_file(args):
    """
        debug code for multiprocessing
    """
    full_link = args[0]
    out_file_name = args[1]
    print("full_link:{} out_file_name:{}".format(full_link, out_file_name))
    return full_link

def download_ts_multi(input_url_list, working_dir):
    """
        multiprocessing to download in chunks
    """
    input_url_chunks = file_util.chunks(input_url_list, processing_chunks_size)
    output_file_list = []
    pool = Pool()
    print(json.dumps(input_url_chunks, indent=2))
    for chunk_index in range(0, len(input_url_chunks)):
        input_url_chunk = input_url_chunks[chunk_index]
        print("processing: {} out of {}".format(chunk_index, len(input_url_chunks)))
        print("min: {} max: {}".format(min(input_url_chunk), max(input_url_chunk)))
        # results = pool.map(print_ts_file, zip(input_url_list, itertools.repeat(working_dir)))
        results = pool.map(download_ts_file, zip(input_url_chunk, itertools.repeat(working_dir)))
        has_error = False
        for result in results:
            if result:
                output_file_list.append(result)
            else:
                has_error = True
        if has_error:
            break
    return output_file_list
