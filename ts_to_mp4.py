import os
from os.path import basename
import glob
import json
import argparse
import datetime
from multiprocessing import Pool
import urllib.request
import traceback
import itertools
"""
    1. download in chunks until you run into errors and combine at the end
    2. combine ts files into master ts
    3. use ffmpeg to convert ts file to mp4
"""
file_max_index = 10000
processing_chunks_size = 200

def chunks(l, n):
    """
        list to list of list in chunks
    """
    return [list(t) for t in zip(*[iter(l)]*n)]

def run_ts_to_mp4(input_url, working_dir, out_name):
    """
        workflow to convert ts to mp4
        1. multiprocessing to donwload ts files
        2. merge them into a ts file in order
        3. ffmpeg to convert ts file to mp4
    """
    timestamp = datetime.datetime.now().isoformat()
    timestamp_str = str(timestamp)[0:19].replace(":", "_")
    if not os.path.isdir(working_dir):
        os.mkdir(working_dir)
    ts_working_dir = os.path.join(working_dir, "{}_{}".format(timestamp_str, out_name))
    output_file = os.path.join(working_dir, out_name+".ts")
    output_mp4 = os.path.join(working_dir, out_name+".mp4")

    if not os.path.isdir(ts_working_dir):
        os.mkdir(ts_working_dir)
    print('ts_working_dir:{}'.format(ts_working_dir))
    dwn_link = os.path.dirname(input_url)
    input_url_template = input_url.replace('index0.ts', 'index%d.ts')
    print('input_url_template:{}'.format(input_url_template))
    out_file_name = os.path.join(ts_working_dir, basename(input_url))
    # download_ts_file([input_url, out_file_name])

    input_url_list = []
    for x in range(0, file_max_index):
        full_link = dwn_link + "/index%d.ts"%(x)
        print("full_link: {}".format(full_link))
        input_url_list.append(full_link)
    # print(input_url_chunks)
    file_list = download_ts_multi(input_url_list, ts_working_dir)
    merge_file(file_list, output_file)
    convert_ts_to_mp4(output_file, output_mp4)

def run_merge_ts(ts_video_dir, output_file):
    """
        merge file
        1. Validate
        TODO generalize validation
    """
    file_list = glob.glob(os.path.join(ts_video_dir, '*.ts'))
    print(json.dumps(file_list, indent=2))
    print("min: {} max: {}".format(min(file_list), max(file_list)))
    ordered_file_list = []
    # validate and sort
    min_index = 10000000
    max_index = 0
    for filename in file_list:
        index = int(basename(filename).replace("index","").replace(".ts",""))
        if index > max_index:
            max_index = index
        if index < min_index:
            min_index = index
    for index in range(min_index, max_index+1):
        ts_file = os.path.join(ts_video_dir, "index%d.ts"%(index))
        print("added: {}".format(ts_file))
        if not os.path.isfile(ts_file):
            raise Exception("not found ts_file:{}".format(ts_file))
        ordered_file_list.append(ts_file)
    output_file = merge_file(ordered_file_list, output_file)
    return output_file
def convert_ts_to_mp4(input_ts, output_mp4):
    # ffmpeg -i  input_file -map 0 -c copy output_file
    ffmpeg_cmd = "ffmpeg -i {} -map 0 -c copy {}".format(input_ts, output_mp4)
    print(ffmpeg_cmd)
    os.system(ffmpeg_cmd)

def download_ts_file(args):
    """
        multiprocessing function to download ts file
    """
    full_link = args[0]
    working_dir = args[1]
    out_file_name = os.path.join(working_dir, basename(full_link))
    try:
        rsp = urllib.request.urlopen(full_link)
    except:
        print('failed on {}'.format(full_link))
        print(traceback.format_exc())
        return False
    f = open(out_file_name, 'wb')
    f.write(rsp.read())
    f.close()
    print('downloaded: {}'.format(out_file_name))
    return out_file_name

def print_ts_file(args):
    """
        debug code for multiprocessing
    """
    full_link = args[0]
    out_file_name = args[1]
    print(full_link)
    return full_link

def merge_file(file_list, output_file):
    """
        file processing to merge ts files to one file
    """
    with open(output_file, 'ab') as f:
        for file_name in file_list:
            print("writing:{}".format(file_name))
            with open(file_name, 'br') as in_f:
                f.write(in_f.read())
    return output_file
def download_ts_multi(input_url_list, working_dir):
    """
        multiprocessing to download in chunks
    """
    input_url_chunks = chunks(input_url_list, processing_chunks_size)
    output_file_list = []
    pool = Pool()
    print(json.dumps(input_url_chunks, indent=2))
    for chunk_index in range(0, len(input_url_chunks)):
        print("processing: {} out of {}".format(chunk_index, len(input_url_chunks)))
        print("min: {} max {}".format(min(input_url_chunks), max(input_url_chunks)))
        input_url_chunk = input_url_chunks[chunk_index]
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

def main(args):
    input_url = args.input_url
    working_dir = args.working_dir
    out_name = args.out_name
    ts_video_dir = args.ts_video_dir
    if out_name and ts_video_dir and working_dir:
        output_file = os.path.join(working_dir, out_name + '.ts')
        output_mp4 = os.path.join(working_dir, out_name + '.mp4')
        ts_file = run_merge_ts(ts_video_dir, output_file)
        convert_ts_to_mp4(output_file, output_mp4)
    elif input_url and working_dir and out_name:
        run_ts_to_mp4(input_url, working_dir, out_name)
    else:
        parser.print_help()
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run TS to mp4')

    # Flags
    parser.add_argument('--input_url', nargs='?',
                        help='input_url')
    parser.add_argument('--working_dir', nargs='?',
                        help='working_dir')
    parser.add_argument('--out_name', nargs='?',
                        help='out_name')
    parser.add_argument('--ts_video_dir', nargs='?',
                        help='ts_video_dir')

    args = parser.parse_args()
    main(args)