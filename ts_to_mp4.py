import os
from os.path import basename
import json
import argparse
import datetime
import traceback
import shutil

import libs.file_util as file_util
import libs.video_util as video_util
import libs.download_manager as download_manager
"""
    1. download in chunks until you run into errors and combine at the end
    2. combine ts files into master ts
    3. use ffmpeg to convert ts file to mp4
"""
file_max_index = 10000
def run_ts_to_mp4(working_dir:str, out_name:str, input_url:str = None, template_url:str = None):
    """
        workflow to convert ts to mp4
        1. multiprocessing to donwload ts files
        2. merge them into a ts file in order
        3. ffmpeg to convert ts file to mp4
    """

    # Init dir
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
    dwn_template = None
    if input_url:
        dwn_template = input_url.replace("index0.ts", "index")
    if template_url:
        dwn_template = template_url
    print('input_url_template:{}'.format(dwn_template))
    out_file_name = os.path.join(ts_working_dir, basename(dwn_template))

    # download_ts_file([input_url, out_file_name])

    # Generate target download list
    input_url_list = []
    for x in range(0, file_max_index):
        full_link = dwn_template + "%d.ts"%(x)
        print("full_link: {}".format(full_link))
        input_url_list.append(full_link)

    # Download files
    file_list = download_manager.download_ts_multi(input_url_list, ts_working_dir)
    file_util.merge_file(file_list, output_file)
    video_util.convert_ts_to_mp4(output_file, output_mp4)
    if os.path.isfile(output_mp4):
        if os.path.isfile(output_file):
            os.remove(output_file)
        if os.path.isdir(ts_working_dir):
            shutil.rmtree(ts_working_dir)

def main(args):
    input_url = args.input_url
    template_url = args.template_url
    working_dir = args.working_dir
    out_name = args.out_name
    ts_video_dir = args.ts_video_dir
    if out_name and ts_video_dir and working_dir:
        output_file = os.path.join(working_dir, out_name + '.ts')
        output_mp4 = os.path.join(working_dir, out_name + '.mp4')
        ts_file = video_util.run_merge_ts(ts_video_dir, output_file)
        video_util.convert_ts_to_mp4(output_file, output_mp4)
    elif (input_url or template_url) and working_dir and out_name:
        run_ts_to_mp4(working_dir, out_name, input_url=input_url, template_url=template_url)
    else:
        EXAMPLE_USAGE = "python ts_to_mp4.py --working_dir <working_dir> --out_name <out_name> --template_url https://<host>/<path>/index"
        parser.print_help()
        print(f"EXMAPLE USAGE: {EXAMPLE_USAGE}")
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run TS to mp4')

    # Flags
    parser.add_argument('--input_url', nargs='?',
                        help='input_url')
    parser.add_argument('--template_url', nargs='?',
                        help='template_url')
                        
    parser.add_argument('--working_dir', nargs='?',
                        help='working_dir')
    parser.add_argument('--out_name', nargs='?',
                        help='out_name')
    parser.add_argument('--ts_video_dir', nargs='?',
                        help='ts_video_dir')

    args = parser.parse_args()
    main(args)