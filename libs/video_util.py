import os
import json
import glob
from os.path import basename
import libs.file_util as file_util

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
    output_file = file_util.merge_file(ordered_file_list, output_file)
    return output_file


def convert_ts_to_mp4(input_ts, output_mp4):
    # ffmpeg -i  input_file -map 0 -c copy output_file
    ffmpeg_cmd = "ffmpeg -i {} -map 0 -c copy {}".format(input_ts, output_mp4)
    print(ffmpeg_cmd)
    os.system(ffmpeg_cmd)
