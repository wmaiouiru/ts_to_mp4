"""
    File Processing Utilities
"""

def merge_file(file_list: list, output_file: str):
    """
        file processing to merge ts files to one file
    """
    with open(output_file, 'ab') as f:
        for file_name in file_list:
            print("writing:{}".format(file_name))
            with open(file_name, 'br') as in_f:
                f.write(in_f.read())
    return output_file

def chunks(l, n):
    """
        list to list of list in chunks
    """
    return [list(t) for t in zip(*[iter(l)]*n)]