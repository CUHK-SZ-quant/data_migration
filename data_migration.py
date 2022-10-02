from email import parser
import os, sys
import traceback 
import pandas as pd 
import numpy as np 
import argparse
import re
import multiprocessing
from multiprocessing import Pool
# from multi_process_utils import mp_apply_async
from config_argparser import Config_Parser
from data_structure import Raw_Data_Structure

parser = Config_Parser()
parser.add_argument('--raw_dir', type=str, help = 'The directory of the raw data')
parser.add_argument('--to_dir', type=str, help = 'The directory to save the migrated data')
parser.add_argument('--num_progress', type=int, help = 'The number of progress to be used')
args = parser.parse_args()

data_dir = args.raw_dir
to_dir = args.to_dir

if not os.path.isdir(to_dir):
    os.makedirs(to_dir)

def get_str_until_first_int(s:str):
    target_str = re.findall('([a-zA-Z\_ ]*)\d*.*', s)
    return target_str[0]

def get_all_dates(file_lst:list):
    dates = list(set([f.split('_')[2] for f in file_lst]))
    return dates

def get_all_symbols(file_lst:list):
    symbols = list(set(list(map(get_str_until_first_int, file_lst))))
    return symbols

def get_files_at_date(file_lst:list, date:str):
    file_lst = [f for f in file_lst if f.split('_')[2] == date]
    return file_lst

def concat_date_data(file_lst, date, curr_path):
    files_at_date = get_files_at_date(file_lst, date)
    df_lst = []
    for file in files_at_date:
        tmp_df = pd.read_excel(os.path.join(curr_path, file), engine='openpyxl').drop('Unnamed: 0', axis=1)
        df_lst.append(tmp_df)
    concat_df = pd.concat(df_lst, axis=0).sort_values('datetime')
    save_dir = os.path.join(to_dir, date)
    if not os.path.isdir(save_dir):
        os.makedirs(save_dir)
    concat_df.to_parquet(os.path.join(save_dir, 'hft-level1-migration.parquet.gzip'))
    print(f'Finish {date}')

def log_failure(caught):
    traceback.print_exc(file=sys.stderr)

def main():
    for path in os.scandir(data_dir):
        if not path.is_dir():
            continue
        file_lst = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
        # print(file_lst)
        dates = get_all_dates(file_lst)
        
        pool = Pool(args.num_progress)
        missions = [
            pool.apply_async(
                func = concat_date_data, args=(file_lst, date, path.path), callback=None, error_callback=log_failure
            ) for date in dates
        ]
        pool.close()
        pool.join()


if __name__ == '__main__':
    main()