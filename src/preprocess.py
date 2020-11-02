import argparse
import re
from typing import Tuple

import pandas as pd

from .constants import data_dir
from .utils import extract_digit, is_digit_in, composite_func, load_all_csv


def drop_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=['Unnamed: 0', 'name'])
    return df


def parse_address(s: pd.Series) -> Tuple[str, str]:
    a = s.address
    d = s.district
    i = a.find(d)
    return a[:i], a[i+len(d):]

def disolve_address_col(df: pd.DataFrame) -> pd.DataFrame:
    df['prefecture'], df['address'] = zip(*df.apply(parse_address, axis=1))
    return df


def parse_location(location: str) -> Tuple[str, int, int]:
    walk_time, bus_time = 0, 0
    if pd.isna(location):
        return None, walk_time, bus_time

    station_i = location.find(' ')
    station, times = location[:station_i], location[station_i + 1:]

    for way_time in re.finditer('(歩|バス)[0-9]+分', times):
        if '歩' in way_time[0]:
            walk_time += int(extract_digit(way_time[0]))
        elif 'バス' in way_time[0]:
            bus_time += int(extract_digit(way_time[0]))

    return station, walk_time, bus_time

def disolve_location_cols(df: pd.DataFrame) -> pd.DataFrame:
    for i in range(3):
        (df[f'station_{i}'],
         df[f'walk_time_{i}'],
         df[f'bus_time_{i}']) = zip(*df[f'location{i}'].apply(parse_location))
        df = df.drop(columns=[f'location{i}'])
    return df


def int_age_col(df: pd.DataFrame) -> pd.DataFrame:
    df['age'] = df['age'].apply(lambda x: 0 if x == '新築' else int(extract_digit(x)))
    return df

def sum_above_below(s: str) -> int:
    if s == '平屋':
        return 1
    sum = 0
    for story in re.finditer('(地(上|下)|)[0-9]+', s):
        if re.search('地下', story[0]):
            sum += min(2, int(extract_digit(story[0])))
        else:
            sum += int(extract_digit(story[0]))
    return sum

def num_story_col(df: pd.DataFrame) -> pd.DataFrame:
    df['stories'] = df['height'].apply(sum_above_below)
    df = df.drop(columns=['height'])
    return df


def int_maisonette(s: pd.Series) -> Tuple[int, int]:
    if re.search('[0-9]+', s) is None:
        return 1, 0
    floors = list(map(lambda x: int(x[0]), re.finditer('[0-9]+', s)))
    if len(floors) == 1:
        return floors[0], 0
    if len(floors) == 2:
        if abs(floors[0] - floors[1]) <= 1:
            return max(floors), 1
        return max(floors), 0

def int_floor_col(df: pd.DataFrame) -> pd.DataFrame:
    df['floor'], df['maisonette'] = zip(*df['floor'].apply(int_maisonette))
    return df


def yen2kilo(s: str) -> int:
    if not is_digit_in(s):
        return 1
    yen = int(extract_digit(s))
    if '万' in s:
        return 10 * yen
    if '千' in s:
        return yen
    return int(yen / 1000)

def fee2int_col(df: pd.DataFrame) -> pd.DataFrame:
    for col in ['rent', 'admin', 'deposit', 'gratuity']:
        df[col] = df[col].apply(yen2kilo)
    return df


def area2float_col(df: pd.DataFrame) -> pd.DataFrame:
    df['area'] = df['area'].apply(extract_digit)
    return df


def encode_architecture_col(df: pd.DataFrame) -> pd.DataFrame:

    def encode_architecture(arch: str) -> Tuple[int, int, int, int]:
        if arch == '鉄筋系':
            return 1, 0, 0, 0
        if arch == '鉄骨系':
            return 0, 1, 0, 0
        if arch == '木造':
            return 0, 0, 1, 0
        if arch == 'ブロックその他':
            return 0, 0, 0, 1

    (df['RC'],
     df['ST'],
     df['WD'],
     df['OT']) = zip(*df['architecture'].apply(encode_architecture))
    return df


preprocess = composite_func(
    drop_cols,
    disolve_address_col,
    disolve_location_cols,
    num_story_col,
    int_floor_col,
    fee2int_col,
    area2float_col
)

def main(args):
    df = load_all_csv(data_dir / 'raw' / args.load_dir)
    df = preprocess(df)

    save_dir = data_dir / 'interim'
    save_dir.mkdir(exist_ok=True)
    df.to_csv(save_dir / args.save_path)


if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Preprocess for all csv files in the input directory.')
    parser.add_argument(
        'load_dir',
        type = str,
        default = '',
        help='Execute all preprocesses all csv in this directory.')
    parser.add_argument(
        'save_path',
        type = str,
        default = 'preprocessed.csv',
        help='Save preprocessed csv to this path.')

    main(parser.parse_args())