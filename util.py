import os
import csv
from csv import DictReader

def fast_scandir(dirname):
    subfolders= [f.path for f in os.scandir(dirname) if f.is_dir()]
    for dirname in list(subfolders):
        subfolders.extend(fast_scandir(dirname))
    return subfolders


def letter_to_int(letter):
    alphabet = list('abcdefghijklmnopqrstuvwxyz')
    return alphabet.index(letter) + 1

def readCSV_into_object(file):
    with open(file, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter = ',')
    return csv_reader    

def readCSV_into_Dict(filename):
    # open file in read mode
    with open(filename, 'r') as read_obj:
        # pass the file object to DictReader() to get the DictReader object
        dict_reader = DictReader(read_obj, delimiter = ',')
        # get a list of dictionaries from dct_reader
        list_of_dict = list(dict_reader)
        # print list of dict i.e. rows
    return list_of_dict
