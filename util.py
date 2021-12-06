import os

def fast_scandir(dirname):
    subfolders= [f.path for f in os.scandir(dirname) if f.is_dir()]
    for dirname in list(subfolders):
        subfolders.extend(fast_scandir(dirname))
    return subfolders


def letter_to_int(letter):
    alphabet = list('abcdefghijklmnopqrstuvwxyz')
    return alphabet.index(letter) + 1
