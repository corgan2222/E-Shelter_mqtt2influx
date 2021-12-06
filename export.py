import os

def checkFolder(folder):
    if not os.path.exists(folder):
        os.makedirs(folder)

def saveTextFile(data, filename):           
    with open(filename,"w") as filewrite:
        filewrite.write(str(data))
   
def checkInputFolderPath(giveFolder, defaultStr):
    if len(giveFolder) > 0 & os.path.exists(giveFolder):
        return giveFolder

    cwd = os.getcwd()
    folder = os.path.join(cwd, 'data',defaultStr)
    if not os.path.exists(folder):
        os.makedirs(folder)
    return folder    


def createFolderPath(giveFolder, datafolder):
    folder = os.path.join(giveFolder, datafolder,"")   

    if os.path.exists(folder):
        return folder


    if not os.path.exists(folder):
        os.makedirs(folder)
        return folder

        
    


def is_valid_file(parser, arg):
    arg = os.path.abspath(arg)
    if not os.path.exists(arg):
        parser.error("The file %s does not exist!" % arg)
    else:
        return arg      



# def readCSVintoPanda(readCSV_filename):    
#     if os.path.exists(readCSV_filename):
#         df = pandas.read_csv(readCSV_filename, index_col='Count')        
#         df.head
#         return df.rename(columns={'Contents':'room','PositionX':'rx','PositionY':'ry'}) 
         

# def path_leaf(path):
#     head, tail = ntpath.split(path)
#     return tail or ntpath.basename(head)