from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime
import sqlalchemy
import shutil

def add_column(engine, table_name, column):
    column_name = column.compile(dialect=engine.dialect)
    column_type = column.type.compile(engine.dialect)
    engine.execute('ALTER TABLE %s ADD COLUMN %s %s' % (table_name, column_name, column_type))

def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█'):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s %s/%s' % (prefix, bar, percent, suffix, iteration, total), end = '', flush=True)
    # Print New Line on Complete
    if iteration == total:
        print()

# Getting directory names
input_dir = "input/"
complete_dir = "complete/"
current_dir = os.getcwd()

# Create or connect to Database
db = sqlalchemy.create_engine(r'sqlite:///'+current_dir+'/chfin.db', echo=False)

# Counters for progress bar
i = 0
l = len(os.listdir(input_dir))

# Initialize progressbar
printProgressBar(0, l)
for filename in os.listdir(input_dir):
    # Update progress bar once every 100 files
    if i % 100 == 0:
        printProgressBar(i + 1, l)
    i += 1

    # Extracting date and companies house number
    day = filename[-7:-5]
    month = filename[-9:-7]
    year = filename[-13:-9]
    chn = filename[-22:-14]

    # Opening file and looping through figures available
    soup = BeautifulSoup(open("%s%s" % (input_dir, filename), encoding='utf-8'), "html.parser")
    figures = soup.find_all(name="ix:nonfraction")
    out_dict = {}
    # for table_row in table_rows:
    #
    #     figures = table_row.find_all(name="ix:nonfraction")
    #
    #     # if len(figures) > 0:
    #     #     key_name_appender = table_row.find_all(name="span")[0].text

    # Fixing Column Name and changing format of figures
    # TODO: Check if there are more names than just one. If yes, assign contextref.

    name_checker = {}
    for fig in figures:
        key_name = fig["name"].split(":")[-1]
        name_checker.setdefault(key_name, 0)
        name_checker[key_name] += 1

    for fig in figures:
        key_name = fig["name"].split(":")[-1]
        if name_checker[key_name] > 2:
            key_name = key_name + ":" + fig["contextref"]
        # TODO: Multiply by scale and check if value should be negative
        value = float(fig.text.replace(',', '').replace('-', "0"))
        out_dict.setdefault(key_name, []).append(value)

    # # Removing additional entries, only including current and previous year figures
    # for key in out_dict:
    #     out_dict[key] = out_dict[key][:2]

    # Creating a temporary DataFrame
    out_pd = pd.DataFrame.from_dict(out_dict, orient='index')
    out_pd = out_pd.transpose()
    out_pd["chn"] = chn
    try:
        if out_pd["chn"].count() > 1:
            se = pd.Series(["%s/%s/%s" % (day, month, year), "%s/%s/%s" % (day, month, str(int(year)-1))])
            out_pd["date"] = se.values
        else:
            out_pd["date"] = "%s/%s/%s" % (day, month, year)
    except:
        out_pd.to_csv("out_pd.csv")

    # Adding any previously missing column into the sqlite database
    if db.dialect.has_table(db, "fin"):
        for col in out_pd:
            if col not in db.execute("SELECT * FROM fin LIMIT 1").keys():
                new_col = sqlalchemy.Column(col, sqlalchemy.FLOAT)
                add_column(db, "fin", new_col)

    # Appending data from DataFrame into sqlite database
    out_pd.to_sql("fin", con=db, if_exists='append', index=False)
    del out_pd

    # Moving file into the complete dir
    shutil.move(input_dir + filename, complete_dir)

printProgressBar(1, 1)
