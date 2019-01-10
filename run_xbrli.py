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

    # Fixing Column Name and changing format of figures

    for fig in figures:
        tag_name = fig["name"].split(":")[-1]
        contextref = fig["contextref"]
        context = soup.find(name="xbrli:context", id=contextref)
        if context is not None:

            end_date = context.find(name="xbrli:instant")
            start_date = None
            if end_date is None:
                start_date = context.find(name="xbrli:startdate").text
                end_date = context.find(name="xbrli:enddate").text
            else:
                end_date = end_date.text

            out_dict.setdefault(end_date, {})
            if start_date is not None:
                out_dict[end_date].setdefault("start_date", [start_date])
            out_dict[end_date].setdefault("end_date", [end_date])

            segment = context.find(name="xbrli:segment")
            key_name = tag_name
            if segment is not None:
                segment_text = segment.getText().split(":")[-1]
                key_name = tag_name + ":" + segment_text

            # Multiplying by scale and check if value should be negative
            multiplier = 1
            if fig.has_attr('sign'):
                if fig['sign'] == "-":
                    multiplier = -1
            if fig.has_attr('scale'):
                if int(fig['scale']) > 0:
                    multiplier = multiplier * (10 ** int(fig['scale']))
                    print(filename)
            # if fig.has_attr('decimals'):
            #     if fig['decimals'] == "INF":
            #         multiplier = multiplier * 0.01
            value = float(fig.text.replace(',', '').replace('-', "0"))
            out_dict[end_date].setdefault(key_name, [""])[0] = value * multiplier

    # # Removing additional entries, only including current and previous year figures
    # for key in out_dict:
    #     out_dict[key] = out_dict[key][:2]

    # Adding Companies house number and filename to pandas df for reference. Adding any missing columns to sql table
    # and appending dataframe to sql db.
    for key in out_dict:
        out_pd = pd.DataFrame.from_dict(out_dict[key], orient='columns')
        out_pd["chn"] = chn
        out_pd["file_name"] = filename

        # Adding any previously missing column into the sqlite database

        if db.dialect.has_table(db, "fin"):
            for col in out_pd:
                if col not in db.execute("SELECT * FROM fin LIMIT 1").keys():
                    new_col = sqlalchemy.Column(col, sqlalchemy.FLOAT)
                    add_column(db, "fin", new_col)

        # Appending data from DataFrame into sqlite database
        out_pd.to_sql("fin", con=db, if_exists='append', index=False)
    del out_dict

    # # Moving file into the complete dir
    # shutil.move(input_dir + filename, complete_dir)

printProgressBar(1, 1)
