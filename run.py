from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime

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
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = '', flush=True)
    # Print New Line on Complete
    if iteration == total:
        print()

input_dir = "input/"

final_pd = pd.DataFrame(columns=['chn'])
i = 0
l = len(os.listdir(input_dir))

printProgressBar(0, l, prefix='Progress:', suffix='Complete')
for filename in os.listdir(input_dir):
    if i % 100 == 0:
        printProgressBar(i + 1, l, prefix='Progress:', suffix='Complete')
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

    for fig in figures:
        key_name = fig["name"].split(":")[-1]
        value = float(fig.text.replace(',', '').replace('-', "0"))
        out_dict.setdefault(key_name, []).append(value)

    # Removing additional entries, only including current and previous year figures
    for key in out_dict:
        out_dict[key] = out_dict[key][:2]

    out_pd = pd.DataFrame.from_dict(out_dict, orient='index')
    out_pd = out_pd.transpose()
    out_pd["chn"] = chn
    if out_pd["chn"].count() > 1:
        se = pd.Series(["%s/%s/%s" % (day, month, year), "%s/%s/%s" % (day, month, str(int(year)-1))])
        out_pd["date"] = se.values
    else:
        out_pd["date"] = "%s/%s/%s" % (day, month, year)

    final_pd = final_pd.append(out_pd, sort=False)

final_pd.to_csv("output/output_%s.csv" % (datetime.now().strftime("%Y%m%d-%H%M%S")), index=False)

printProgressBar(1, 1, prefix='Progress:', suffix='Complete')
