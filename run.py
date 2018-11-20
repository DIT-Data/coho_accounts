from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime

input_dir = "input/"
output_dir = "complete/"

final_pd = pd.DataFrame(columns=['chn'])

for filename in os.listdir(input_dir):

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
    se = pd.Series(["%s/%s/%s" % (day, month, year), "%s/%s/%s" % (day, month, str(int(year)-1))])
    out_pd["date"] = se.values
    final_pd = final_pd.append(out_pd, sort=False)

final_pd.to_csv("output/output_%s.csv" % (datetime.now().strftime("%Y%m%d-%H%M%S")), index=False)

# Moving files to completed
for filename in os.listdir(input_dir):
    os.rename("%s%s" % (input_dir, filename), "%s%s" % (output_dir, filename))
