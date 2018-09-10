import os
import sys
import glob
import numpy  as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt

#sys.path.insert(0, '/home/om/Downloads/netapp_scripts/commons')
#import dataloader
#from dataloader import load_joined_ems_events

COLNAMES_EMS_EVENTS = 'timestamp,seqID,eventType,severity,nodeName,stack,shelf,bay,dev,componentName'.split(',')

def load_ems_events(path=None, low_memory=True, delim=None):

    ems_glob = os.path.join(path, 'ems-events-split*.csv')

    ret_df = None
    
    for filename in sorted(glob.iglob(ems_glob)):
        df = pd.read_csv(filename, delimiter=delim, header=None, names=COLNAMES_EMS_EVENTS, low_memory=low_memory)
        if ret_df is None:
            ret_df = df
        else:
            ret_df = pd.concat([df, ret_df])

    #ret_df = ret_df[np.isfinite(ret_df['stack'])]
    ret_df['stack'].replace('NULL', np.nan, inplace=True)
    ret_df['stack'].replace('', np.nan, inplace=True)
    ret_df['shelf'].replace('', np.nan, inplace=True)

    ret_df.dropna(subset=['stack'], inplace=True)
    ret_df.sort_values(['timestamp','seqID'], inplace=True)
    return ret_df
    #ret_df = ret_df.join(ret_df)


final_df = load_ems_events('/home/om/Downloads/NetApp_dataset/20170522-SystemA/systemA', True,',')

#print(final_df.head(1000))
final_df.hist(column='bay', bins=24)
#bay_fig = bay_hist.get_figure()
#bay_fig.savefig('./bay_hist.pdf')
#bay_hist.plot()
plt.show()

