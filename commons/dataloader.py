import os
import glob
import numpy as np
import pandas as pd
#from . import paths

COLNAMES_EMS_EVENTS = 'Timestamp,SequenceId,EventType,SeverityLevel,NodeName,DeviceName,ComponentName'.split(',')
COLNAMES_RAID_GROUP = 'RaidGroupId,DeviceName,DeviceType'.split(',')

CLUSTER_NAMES = ['cluster' + c for c in 'ABCDEFGH']
WORKLOAD_NAMES = ['Workload' + c for c in 'ABCD']

def load_ems_events(cluster_or_workload_name, base_path=None, low_memory=True, delim=None):
    isCluster = 'cluster' in cluster_or_workload_name

    #if base_path:
    #    ems_glob = os.path.join(base_path, cluster_or_workload_name, 'ems-events-*.csv')
    #else:
    #    if isCluster:
    #        ems_glob = os.path.join(paths.CLUSTER_DATA, cluster_or_workload_name, 'ems-events-*.csv')
    #    else:
    #        ems_glob = os.path.join(paths.WORKLOAD_DATA, cluster_or_workload_name, 'ems-events-*.csv')
    ems_glob = os.path.join(base_path, 'ems-events-*.csv')

    final_df = None
    if delim is None:
        if isCluster:
            delim = "\t"
        else:
            delim = " "
    for filename in sorted(glob.iglob(ems_glob)):
        df = pd.read_csv(filename, delimiter=delim, header=None, names=COLNAMES_EMS_EVENTS, low_memory=low_memory)
        if final_df is None:
            final_df = df
        else:
            final_df = pd.concat([df, final_df])
    final_df.sort_values(['Timestamp', 'SequenceId'], inplace=True)
    return final_df

def load_joined_ems_events(cluster_or_workload_name, base_path=None, low_memory=True, delim=None):
    #isCluster = 'cluster' in cluster_or_workload_name
    #if base_path:
    #    ems_glob = os.path.join(base_path, cluster_or_workload_name, 'ems-events-*.csv')
    #else:
    #    if isCluster:
    #        ems_glob = os.path.join(paths.CLUSTER_DATA, cluster_or_workload_name, 'ems-events-*.csv')
    #    else:
    #        ems_glob = os.path.join(paths.WORKLOAD_DATA, cluster_or_workload_name, 'ems-events-*.csv')
    ems_glob = os.path.join(base_path, 'ems-events-*.csv')
    final_df = None
    if delim is None:
        if isCluster:
            delim = "\t"
        else:
            delim = " "
    for filename in sorted(glob.glob(ems_glob)):
        node_name = filename[ filename.find('ems-events-') + len('ems-events-') : -4 ]

        df = pd.read_csv(filename, delimiter=delim, header=None, names=COLNAMES_EMS_EVENTS, low_memory=low_memory)
        df['Node'] = node_name

        raid_filename = filename.replace('ems-events-', 'raid-groups-')
        try:
            raid_df = pd.read_csv(raid_filename, delimiter="\t", header=None, names=COLNAMES_RAID_GROUP, low_memory=low_memory)
            raid_df = raid_df.drop_duplicates(subset=COLNAMES_RAID_GROUP)
            merged = df.merge(raid_df, how='left', left_on=['DeviceName'], right_on=['DeviceName'], sort=False, suffixes=('', ''))
        except FileNotFoundError:
            print(raid_filename, 'not found. Skipping raid stuff.')
            merged = df
            continue

        def get_stack(val):
            try:
                return str(val).split('.')[0]
            except:
                return np.nan
        def get_shelf(val):
            try:
                return str(val).split('.')[1]
            except:
                return np.nan
        def get_bay(val):
            try:
                return str(val).split('.')[2]
            except:
                return np.nan
        def get_bay_x(val):
            try:
                x = str(val).split('.')[0]
                if 'L' in x:
                    parts = x.split('L')
                    x = np.uint8(parts[0]) * np.uint8(parts[1])
                    x = x % 8
                else:
                    x = (np.uint8(x) * 2) % 8
                return x
            except:
                return np.nan
        def get_bay_y(val):
            try:
                y = str(val).split('.')[0]
                if 'L' in y:
                    parts = y.split('L')
                    y = np.uint8(parts[0]) * np.uint8(parts[1])
                    y = y // 8
                else:
                    y = (np.uint8(y) * 2 ) // 8
                return y
            except:
                return np.nan
        merged['Stack'] = merged['DeviceName'].apply(get_stack)
        merged['Shelf'] = merged['DeviceName'].apply(get_shelf)
        merged['Bay'] = merged['DeviceName'].apply(get_bay)
        merged['Bay_X'] = merged['Bay'].apply(get_bay_x)
        merged['Bay_Y'] = merged['Bay'].apply(get_bay_y)

        if final_df is None:
            final_df = merged
        else:
            final_df = pd.concat([merged, final_df])
    if final_df is not None:
        final_df.sort_values(['Timestamp', 'SequenceId'], inplace=True)
        final_df.reset_index(drop=True, inplace=True)
    return final_df

def load_event_names(cluster_or_workload_name, base_path=None, low_memory=True, delim=None):
    isCluster = 'cluster' in cluster_or_workload_name

    if base_path:
        ems_glob = os.path.join(base_path, cluster_or_workload_name, 'ems-events-*.csv')
    else:
        if isCluster:
            ems_glob = os.path.join(paths.CLUSTER_DATA, cluster_or_workload_name, 'ems-events-*.csv')
        else:
            ems_glob = os.path.join(paths.WORKLOAD_DATA, cluster_or_workload_name, 'ems-events-*.csv')
    
    final_df = None
    if delim is None:
        if isCluster:
            delim = "\t"
        else:
            delim = " "
    for filename in sorted(glob.iglob(ems_glob)):
        df = pd.read_csv(filename, delimiter=delim, header=None, names=COLNAMES_EMS_EVENTS, usecols=['EventType'], low_memory=low_memory)
        if final_df is None:
            final_df = df
        else:
            final_df = pd.concat([df, final_df])
    
    return final_df.EventType.unique()
