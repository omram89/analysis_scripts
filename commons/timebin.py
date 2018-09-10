from datetime import datetime
from numpy import uint32

__calculators = {}
__format = '%Y-%m-%dT%H:%M:%S:%f'

"""
timebin usage example

min_date = datetime.strptime(df.Timestamp.min(), '%Y-%m-%dT%H:%M:%S:%f')
one_sec_binner = timebin.get_calculator(window, min_date)
df['Timebin'] = df.Timestamp.apply(one_sec_binner)
"""

# TODO: Round down min_date to widow size when calculating timebins

def get_calculator(window, min_date, format=__format):
    key = window + min_date.strftime(format)
    if key not in __calculators:
        def _calculator(item):
            ts = datetime.strptime(item, __format) - min_date
            ts = ts.total_seconds()
            bin_size = timebin_to_milliseconds(window) / 1000
            return uint32(ts // bin_size)
        __calculators[key] = _calculator
    return __calculators[key]

def calculate(item, window, min_date, format=__format):
    calculator = get_calculator(window, min_date, format)
    return calculator(item)

def df_time_binner(df, window):
    """
    Assigns timebin to rows based on the given "window".
    Stores bins in a new column 'TimeBin'.
    Requires the 'Timestamp' column with time in __format format.
    For acceptable window sizes, refer to commons.timebin#timebin_to_milliseconds().
    """
    # Calculate timebin
    min_date = datetime.strptime(df.Timestamp.min(), __format)
    time_binner = get_calculator(window, min_date)
    df['TimeBin'] = df.Timestamp.apply(time_binner)
    return df

def timebin_to_milliseconds(timebin):
    multiplier, count = timebin.split('_')

    if multiplier == 'mins':
        multiplier = 1000 * 60
    elif multiplier == 'hours':
        multiplier = 1000 * 60 * 60
    elif multiplier == 'days':
        multiplier = 1000 * 60 * 60 * 24
    elif multiplier == 'secs':
        multiplier = 1000
    else:
        multiplier = 1000
    
    return multiplier * int(count)
