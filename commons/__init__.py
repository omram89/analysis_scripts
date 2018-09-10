import os
import csv
import json
import matplotlib.pyplot as plt
from matplotlib import rcParams
from . import paths, dataloader

Event_Type_Aliases = None

def set_plot_params(ratio=4/3, width=3.138, large_plot=False, usetex=False, font_size=8):
    # http://jonchar.net/notebooks/matplotlib-styling/
    # https://www.bastibl.net/publication-quality-plots/
    # Get size of figure in inches: 3.138
    # Get font size and family (Times, 10pt for text, min. 8pt for figs, typically 10pt)
    # Use golden ratio. height = width / 1.618
    if large_plot:
        width = width * 1.85
    height = width / ratio
    params = {
        'font.family': 'serif',
        'font.size': font_size,
        'axes.labelsize': font_size,
        'axes.axisbelow': True,
        'legend.fontsize': font_size,
        'xtick.labelsize': font_size, # 8, 10, x-small
        'ytick.labelsize': font_size, # 8, 10, x-small
        'text.usetex': usetex,
        'figure.figsize': [width, height]
    }
    rcParams.update(params)
    plt.rc('text', usetex=usetex)
    return width, height

def event_type_alias(event_type):
    global Event_Type_Aliases
    if Event_Type_Aliases is None:
        __load_event_type_aliases()
    if event_type in Event_Type_Aliases:
        return Event_Type_Aliases[event_type]
    return event_type

def __load_event_type_aliases():
    global Event_Type_Aliases
    Event_Type_Aliases = {}
    cache_path = os.path.join(paths.CACHE, 'event_type_aliases.json')
    map_path = os.path.join(paths.SOURCE_DATA, 'MappedEventNames.csv')
    try:
        with open(cache_path) as fp:
            Event_Type_Aliases = json.load(fp)
    except:
        all_types = set()
        for dataset in [*dataloader.CLUSTER_NAMES, *dataloader.WORKLOAD_NAMES]:
            ev_types = dataloader.load_event_names(dataset)
            ev_types = set(ev_types)
            all_types |= ev_types
        all_types = sorted(list(all_types))
        for i, ev in enumerate(all_types):
            index = 1000 + (i + 1)
            Event_Type_Aliases[ev] = 'Failure-{}'.format(index)
    try:
        with open(map_path) as fp:
            reader = csv.reader(fp)
            dct = {}
            for rows in reader:
                name, pub_name = rows[0].strip(), rows[1].strip()
                name = '_'.join( name.split(' ') ) + '_1'
                Event_Type_Aliases[name] = pub_name
    except:
        pass
    # with open(cache_path, 'w') as fp:
    #     json.dump(Event_Type_Aliases, fp, indent=2)
