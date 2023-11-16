# -*- coding: utf-8 -*-title
"""
Created on Sun Jul 26 10:51:30 2020

@author: ERIC
"""
#conda install -c conda-forge guidata


import os
import platform
import logging

logging.basicConfig(filename='nmrstatsexcel.log',  level=logging.DEBUG)


# import shutil

import datetime as dt
# from datetime import datetime, date, timedelta
# import datetime as dt
import dateutil


import numpy as np
import pandas as pd

import yaml
import xlsxwriter as xls
import re

import guidata
import guidata.dataset.datatypes as gdt
import guidata.dataset.dataitems as gdi

# global constants
# tuple of bruker spectrometer names
BRUKERSPECTROMETERS = ('n4', 'e7', 'b4', 'a4')
VARIANSPECTROMEETERS = ('p6', 'c5')
SPECTROMETERS = BRUKERSPECTROMETERS + VARIANSPECTROMEETERS

E7N4 = ('e7', 'n4')
A4B4 = ('a4', 'b4')
B4A4 = ('b4', 'a4')
P6C5 = ('p6', 'c5')
N4 = ('n4',)


# from dateutil.rrule import rrule, MONTHLY, YEARLY

with open("main_columns_dict.yml", 'r') as fp:
    main_columns_dict = yaml.safe_load(fp)

nmr_nuclei_list = main_columns_dict['obs_nuc']['n_list']

reverse_nuclei_list = ["".join(re.match(r"([0-9]+)([a-z]+)",
                                        c,
                                        re.I).groups()[::-1])
                       for c in nmr_nuclei_list]

known_groups = ['DP',
 'PRM',
 'MOK',
 'PGS',
 'JWW',
 'MRB',
 'DRWH',
 'EXT',
 'AMOD',
 'AJA',
 'IRB',
 'JAGW',
 'AW',
 'GS',
 'SLC',
 'RAT',
 'CFH',
 'NMS',
 'JWS',
 'RP',
 'Y3L',
 'LRH',
 'JSW',
 'EJG',
 'JWG',
 'PWD',
 'MRG',
 'BZ',
 'IRB',
 'JMS',
 'RAGT',
 'AB',
 'RG',
 'PGS',
 'BZ',
 'DM',
 'CSM',
 'KSC',
 'RK',
 'SJC',
 'HCG',
 'PHA']

def read_procpar(procpar_file: str):
    """read the procpar file and return a dictionary of the parameters"""
    with open(procpar_file, 'r') as f:
        procpar_txt = f.read()
    # process the procpar text by splitting it into chunks of text if first line is a 0
    procpar_chunks = procpar_txt.split('\n')
    procpar = {}
    for i, chunk in enumerate(procpar_chunks):
        words = chunk.split()
        if len(words) == 11:
            ky = words[0]
            val = procpar_chunks[i+1].split()[-1].strip()
            val = val.strip('"')
            procpar[ky] = val
    return procpar

# def read_procpar(procpar_file):
#     """
#     Read the procpar file and return a dictionary of the parameters.

#     Parameters
#     ----------
#     procpar_file : str
#         Path to the procpar file.

#     Returns
#     -------
#     procpar : dict
#         A dictionary containing the parameters.
#     """
#     with open(procpar_file, 'r') as f:
#         procpar_txt = f.read()

#     lines = procpar_txt.splitlines()
#     procpar = {}

#     for i in range(len(lines) - 1):
#         if lines[i].startswith(" 0"):
#             ky = lines[i].split()[1]
#             val = lines[i + 1].split()[-1].strip('"')
#             procpar[ky] = val

#     return procpar


class ExperimentTime:
    """expt_time = ExperimentTime(procpar["time_run"], procpar["time_saved"])"""

    def __init__(self, start_time, end_time, fmt="%Y%m%dT%H%M%S"):

        print(start_time, end_time)
        self.start_time = dt.datetime.strptime(start_time, fmt)
        self.end_time = dt.datetime.strptime(end_time, fmt)
        self.duration = self.end_time - self.start_time

    def in_hours(self):
        return self.duration.total_seconds()/3600

    def in_minutes(self):
        return self.duration.total_seconds()/60

    def in_seconds(self):
        return self.duration.total_seconds()

    def start_time_str(self):
        return self.start_time.strftime("%Y-%m-%d %H:%M:%S")

    def end_time_str(self):
        return self.end_time.strftime("%Y-%m-%d %H:%M:%S")


    def __str__(self):
        expt_mins = self.duration.total_seconds()/60
        return f"Experiment Time: {expt_mins:.2f} minutes"


class NMRstats():
    """
    Holds values of NMR stats dataframe to be queried against

    Attributes
    ----------

    p_spcs : list of str
        Holds spectrometers names that NMR dataframe will be queried against
    p_yrs : list of str
        Holds year names that NMR dataframe will be queried against
    p_mnths : list of str
        Holds month names that NMR dataframe will be queried against
    p_grps : list of str
        Holds group names that NMR dataframe will be queried against

    Methods
    -------
    create_query_str(col_id, col_params)
        Defines and returns query string derived from lists.
    """

    p_spcs = ""
    p_yrs = ""
    p_mnths = ""
    p_grps = ""

    def create_query_str(self, col_id: str, col_params: list) -> str:
        """
        Defines and returns query string derived from lists.

        Parameters
        ----------
        col_id : str
            dataframe column name, typically years, month_name, group_id.
        col_params : list
            List of values that NMR dataframe is to be queried against.

        Returns
        -------
        q_str : str
            A formatted pandas query string.

        """

        if col_id == 'years':
            q_str = "({}=={})".format(col_id, col_params[0])
            print('q_str')
            print(q_str)
            if len(col_params) > 1:
                q_str = "("+q_str
                for s in col_params[1:]:
                    q_str += " or ({}=={})".format(col_id, s)
                q_str += ")"
        else:
            q_str = "({}==\'{}\')".format(col_id, col_params[0])
            print('q_str')
            print(q_str)
            if len(col_params) > 1:
                q_str = "("+q_str
                for s in col_params[1:]:
                    q_str += " or ({}==\'{}\')".format(col_id, s)
                q_str += ")"

        return q_str

    def isolate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Function to reduce NMR dataframe based on lists of years,groups,months

        Parameters
        ----------
        df : pd.DataFrame
            NMR stats dataframe

        Returns
        -------
        df2 : TYPE
            dataframe after query has been performed
        """
        yqstr = self.create_query_str('years', self.p_yrs)
        mqstr = self.create_query_str('month_name', self.p_mnths)
        sqstr = self.create_query_str('spectrometer', self.p_spcs)
        df2 = df.query(yqstr)
        df2 = df2.query(mqstr)
        df2 = df2.query(sqstr)

        return df2





def bruker_find_group_member_grantnumber_sample(path: str):
    """
    Extract group_id, member_id, grantnumber_id, sample_id, and title string from
    Bruker title file

    Parameters
    ----------
    path : str
        directory stem.

    Returns
    -------
    grp_id : str
        name of the group the data belongs to
    member_id : str
        name of the member  the data bleongs to

    grantnumber_id : str
        grant number
    sample_id : str
        sample description
    title0 : str
        string from where group_id, member_id and sample_id have been extracted

    """

    grp_id = 'UNKNOWN'
    member_id = 'UNKNOWN'
    grant_id = 'UNKNOWN'
    sample_id = 'UNKNOWN'
    title0 = 'UNKNOWN'

    titlepath = os.path.join(path, "pdata", "1", "title")
    origpath = os.path.join(path, "orig")

    num_words_in_orig = -1

    # read in grp_id, member_id and grant_id from origpath
    if os.path.exists(origpath):
        with open(origpath, "r", errors='replace') as fp:
            origtxt = fp.readline().strip().split(sep=":")
            if len(origtxt) == 3:
                grp_id, member_id, grant_id = origtxt
                num_words_in_orig = 3
            elif len(origtxt) == 2:
                grp_id, member_id = origtxt
                num_words_in_orig = 2
    else:
        logging.info(f"Could not find orig file in {path}")




    with open(titlepath, "r", errors='replace') as fp:
        title0 = fp.readline().strip()
        # title2 = fp.readline().strip()

    title1 = title0.replace(';', ':')
    title1 = title1.replace('::', '')
    title = title1.strip().split(sep=':')
    if num_words_in_orig == -1:
        # then orig file does not exist
        # check to see if experiment is from teaching labs by looking for "TLabs" in title
        if "Tlabs" in title0:
            grp_id = title[0].upper()
            member_id = title[1].upper()
            if "Teaching" in title0:
                grant_id = "Teaching"
                sample_id = ":".join(title[3:])
            else:
                sample_id = ":".join(title[2:])
    elif num_words_in_orig == 2:
        sample_id = ":".join(title[2:])
    elif num_words_in_orig == 3:
        sample_id = ":".join(title[3:])


    return grp_id, grant_id, member_id, sample_id, title0


def bruker_find_experiment_time(pathf: str, spec_id: str):
    """
    extracts experiment time and start date of NMR experiment from audit file

    Parameters
    ----------
    path : str
        valid file path.
        
        

    Returns
    -------
    time_start: datetime.datetime
        start time of experiments
    time_diff: datetime.timedelta
        duration of experiment

    """
    re_str = r"[0-9]+\-[0-9]+\-[0-9]+\s+\d+:\d+:\d\d"
    time_format_str = '%Y-%m-%d %H:%M:%S'
    if os.path.exists(pathf):
        logname = "-".join((pathf.replace('.','').split(os.path.sep))[-3:]) + '.txt'
#        shutil.copyfile( path, os.path.join("logfiles", logname))
        with open(pathf, "r", errors='replace') as fp:
            auditp = fp.read()

        if spec_id.lower() in A4B4:
            lll = [dt.datetime.strptime(t, '%Y-%m-%d %H:%M:%S') for t in re.findall(re_str, auditp)]
            lll.sort()
        else:
            start_end_time = extract_time(auditp)
            if None in start_end_time:
                lll = []
            else:
                lll = [dt.datetime.strptime(t, "%Y-%m-%d %H:%M:%S") for t in start_end_time]
            
            lll.sort()
            


        try:
            time_end = lll[-1]
            time_start = lll[0]
        except:
            time_start = dt.datetime.now()
            time_end = dt.datetime.now()
            
        # auditp_list = [ vvv + '>)' for vvv in auditp.split('>)\n')]

        # ttt = [ lll[lll.rfind('(  '):].replace('\n', '').strip().split(',') for lll in auditp_list ]


        # tstmps = {}
        # for t in ttt:
        #     if t[0][4:].isnumeric():
        #         tstmps[int(t[0][4:])]= t[1]
                
        # try:        
        #     time_end =  dt.datetime.strptime(re.findall(re_str, tstmps[2])[0], time_format_str) 
        #     time_start = dt.datetime.strptime(re.findall(re_str, tstmps[1])[0], time_format_str) 
        # except:
        #     logging.info("Could not convert time stamp statements")
        #     time_start = dt.datetime.now()
        #     time_end = dt.datetime.now()
            
    else:
        logging.info("Could not find audit file")
        time_start = dt.datetime.now()
        time_end = dt.datetime.now()

    time_diff = time_end-time_start

    return time_start, time_end, time_diff

def extract_time(text):
    re_str = r"[0-9]+\-[0-9]+\-[0-9]+\s+\d+:\d+:\d\d"
    start_time = None
    completed_time = None
    lines = text.split("\n")
    for line in lines:
        if "started at" in line:
            start_time = line.split("started at ")[1].split(",")[0]
            start_time = re.findall(re_str, start_time)[0]
        if "completed at" in line:
            completed_time = line.split("completed at ")[1].split("\n")[0]
            completed_time = re.findall(re_str, completed_time)[0]
    return (start_time, completed_time)

# def bruker_find_experiment_time(pathf: str, spec_id: str):
#     """
#     extracts experiment time and start date of NMR experiment from audit file

#     Parameters
#     ----------
#     path : str
#         valid file path.
        
        

#     Returns
#     -------
#     time_start: datetime.datetime
#         start time of experiments
#     time_diff: datetime.timedelta
#         duration of experiment

#     """
#     re_str = r"[0-9]+\-[0-9]+\-[0-9]+\s+\d+:\d+:\d\d"
#     time_format_str = '%Y-%m-%d %H:%M:%S'
#     if os.path.exists(pathf):
#         logname = "-".join((pathf.replace('.','').split(os.path.sep))[-3:]) + '.txt'
# #        shutil.copyfile( path, os.path.join("logfiles", logname))
#         with open(pathf, "r", errors='replace') as fp:
#             auditalist = fp.readlines()

#         # for spectrometers N4, B4, A4
#         l1 = 6
#         l2 = 8
#         try:
#             if spec_id.lower() == 'e7':
#                 l1 = 9
#                 l2 = 6
#             time_end = datetime.strptime(re.findall(re_str, auditalist[l1])[0],
#                                          time_format_str)
            
#             time_start = datetime.strptime(re.findall(re_str, auditalist[l2])[0],
#                                            time_format_str)
#         except:
#             print("Could not convert time stamp statements")
#             time_start = datetime.now()
#             time_end = datetime.now()
#     else:
#         print("Could not find audit file")
#         time_start = datetime.now()
#         time_end = datetime.now()

#     time_diff = time_end-time_start

#     return time_start, time_end, time_diff



# def bruker_find_experiment_time(path: str):
#     """
#     extracts experiment time and start date of NMR experiment from audit file

#     Parameters
#     ----------
#     path : str
#         valid file path.

#     Returns
#     -------
#     time_start: datetime.datetime
#         start time of experiments
#     time_diff: datetime.timedelta
#         duration of experiment

#     """
#     re_str = r"[0-9]+\-[0-9]+\-[0-9]+\s+\d+:\d+:\d\d"
#     time_format_str = '%Y-%m-%d %H:%M:%S'
#     if os.path.exists(path):
#         logname = "-".join((path.replace('.','').split(os.path.sep))[-3:]) + '.txt'
# #        shutil.copyfile( path, os.path.join("logfiles", logname))
#         with open(path, "r", errors='replace') as fp:
#             auditalist = fp.readlines()

#         try:
#             time_end = datetime.strptime(re.findall(re_str, auditalist[6])[0],
#                                          time_format_str)
#             time_start = datetime.strptime(re.findall(re_str, auditalist[8])[0],
#                                            time_format_str)
#         except:
#             print("Could not convert time stamp statements")
#             time_start = datetime.now()
#             time_end = datetime.now()
#     else:
#         print("Could not find audit file")
#         time_start = datetime.now()
#         time_end = datetime.now()

#     time_diff = time_end-time_start

#     return time_start, time_end, time_diff



def bruker_find_experiment_params_from_acqus(fpath: str):
    """
        Returns pulse program name, solvent and nucleus from Bruker acqus file

    Parameters
    ----------
    path : str
        DESCRIPTION.

    Returns
    -------
    str
        pulse sequence name.
    str
        solvent used in experiment.
    str
        observed nucleus.
    """
    
    sss_list = []
    sss_dict = {}
    
    if not os.path.exists(fpath):
        return 'UNKNOWN', 'UNKNOWN', 'UNKNOWN'
    else:
        with open(fpath, "r") as fp:
            sss_list = [ sss.strip().split('=') for sss in fp if sss.startswith('##')]

        sss_list2 = [ [sss[0][2:].strip('$'), sss[1].strip()] for sss in sss_list]

        for k,v in sss_list2:
            sss_dict[k] = v 
                        
    return  sss_dict['EXP'][1:-1], sss_dict['SOLVENT'][1:-1], sss_dict['NUC1'][1:-1]


def bruker_find_experiment_params(path: str):
    """
    Returns pulse program name, solvent and nucleus from Bruker parm file

    Parameters
    ----------
    path : str
        DESCRIPTION.

    Returns
    -------
    str
        pulse sequence name.
    str
        solvent used in experiment.
    str
        observed nucleus.

    """
    parmdict = {}


    print("**********************************")
    print("\nbruker_find_experiment_params\n",   path)
    print("**********************************")


    with open(path, "r") as fp:
        for line in fp:
            parmlist = line.split()[:2]

            if len(parmlist) == 2:
                parmdict[parmlist[0]] = parmlist[1]

    return parmdict['PULPROG'], parmdict['SOLVENT'], parmdict['NUC1']


def varian_specdate_filename_sequence(path: str):
    """
    extract and return date, filename and sequence from directory path of
    varian NMR data

    Parameters
    ----------
    path : str
        directory path of varian nmr data.

    Returns
    -------
    specdate : str
        folder name holding NMR data eg p62007
    filename : str
        folder name holding set of varian nmr experiments.
        eg jul24162059,  month,day,hour,minutes,seconds.
    sequence_only : str
        name of the nmr sequence corresponding to the data, er. COSY, PROTON.
    expt_num : str
        number of the experiment derived from the specific NMR experiment.
    """

    lll = path.split(os.path.sep)
    specdate, filename, sequence = lll[-3:]
    sequence_id = sequence.split(sep='.')[0]
    try:
        sequence_only, expt_num = sequence_id.rsplit(sep='_', maxsplit=1)
    except:
        sequence_only = sequence_id.rsplit(sep='_', maxsplit=1)
        expt_num = '00'

    if isinstance(sequence_only,list):
        sequence_only = str(sequence_only[0])
        
    return specdate, filename,  sequence_only,  expt_num


def varian_find_group_member_sample(path: str):
    """
    return group_id, member_id, sample_id from title file for varian NMR data

    Parameters
    ----------
    path : str
        DESCRIPTION.

    Returns
    -------
    grp_id : str
        Group name
    member_id : str
        member name
    sample_id : str
        sample description
    title : str
        full title sting from which group, member and sample information
        extracted.

    """
    grp_id = 'UNKNOWN'
    grant_id = 'UNKNOWN'
    member_id = 'UNKNOWN'
    sample_id = 'UNKNOWN'

    with open(path, "r", errors='replace') as fp:
        title1 = fp.readline().strip()
        title2 = fp.readline().strip()

        title = title1 + title2

        g_m_s = re.split(r";*:+;*|:*;+:*", title)

        if len(g_m_s) == 3:
            grp_id, member_id, sample_id = g_m_s
        
        elif len(g_m_s) >= 4:
            if g_m_s[2].upper() in ["UNKNOWN", "NONE"] or g_m_s[2].isnumeric():
                g_m_s = [g_m_s[0], g_m_s[1], g_m_s[2], ":".join(g_m_s[3:])]
                grp_id, member_id, grant_id, sample_id = g_m_s
            else:
                g_m_s = [g_m_s[0], g_m_s[1],  ":".join(g_m_s[2:])]
                grp_id, member_id, sample_id = g_m_s



    return grp_id.upper(), grant_id.upper(), member_id.upper(), sample_id, title



def varian_find_experiment_time(path: str):
    """
    returns the date start time and duration of the varian NMR experiment

    Parameters
    ----------
    path : str
        valid path string of log file holding timing information of the
        NMR experiment

    Returns
    -------
    time_start: datetime.datetime
        start time of experiments
    time_diff: datetime.timedelta
        duration of experiment

    """
    print(path)
    logname = "-".join((path.replace('.','').split(os.path.sep))[-3:]) + ".txt"
#    shutil.copyfile( path, os.path.join("logfiles", logname))
    with open(path, "r", errors='replace') as fp:
        txt = fp.readlines()
        strt = ":".join(txt[0].split(sep=":")[:3])
        end = ":".join(txt[-1].split(sep=":")[:3])
        starttime = dateutil.parser.parse(strt)
        endtime = dateutil.parser.parse(end)

    duration = endtime-starttime
    if (duration.days < 0) or (duration.seconds < 0):

        starttime = dt.datetime.now()
        endtime = dt.datetime.now()
        duration = endtime-starttime

    return starttime, endtime, duration


def varian_find_experiment_params(path: str):
    """
    return solven and observe nucleus from Varian param file

    Parameters
    ----------
    path : str
        valid path to param file

    Returns
    -------
    solvent: str
        name of solvent used in the experiment
    obs_nuc : str
        name of observe nucleus in the experiment

    """

    with open(path, "r", errors='replace') as fp:
        procpar_file = fp.readlines()

    solvent_obs_nuc = [procpar_file[i+1].strip().replace("\"", "").split()[-1]
                        for i, p in enumerate(procpar_file) if ("tn " in p) or ("solvent " in p)]

    if solvent_obs_nuc[0] in reverse_nuclei_list:
        solvent = solvent_obs_nuc[1]
        obs_nuc = solvent_obs_nuc[0]
    else:
        solvent = solvent_obs_nuc[0]
        obs_nuc = solvent_obs_nuc[1] 
        
    return solvent.upper(), obs_nuc


def summary_table(nmrstats: NMRstats,
                  df: pd.DataFrame,
                  rowGroup: list,
                  colGroup: list,
                  pivotcrosstab: str,
                  chosen_grp=None,
                  addTotals=True) -> pd.DataFrame:
    """
    Create a summary table of NMR data based on a pivot or crosstab table


    Parameters
    ----------
    nmrstats : NMRstats
        Holds years, spectrometers and months parameters used to refine the
        main nmr pandas dataframe.
    df : pd.DataFrame
        Main NMR pandas dataframe to be summarized
    rowGroup : list
        Column titles strings from 'df' which makes rows of summary table
    colGroup : list
        Column titles strings from 'df' which makes columns of summary table.
    pivotcrosstab : str
        'pivot' or 'crosstab' depending on the summary table required.
    chosen_grp : TYPE, optional
        String holding name of group_id or member_id to further refine the
        summary table. The default is None.
    addTotals : TYPE, optional
        Bool to choose whether the columns and rows are totalled up and then
        added to the summary table. The default is True.

    Returns
    -------
    df2 : pd.DataFame
        Summary pandas dataframe.

    """

    df2 = nmrstats.isolate_dataframe(df)

    if df2.index.size == 0:
        print( "1 dataframe size = 0")
        return df2, False

    if isinstance(chosen_grp, str):
        df2 = df2[df2.group_id == chosen_grp]

    if df2.index.size == 0:
        print( "2 dataframe size = 0")
        return(df2), False

    good_nuclei = [n for n in nmr_nuclei_list
                   if n in df2.obs_nuc.unique().tolist()]
    print("good_nuclei")
    print(good_nuclei)
    print("df2.obs_nuc.unique().tolist()")
    print(chosen_grp, df2.obs_nuc.unique().tolist())
    
    if good_nuclei == []:
        print("good nuclei is empty")
        return df2, False
    
    df2 = df2.query(" or ".join(["obs_nuc==\'{}\'".format(n)
                                 for n in good_nuclei]))
        
    print(df2.obs_nuc.unique().tolist())

    if df2.index.size == 0:
        print( "3 dataframe size = 0")
        return df2, False
    
    print("rowGroup", rowGroup)
    print("colGroup", colGroup)

    if pivotcrosstab == 'crosstab':

        df2 = pd.crosstab([df2[rg] for rg in rowGroup],
                          [df2[cg] for cg in colGroup])

    elif pivotcrosstab == 'pivot':

        df2 = df2[rowGroup+colGroup].pivot_table(index=rowGroup,
                                                 columns=colGroup[0:-1],
                                                 values=colGroup[-1],
                                                 aggfunc=np.sum)
        df2 = df2.fillna(0)

    #
    # Sort column headings
    #

    if isinstance(df2.columns, pd.core.indexes.multi.MultiIndex):
        for i, cname in enumerate(df2.columns.names):
            if cname in main_columns_dict:
                df2 = df2.reindex(main_columns_dict[cname]['n_list'],
                                  axis=1,
                                  level=i)

        if addTotals:
            df2['Total'] = df2.sum(axis=1)
    else:
        print("sort columns only one level")
        for i, cname in enumerate(df2.columns.names):

            if cname in main_columns_dict:
                sorted_columns = [m for m in main_columns_dict[cname]['n_list']
                                  if m in df2.columns]
                df2 = df2[sorted_columns]

        if addTotals:
            df2['Total'] = df2.sum(axis=1)

    #
    # sort index headings
    #
    print(df2.shape, chosen_grp )
    print("rowGroup", rowGroup)
    print("colGroup", colGroup)
    if isinstance(df2.index, pd.core.indexes.multi.MultiIndex):
        for i, n in enumerate(df2.index.names):
            if n in main_columns_dict:
                df2 = df2.reindex(main_columns_dict[n]['n_list'],
                                  axis=0,
                                  level=i)

        if addTotals:
            print("df2.sum().tolist()")
            print(df2.sum().tolist())
            print("df2.index")
            print(df2.index)
            df2.loc[('Total'), :] = df2.sum().tolist()

    else:
        if df2.index.name in main_columns_dict:
            sorted_columns = [m for m in main_columns_dict[df2.index.name]['n_list']
                              if m in df2.index]

            df2 = df2.reindex(sorted_columns, axis=0)

        if addTotals:
            df2.loc[('Total'), :] = df2.sum().tolist()

    if pivotcrosstab == 'crosstab':

        return df2.astype(int), True
    else:
        return df2, True


def pandas_table_to_excel(workbook: xls.workbook,
                          worksheet: xls.worksheet,
                          df: pd.DataFrame,
                          row_index: int,
                          col_index: int,
                          fmt: str) -> "tuple[int, int]":
    """
    Function to generate a formatted excel version of the pandas dataframe

    Parameters
    ----------
    workbook : xls.workbook
        Current open xlsxwriter workbook.
    worksheet : xls.worksheet
        Current xlsxwriter worksheet to add contents of pandas dataframe to.
    df : pd.DataFrame
        Summarized pandas Dataframe,
        can be multi-index for both rows and columns.
    row_index : int
        row index offset value to place contents of pandas dataframe.
    col_index : int
        column index offset value to place contents of pandas dataframe.
    fmt : str
        Simple xlsxwriter formatter for values '0' for ints, '0.00' for floats.

    Returns
    -------
    row_index : int
        Updated row_index position based on the size of the table.
    col_index : int
        Updated col_index position based on the size of the table.
    """

    bold_left = workbook.add_format({'bold': True,
                                     'align': 'left'})

    bold_right = workbook.add_format({'bold': True,
                                      'align': 'right'})

    bottom_line_bold_left = workbook.add_format({'bottom': 1,
                                                 'bold': True,
                                                 'align': 'left'})

    white_bg_right = workbook.add_format({'pattern': 1,
                                          'bold': False,
                                          'align': 'right',
                                          'bg_color': 'white',
                                          'num_format': fmt})

    grey_bg_right = workbook.add_format({'pattern': 1,
                                         'bold': False,
                                         'align': 'right',
                                         'bg_color': '#F6F2F2',
                                         'num_format': fmt})

    white_bg_left_bold = workbook.add_format({'pattern': 1,
                                              'bold': True,
                                              'align': 'left',
                                              'bg_color': 'white'})

    grey_bg_left_bold = workbook.add_format({'pattern': 1,
                                             'bold': True,
                                             'align': 'left',
                                             'bg_color': '#F6F2F2'})

    white_grey = [grey_bg_right, white_bg_right]
    white_grey_bold = [grey_bg_left_bold, white_bg_left_bold]

    nr, nc = df.shape

    colheadings = []
    for i in range(df.columns.nlevels-1):
        titles = [df.columns.get_level_values(i)[0]]
        for j, c in enumerate(df.columns.get_level_values(i)[1:]):
            if c == df.columns.get_level_values(i)[j]:
                titles.append("\t")
            else:
                titles.append(c)
        colheadings.append(titles)
    colheadings.append(df.columns.get_level_values(-1).tolist())

    rowheadings = []
    for i in range(df.index.nlevels-1):
        titles = [df.index.get_level_values(i)[0]]
        for j, c in enumerate(df.index.get_level_values(i)[1:]):
            if c == df.index.get_level_values(i)[j]:
                titles.append("\t")
            else:
                titles.append(c)
        rowheadings.append(titles)
    rowheadings.append(df.index.get_level_values(-1).tolist())

    num_index_headings = df.index.nlevels-1
    # num_column_headings = len(df1.columns.names)-1

    for i, rt in enumerate(df.columns.names):
        print(rt)
        print(rt.replace('_', ' ').capitalize())
        worksheet.write(row_index,
                        col_index+num_index_headings,
                        rt.replace('_', ' ').capitalize(),
                        bold_left)

        for ic, c in enumerate(colheadings[i]):
            worksheet.write(row_index,
                            col_index+num_index_headings+1+ic,
                            c,
                            bold_right)

        row_index += 1

    for i, rt in enumerate(df.index.names):
        worksheet.write(row_index,
                        col_index+i,
                        rt.replace('_', ' ').capitalize(),
                        bottom_line_bold_left)

    for ic in range(nc):
        worksheet.write(row_index,
                        col_index+num_index_headings+1+ic,
                        " ",
                        bottom_line_bold_left)

    row_index += 1

    #
    # Add row titles and values in the table
    #

    for r in range(nr):
        for k in range(df.index.nlevels):
            worksheet.write(row_index+r,
                            k+col_index,
                            rowheadings[k][r],
                            white_grey_bold[r % 2])

        for c in range(nc):
            worksheet.write(row_index+r,
                            col_index+c+df.index.nlevels,
                            df.values[r, c],
                            white_grey[r % 2])

    return row_index + nr+1, col_index+nc+df.index.nlevels



#class GUInmrstats(dt.DataSet):
#    """Input Dialog"""
#    today = date.today().strftime("%d:%m:%y")
#    yesterday = (date.today() - timedelta(days = 1)).strftime("%d:%m:%y")
#    lastmonth = (date.today() - timedelta(days = 30)).strftime("%d:%m:%y")
#    
#    oneDayOnly = di.BoolItem("Single Day Only", default=True)
#    day_only_str = di.StringItem("Enter Single Date dd:mm:yy", default=yesterday)
#    
#    date_from_str = di.StringItem("Enter Start Date dd:mm:yy",default=lastmonth)
#    date_to_str = di.StringItem("Enter Finish Date dd:mm:yy", default=yesterday)
    
    
class GUInmrstats(gdt.DataSet):
    """Input Dialog"""
    today = dt.date.today().strftime("%d-%m-%y")
    yesterday = (dt.date.today() - dt.timedelta(days = 1)).strftime("%Y-%m-%d")
    _yr, _mth, _dy = [ int(i) for i in yesterday.split("-")]
    yesterdaylist = [ int(i) for i in yesterday.split("-")]
    lastmonth = (dt.date.today() - dt.timedelta(days = 30)).strftime("%Y-%m-%d")
    # yesterday = date.today() - timedelta(days = 1)
    lastmonthlist = [ int(i) for i in lastmonth.split("-")] 

    if platform.system().lower() == 'darwin':
    
        rootDir    = gdi.StringItem("Root Directory", default="/Volumes/nmrdata")
    else:
        rootDir    = gdi.StringItem("Root Directory", default="y:")
    
    oneDayOnly = gdi.BoolItem("Single Day Only", default=True)
    saveDataframe = gdi.BoolItem("Save Pandas DataFrame (csv)", default=True)
    
    # day_only_str = di.StringItem("Enter Single Date dd-mm-yy", default=yesterday)
    # date = gdi.DateItem("Date", default=datetime.date(2010, 10, 10)).set_pos(col=1)
    day_only_str = gdi.DateTimeItem("Enter Single Date dd-mm-yy", default=dt.datetime(*yesterdaylist))
    # print(day_only_str, type(day_only_str))
    
    date_from_str = gdi.DateTimeItem("Enter Start Date dd-mm-yy",default=dt.datetime(*lastmonthlist))
    date_to_str = gdi.DateTimeItem("Enter Finish Date dd-mm-yy", default=dt.datetime(*yesterdaylist))
    
    shortReport = gdi.BoolItem("Short Report Summary", default=True)
    
    reportCounts = gdi.BoolItem("Report Experiments Run", default=True)
    reportTimes = gdi.BoolItem("Report Experiments Times", default=False)
    
    createExcelSheets = gdi.BoolItem("Create Excel Sheets", default=True)



def return_dict_of_nmr_dirs(searchPeriod, root_dir):
    
    # bruker_specs = ['e7', 'a4','b4', 'n4']
    varian_specs = ['p6', 'c5']
    month_str = ['00','01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    
    day_str  = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09',
                '10', '11', '12', '13', '14', '15', '16', '17', '18', '19',
               '20', '21', '22', '23', '24', '25', '26', '27', '28', '29',
               '30', '31']
        
    # column_headers = ['date',
    #                   'spectrometer',
    #                   'file',
    #                   'group_id',
    #                   'member_id',
    #                   'sample_id',
    #                   'title',
    #                   'expt_num',
    #                   'sequence',
    #                   'solvent',
    #                   'obs_nuc',
    #                   'startdate',
    #                   'duration']

    nmrdatadir_dict = {}
    
    # for spec in bruker_specs+varian_specs:
    #     nmr_dirs_dict[spec]=[]
    for date_i in searchPeriod:
        year = date_i.year
        month = date_i.month
        day = date_i.day
        # for spec in bruker_specs:      
        for spec in BRUKERSPECTROMETERS:      
            # datadir = os.path.join(root_dir, os.path.sep, spec+str(year)[-2:]+month_str[month])
            print('os.path.join', root_dir)
            # datadir = os.path.join(root_dir, os.path.sep, spec+str(year)[-2:]+month_str[month]) 
            if spec.lower() in N4:

                datadir = os.path.join(root_dir,  os.path.sep, spec+str(year)[-2:]+month_str[month],  'walkup')
            else:
                datadir = os.path.join(root_dir,  os.path.sep, spec+str(year)[-2:]+month_str[month])

            if os.path.exists(datadir):
                for f in os.listdir(datadir):
                    if f.startswith(day_str[day]):
#                        print( os.path.join(datadir,f), 'in range')
                        if spec not in nmrdatadir_dict:
                            nmrdatadir_dict[spec] = [os.path.join(datadir,f)]
                        else:
                            nmrdatadir_dict[spec].append(os.path.join(datadir,f))
            else:
                print("invalid directory", datadir)

    for date_i in searchPeriod:
        print(date_i)
        year = date_i.year
        month = date_i.month
        day = date_i.day
        for spec in varian_specs:      
            datadir = os.path.join(root_dir, os.path.sep, spec+str(year)[-2:]+month_str[month])        
            if os.path.exists(datadir):
                for f in os.listdir(datadir):
                    if f[3:].startswith(day_str[day]):
#                        print( os.path.join(datadir,f), 'in range')
                        if spec not in nmrdatadir_dict:
                            nmrdatadir_dict[spec] = [os.path.join(datadir,f)]
                        else:
                            nmrdatadir_dict[spec].append(os.path.join(datadir,f))
                            
    return nmrdatadir_dict


def excel_summary_worksheet( df: pd.DataFrame, 
                            workbook: xls.workbook ):
    pass

def startNMRstats():
    
    import sys
    
    _app = guidata.qapplication() # not required if a QApplication has already been created
    
    startDialog = GUInmrstats()
    ok = startDialog.edit()
    
    


    if ok:
    
        root_dir = startDialog.rootDir
    
        if startDialog.oneDayOnly:
            print(startDialog.day_only_str, type(startDialog.day_only_str), str(startDialog.day_only_str))
            print(dir(startDialog.day_only_str))
            print("")
            print("")
            searchPeriod = pd.date_range(dateutil.parser.parse(str(startDialog.day_only_str)[:10], dayfirst=False), 
                                         dateutil.parser.parse(str(startDialog.day_only_str)[:10], dayfirst=False))
            logging.info("searchPeriod", searchPeriod)
        else:
            searchPeriod = pd.date_range(dateutil.parser.parse(str(startDialog.date_from_str)[:10], dayfirst=False), 
                                         dateutil.parser.parse(str(startDialog.date_to_str)[:10], dayfirst=False))
    
    else:
        sys.exit()
        
        
# root_dir = "w:"

    # bruker_specs = ['a4','b4', 'n4']
    # varian_specs = ['p6', 'c5']
    # month_str = ['00','01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    
    # day_str  = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09',
    #             '10', '11', '12', '13', '14', '15', '16', '17', '18', '19',
    #            '20', '21', '22', '23', '24', '25', '26', '27', '28', '29',
    #            '30', '31']
    
    
    nmrdatadir_dict = return_dict_of_nmr_dirs(searchPeriod, root_dir)
        
    column_headers = ['date',
                      'spectrometer',
                      'file',
                      'group_id',
                      'grant_id',
                      'member_id',
                      'sample_id',
                      'title',
                      'expt_num',
                      'sequence',
                      'solvent',
                      'obs_nuc',
                      'startdate',
                      'enddate',
                      'duration']

    raw_data_list = []
     
    for ss, vv in nmrdatadir_dict.items():
        print(ss,vv)
        for v in vv:
            for d in os.listdir(v):
                # print(ss.lower(), "ss.lower() in ['a4', 'b4', 'n4', 'e7']", ss.lower() in ['a4', 'b4', 'n4', 'e7'])
                # if ss.lower() in ['a4', 'b4', 'n4', 'e7']:
                if ss.lower() in BRUKERSPECTROMETERS:
                    if ss.lower() in N4:
                        print(ss.lower())
                    if d.isdigit() and os.path.isdir(os.path.join(v, d)):
                        file_params = v.split(os.path.sep)
                        exptdir = file_params[-1]

                        if ss.lower() in N4:
                            spmnthyr = file_params[-3]
                        else:
                            spmnthyr = file_params[-2]

                        print(v, spmnthyr)
    
                        expt_num = d
                        spectrometer = ss
                        if not os.path.exists(os.path.join(v, d, 'pdata', '1', 'title')):
                            continue
                        if not os.path.exists(os.path.join(v, d, 'audita.txt')):
                            continue                    

                        group_id, grant_id, member_id, sample_id, title = bruker_find_group_member_grantnumber_sample(os.path.join(v, d))
                        date, enddate, duration = bruker_find_experiment_time(os.path.join(v, d, 'audita.txt'), ss)
                        sequence, solvent, nucleus = bruker_find_experiment_params_from_acqus(os.path.join(v, d, 'acqus'))

                        match = re.search(r"[a-zA-Z]+", group_id)
                        # If-statement after search() tests if it succeeded
                        if match:
                            group_id = match.group()
                            if group_id == 'YL':
                                group_id = 'Y3L'
                        else:
                            group_id = "UNKNOWN"
    
                        match = re.search(r"[a-zA-Z]+", member_id)
                        # If-statement after search() tests if it succeeded
                        if match:
                            member_id = match.group()
                            if member_id == 'YL':
                                member_id = 'Y3L'
                        else:
                            member_id = "UNKNOWN"
                        

                            
                        raw_data_list.append([spmnthyr,
                                              spectrometer,
                                              exptdir,
                                              group_id,
                                              grant_id,
                                              member_id,
                                              sample_id,
                                              title,
                                              expt_num,
                                              sequence,
                                              solvent,
                                              nucleus,
                                              date,
                                              enddate,
                                              duration])
                elif ss in ['c5', 'p6', 'e7']:
                    if d.endswith('fid') and 'scout' not in d:
                        if os.path.isdir(os.path.join(v, d)):
                            spectrometer = ss
                            if not os.path.exists(os.path.join(v, d)):
                                continue
                            if not os.path.exists(os.path.join(v, d, 'text')):
                                continue
                            if not os.path.exists(os.path.join(v, d, 'log')):
                                continue
                            if not os.path.exists(os.path.join(v, d,'procpar')):
                                continue

                            grant_id = "UNKNOWN"
                            spmnthyr, exptdir,  sequence, expt_num = varian_specdate_filename_sequence(os.path.join(v, d))
                            group_id, grant_id, member_id, sample_id, title = varian_find_group_member_sample(os.path.join(v, d, 'text'))
                            startdate, enddate, duration = varian_find_experiment_time(os.path.join(v, d, 'log'))
                            solvent, nucleus = varian_find_experiment_params(os.path.join(v, d,'procpar'))

                            # read in the procpar file
                            procpar = read_procpar(os.path.join(v, d,'procpar'))
                            solvent = procpar['solvent']
                            nucleus = procpar['tn']

                            if ss == 'p6':
                                expt_time = ExperimentTime(procpar["time_run"], procpar["time_saved"])
                                startdate = expt_time.start_time
                                enddate = expt_time.end_time
                                duration = expt_time.duration

    
    #                         group_id = re.sub(r"[0-9 .:.;.\..`._.,.]", "", group_id)
    #                         if group_id == 'YL':
    #                             group_id = 'Y3L'
    #                         member_id = re.sub(r"[0-9 .:.;.\..`._.,.]", "",  member_id)
    #                         if member_id == 'YL':
    #                             member_id = 'Y3L'
                                
                            match = re.search(r"[a-zA-Z]+", group_id)
                            # If-statement after search() tests if it succeeded
                            if match:
                                group_id = match.group()
                                if group_id == 'YL':
                                    group_id = 'Y3L'
                            else:
                                group_id = "UNKNOWN"
                            
                            match = re.search(r"[a-zA-Z]+", member_id)
                            # If-statement after search() tests if it succeeded
                            if match:
                                member_id = match.group()
                                if member_id == 'YL':
                                    member_id = 'Y3L'
                            else:
                                member_id = "UNKNOWN"
                            
                            raw_data_list.append([spmnthyr,
                                                  spectrometer,
                                                  exptdir,
                                                  group_id,
                                                  grant_id,
                                                  member_id,
                                                  sample_id,
                                                  title,
                                                  expt_num,
                                                  sequence,
                                                  solvent,
                                                  nucleus,
                                                  startdate,
                                                  enddate,
                                                  duration])
    
    df = pd.DataFrame(raw_data_list, columns=column_headers)
    
    df['startdate'] = pd.to_datetime(df['startdate'])
    df['duration'] = df['duration'].apply(pd.Timedelta)
    
    df['years'] = df.startdate.dt.year
    df['months'] = df.startdate.dt.month
    df['month_name'] = df.startdate.dt.month_name()
    df['days'] = df.startdate.dt.day
    
    df['dys'] = df.duration/pd.Timedelta(1, unit='d')
    
    df['hrs'] = df.duration/pd.Timedelta(1, unit='h')
    
    for c1, c2 in zip(nmr_nuclei_list, reverse_nuclei_list):
        df.obs_nuc.replace(c2, c1, inplace=True)
        
#    df[(df.group_id=="NMR") | (df.group_id=="NMRSERVICE")]['group_id']="NMS"
#    df[(df.group_id=="AMO") | (df.group_id=="AMODK")]['group_id']="AMOD"
#    df[(df.group_id=="IIRB") | (df.group_id=="IRBN") | (df.group_id=="IRBH") | (df.group_id=="IRM")]['group_id']="IRB"
#    df[(df.group_id=="PGSQ")]['group_id']="PGS"
#    df[(df.group_id=="SC")]['group_id']="SLC"
    
    return df, startDialog,raw_data_list

def produceEXCELsheets(df, startDialog):
    
    # prepare root filename for excel file and pandas dataframe csv file
    if startDialog.oneDayOnly:
        fn_str = str(startDialog.day_only_str)[:11].replace("-", "").strip()
        
    else:
        logging.info(str(startDialog.date_from_str))
        logging.info(str(startDialog.date_from_str)[:10])
        fn_str = str(startDialog.date_from_str)[:10].replace("-", "")
        logging.info(str(fn_str))
        fn_str += "_to_"
        logging.info(str(fn_str))
        fn_str += str(startDialog.date_to_str)[:10].replace("-", "") 
        logging.info(str(fn_str))
        
    # save pandas dataframe as csv file
    if startDialog.saveDataframe:
        df_csv = df.copy()
        df_csv['minutes'] = df_csv['duration'].dt.total_seconds()/60
        df_csv['seconds'] = df_csv['duration'].dt.total_seconds()
        df_csv.to_csv("{}.csv".format(fn_str.strip()))
        
    # setup rows and columns for reports depending on short or long report
    if startDialog.shortReport:
        rrrows1 = ['member_id']
        cccols1 = ['group_id']
        rrrows2 = ['member_id']
        cccols2 = ['group_id']
        showTotals = False
    else:
        rrrows1 = ['date',
                   'file',
                   'sample_id',
                   'sequence']
        cccols1 = ['obs_nuc']
        rrrows2 = ['member_id',
                   'date',
                   'file',
                   'sample_id',
                   'sequence']
        cccols2 = ['obs_nuc']
        showTotals = True
    
        
    if startDialog.createExcelSheets:
    
        nmrstats = NMRstats()
        
        nmrstats.p_mnths = df.month_name.unique().tolist()
        nmrstats.p_yrs = df.years.unique().tolist()
        nmrstats.p_spcs = df.spectrometer.unique().tolist()
        
        print(nmrstats.p_mnths)
        print(nmrstats.p_yrs)
        print(nmrstats.p_spcs)
        
        grps = df.group_id.unique().tolist()
        grps.sort()
        
        if "EXT" in grps:
            # put "EXT" at the end of the list
        
            grps.remove("EXT")
            grps.append("EXT")
        
    #    print("{}{}20{}_all.xlsx".format(day_idx,mnth_idx,yr_idx))
            
    #        if startDialog.oneDayOnly:
    #            fn_str = startDialog.day_only_str.replace("-", "")
    #        else:
    #            fn_str = startDialog.date_from_str.replace("-", "")
    #            fn_str += "_to_"
    #            fn_str += startDialog.date_to_str.replace("-", "")
    
    
        workbook = xls.Workbook("{}_all.xlsx".format(fn_str))
    
        worksheet = workbook.add_worksheet("summary")
        num_samples = df.file.nunique()
        print(num_samples)
        worksheet.write(0, 0, num_samples)
        worksheet.write(0, 1,"Samples Ran")
        
        ri = 3
        ci = 0
        
        if startDialog.reportCounts:
            df1, df1_ok = summary_table(nmrstats,
                                df,
                                rowGroup=['group_id',
                                          'member_id',
                                          'date',
                                          'file',
                                          'sample_id',
                                          'sequence'],
                                colGroup=['obs_nuc'],
                                pivotcrosstab='crosstab',
                                addTotals=True)
            print("Summary", df1_ok)
            if df1_ok:
                ri, ci = pandas_table_to_excel(workbook, worksheet, df1, ri, ci, '0')
            
            print('ri,ci',ri,ci)
            
        if startDialog.reportTimes:
            df1, df1_ok = summary_table(nmrstats,
                                df,
                                rowGroup=['group_id',
                                          'member_id',
                                          'date',
                                          'file',
                                          'sample_id',
                                          'sequence'],
                                colGroup=['obs_nuc', 'hrs'],
                                pivotcrosstab='pivot',
                                addTotals=True)
            print("Summary", df1_ok)
            if df1_ok:
                if ci != 0:
                    ci += 2
                
                ri, ci = pandas_table_to_excel(workbook, worksheet, df1, 3, ci, '0.000')
        
        
        #
        # Add groups members worksheet summary
        worksheet = workbook.add_worksheet("MembersByGroups")
        num_samples = df.file.nunique()
        print(num_samples)
        worksheet.write(0, 0, num_samples)
        worksheet.write(0, 1,"Samples Ran")
        
        ri = 3
        ci = 0
        
        if startDialog.reportCounts:
            df1, df1_ok = summary_table(nmrstats,
                                df,
                                rowGroup=['member_id'],
                                colGroup=['group_id'],
                                pivotcrosstab='crosstab',
                                addTotals=True)
            print("MembersByGroups", df1_ok)
            if df1_ok:
                ri, ci = pandas_table_to_excel(workbook, worksheet, df1, ri, ci, '0')
                
            
            print('ri,ci',ri,ci)
            
        if startDialog.reportTimes:
            df1, df1_ok = summary_table(nmrstats,
                                df,
                                rowGroup=['member_id'],
                                colGroup=['group_id', 'hrs'],
                                pivotcrosstab='pivot',
                                addTotals=True)
            print("MembersByGroups", df1_ok)
            if df1_ok:
                if ci != 0:
                    ci += 2
                
                ri, ci = pandas_table_to_excel(workbook, worksheet, df1, 3, ci, '0.0000')    
        
        for grp in grps:
            print(grp)
            ri = 3
            ci = 0
            if grp == "EXT":
                # split into users
                df_ext = df[df.group_id == grp]
        
                for member in df_ext.member_id.unique():
                    
                    ri = 3
                    ci = 0
        
                    df_member = df_ext[df_ext.member_id == member]
        
                    worksheet = workbook.add_worksheet(member)
                    
                    num_samples = df_member.file.nunique()
                    worksheet.write(0, 0, num_samples)
                    worksheet.write(0, 1, "Samples Ran")
                    
                    if startDialog.reportCounts:
                        df1, df1_ok = summary_table(nmrstats,
                                            df_member,
                                            rowGroup=rrrows1,
                                            colGroup=cccols1,
                                            pivotcrosstab='crosstab',
                                            addTotals=showTotals)
            
    
                        print("EXT", df1_ok)
                        if df1_ok:
                            ri, ci = pandas_table_to_excel(workbook,
                                                       worksheet,
                                                       df1,
                                                       3,
                                                       0,
                                                       '0')
                        
                    if startDialog.reportTimes:
                        df1, df1_ok = summary_table(nmrstats,
                                            df_member,
                                            rowGroup=rrrows1,
                                            colGroup=cccols1 + ['hrs'],
                                            pivotcrosstab='pivot',
                                            addTotals=showTotals)
            
                        print("EXT", df1_ok)
                        if df1_ok:
                            if ci != 0:
                                ci += 2
                            ri, ci = pandas_table_to_excel(workbook,
                                                           worksheet,
                                                           df1,
                                                           3,
                                                           ci,
                                                           '0.0000')
        
            else:
                worksheet = workbook.add_worksheet(grp)
                num_samples = df[df.group_id == grp].file.nunique()
                worksheet.write(0, 0, num_samples)
                worksheet.write(0, 1, "Samples Ran")
                
                if startDialog.reportCounts:
                    df1, df1_ok = summary_table(nmrstats,
                                        df,
                                        rowGroup=rrrows2,
                                        colGroup=cccols2,
                                        pivotcrosstab='crosstab',
                                        addTotals=showTotals,
                                        chosen_grp=grp)
        
    
                    print(grp, df1_ok)
                    if df1_ok:
                        ri, ci = pandas_table_to_excel(workbook,
                                                   worksheet,
                                                   df1,
                                                   3, 0,
                                                   '0')
                    
                if startDialog.reportTimes:
                    df1, df1_ok = summary_table(nmrstats,
                                        df,
                                        rowGroup=rrrows2,
                                        colGroup=cccols2 + ['hrs'],
                                        pivotcrosstab='pivot',
                                        addTotals=showTotals,
                                        chosen_grp=grp)
        
                    print(grp, df1_ok)
                    if df1_ok:
                        if ci != 0:
                            ci += 2
                    
                        ri, ci = pandas_table_to_excel(workbook,
                                                       worksheet,
                                                       df1,
                                                       3, ci,
                                                       '0.0000')
        
        workbook.close()
            

        
if __name__ == "__main__":
    
    df, startDialog, raw_data_list = startNMRstats()

    print('df.shape', df.shape)
    
    print("before", df.obs_nuc.unique())
    
    produceEXCELsheets(df, startDialog)
    
    print("after", df.obs_nuc.unique())
    
    