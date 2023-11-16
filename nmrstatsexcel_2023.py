# -*- coding: utf-8 -*-title
"""
Created on Sun Jul 26 10:51:30 2020

@author: ERIC
"""
#conda install -c conda-forge guidata


import os
import platform
import logging
from pathlib import Path

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
# BRUKERSPECTROMETERS = ('n4', 'e7', 'b4', 'a4')
# VARIANSPECTROMEETERS = ('p6', 'c5')
# SPECTROMETERS = BRUKERSPECTROMETERS + VARIANSPECTROMEETERS

# E7N4 = ('e7', 'n4')
# A4B4 = ('a4', 'b4')
# B4A4 = ('b4', 'a4')
# P6C5 = ('p6', 'c5')
# N4 = ('n4',)

from os import path
# path_to_dat = path.abspath(path.join(path.dirname(__file__), 'program_configuration_file.yml'))

class ProgramConfig():
    
    fn = Path(path.abspath(path.join(path.dirname(__file__), 'program_configuration_file.yml')))

    with fn.open() as f:
        data = yaml.safe_load(f)

config = ProgramConfig()

# from dateutil.rrule import rrule, MONTHLY, YEARLY

fn =path.abspath(path.join(path.dirname(__file__), "main_columns_dict.yml"))
with open(fn, 'r') as fp:
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
            if len(col_params) > 1:
                q_str = "("+q_str
                for s in col_params[1:]:
                    q_str += " or ({}=={})".format(col_id, s)
                q_str += ")"
        else:
            q_str = "({}==\'{}\')".format(col_id, col_params[0])
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





def bruker_find_group_member_grantnumber_sample(spectrometer: str, path: Path):

    grp_id, member_id, grant_id, sample_id, title0 = 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN'

    titlepath = path / "pdata" / "1" / "title"
    origpath = path / "orig"

    num_words_in_orig = -1

    if origpath.exists():
        with origpath.open("r", errors='replace') as fp:
            origtxt = fp.readline().strip().split(sep=":")
            num_words_in_orig = len(origtxt)
            grp_id, member_id, *grant_id = origtxt
            grant_id = ":".join(grant_id) if grant_id else 'UNKNOWN'

            if spectrometer.lower() == "e7":
                # attempt to split member_id into group_id and member_id on spaces and keep last word
                member_id = member_id.split()[-1]
                grp_id = grp_id.split()[-1]
                grant_id = grant_id.split()[-1]

            # find sample_id from title file
            with titlepath.open("r", errors='replace') as fp:
                title0 = fp.readline().strip()
                # split title on :  and keep last word
                sample_id = title0.split(sep=":")[-1]

    else:
        logging.info(f"Could not find orig file in {path}")

        with titlepath.open("r", errors='replace') as fp:
            title0 = fp.readline().strip()

        title1 = title0.replace(';', ':').replace('::', '')
        title = title1.strip().split(sep=':')

        if num_words_in_orig == -1:
            if "TLABS" in title0.upper():
                grp_id, member_id, grant_id = map(str.upper, title[:3])
                # grant_id = "Teaching" if "Teaching" in title0 else 'UNKNOWN'
                sample_id = ":".join(title[3:]) if len(title) > 3 else 'UNKNOWN'
            else:
                sample_id = ":".join(title[2:]) if len(title) > 2 else 'UNKNOWN'
        elif num_words_in_orig == 2:
            sample_id = ":".join(title[2:]) if len(title) > 2 else 'UNKNOWN'
        elif num_words_in_orig == 3:
            sample_id = ":".join(title[3:]) if len(title) > 3 else 'UNKNOWN'

    return grp_id.upper(), grant_id.upper(), member_id.upper(), sample_id, title0


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


def bruker_find_experiment_time(pathf: Path, spec_id: str):
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
    if pathf.exists():

        with pathf.open("r", errors='replace') as fp:
            auditp = fp.read()

        if spec_id.lower() in config.data["A4B4"]:
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
                      
    else:
        logging.info("Could not find audit file")
        time_start = dt.datetime.now()
        time_end = dt.datetime.now()

    time_diff = time_end-time_start

    return time_start, time_end, time_diff



def bruker_find_experiment_params_from_acqus(fpath: Path):
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
    
    if not fpath.exists():
        return 'UNKNOWN', 'UNKNOWN', 'UNKNOWN'
    else:
        with fpath.open("r") as fp:
            sss_list = [ sss.strip().split('=') for sss in fp if sss.startswith('##')]

        sss_list2 = [ [sss[0][2:].strip('$'), sss[1].strip()] for sss in sss_list]

        for k,v in sss_list2:
            sss_dict[k] = v 
                        
    return  sss_dict['EXP'][1:-1], sss_dict['SOLVENT'][1:-1], sss_dict['NUC1'][1:-1]


def bruker_find_experiment_params_from_acqus(fpath: Path):
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
    
    if not fpath.exists():
        return 'UNKNOWN', 'UNKNOWN', 'UNKNOWN'
    else:
        with fpath.open("r") as fp:
            sss_list = [ sss.strip().split('=') for sss in fp if sss.startswith('##')]

        sss_list2 = [ [sss[0][2:].strip('$'), sss[1].strip()] for sss in sss_list]

        for k,v in sss_list2:
            sss_dict[k] = v 
                        
    return  sss_dict['EXP'][1:-1], sss_dict['SOLVENT'][1:-1], sss_dict['NUC1'][1:-1]


def varian_specdate_filename_sequence(path: Path):
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

    specdate, filename, sequence = path.parts[-3:]
    sequence_id = sequence.split(sep='.')[0]
    try:
        sequence_only, expt_num = sequence_id.rsplit(sep='_', maxsplit=1)
    except:
        sequence_only = sequence_id.rsplit(sep='_', maxsplit=1)
        expt_num = '00'

    if isinstance(sequence_only,list):
        sequence_only = str(sequence_only[0])
        
    return specdate, filename,  sequence_only,  expt_num


def varian_find_group_member_sample(path: Path):
    """
    return group_id, member_id, sample_id from title file for varian NMR data

    Parameters
    ----------
    path : Path
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

    with path.open("r", errors='replace') as fp:

        title1 = fp.readline().strip()
        title2 = fp.readline().strip()
        title = title1 + title2

        g_m_s = re.split(r";*:+;*|:*;+:*", title)

        if len(g_m_s) == 3:
            grp_id, member_id, sample_id = g_m_s

        elif len(g_m_s) == 4:
            grp_id, member_id, grant_id, sample_id = g_m_s
        
        elif len(g_m_s) >= 4:
            if g_m_s[2].upper() in ["UNKNOWN", "NONE"] or g_m_s[2].isnumeric():
                g_m_s = [g_m_s[0], g_m_s[1], g_m_s[2], ":".join(g_m_s[3:])]
                grp_id, member_id, grant_id, sample_id = g_m_s
            else:
                g_m_s = [g_m_s[0], g_m_s[1],  ":".join(g_m_s[2:])]
                grp_id, member_id, sample_id = g_m_s

    return grp_id.upper(), grant_id.upper(), member_id.upper(), sample_id, title



def varian_find_experiment_time(path: Path):
    """
    returns the date start time and duration of the varian NMR experiment

    Parameters
    ----------
    path : str
        valid path string of log file holding timing information of the
        NMR experiment

    Returns
    -------
    strt: str
    time_start: datetime.datetime
        start time of experiments
    time_diff: datetime.timedelta
        duration of experiment

    """

    with path.open("r", errors='replace') as fp:
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

    return  starttime, endtime, duration


def varian_find_experiment_params(path: Path):
    """
    return solven and observe nucleus from Varian param file

    Parameters
    ----------
    path : Path
        valid path to param file

    Returns
    -------
    solvent: str
        name of solvent used in the experiment
    obs_nuc : str
        name of observe nucleus in the experiment

    """

    with path.open("r", errors='replace') as fp:
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
        return df2, False

    if isinstance(chosen_grp, str):
        df2 = df2[df2.group_id == chosen_grp]

    if df2.index.size == 0:
        return(df2), False

    good_nuclei = [n for n in nmr_nuclei_list
                   if n in df2.obs_nuc.unique().tolist()]
    
    if good_nuclei == []:
        return df2, False
    
    df2 = df2.query(" or ".join(["obs_nuc==\'{}\'".format(n)
                                 for n in good_nuclei]))
        
    if df2.index.size == 0:
        return df2, False
    
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
    if isinstance(df2.index, pd.core.indexes.multi.MultiIndex):
        for i, n in enumerate(df2.index.names):
            if n in main_columns_dict:
                df2 = df2.reindex(main_columns_dict[n]['n_list'],
                                  axis=0,
                                  level=i)

        if addTotals:
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
        rootDir    = gdi.StringItem("Root Directory", default="y:/")
    
    oneDayOnly = gdi.BoolItem("Single Day Only", default=True)
    saveDataframe = gdi.BoolItem("Save Pandas DataFrame (csv)", default=True)
    
    day_only_str = gdi.DateTimeItem("Enter Single Date dd-mm-yy", default=dt.datetime(*yesterdaylist))
    
    date_from_str = gdi.DateTimeItem("Enter Start Date dd-mm-yy",default=dt.datetime(*lastmonthlist))
    date_to_str = gdi.DateTimeItem("Enter Finish Date dd-mm-yy", default=dt.datetime(*yesterdaylist))
    
    shortReport = gdi.BoolItem("Short Report Summary", default=True)
    
    reportCounts = gdi.BoolItem("Report Experiments Run", default=True)
    reportTimes = gdi.BoolItem("Report Experiments Times", default=False)
    
    createExcelSheets = gdi.BoolItem("Create Excel Sheets", default=True)

    costPerHour = gdi.FloatItem("Cost per Hour (£/hr)", default=28.0)



def return_dict_of_nmr_dirs(searchPeriod, root_dir):
    varian_specs = ['p6', 'c5']
        
    nmrdatadir_dict = {}

    print(config.data["BRUKERSPECTROMETERS"])
    for date_i in searchPeriod:
        year = str(date_i.year)[-2:]
        month = f"{date_i.month:02d}"
        day = f"{date_i.day:02d}"
        for spec in config.data["BRUKERSPECTROMETERS"]:
            print(spec)
            if spec.lower() in config.data["N4"]:
                datadir = Path(root_dir, (spec + year + month), 'walkup' )
            else:
                datadir = Path(root_dir,  (spec + year + month))
            print(spec, datadir)
            if datadir.exists():
                print("day", day)
                for f in datadir.iterdir():
                    print(f.name, day)
                    if f.name.startswith(day):
                        if spec not in nmrdatadir_dict:
                            nmrdatadir_dict[spec] = [f]
                        else:
                            nmrdatadir_dict[spec].append(f)
            else:
                print("invalid directory", datadir)

    for date_i in searchPeriod:
        year = str(date_i.year)[-2:]
        month = f"{date_i.month:02d}"
        day = f"{date_i.day:0d}"
        for spec in config.data["VARIANSPECTROMETERS"]:
            datadir = Path(root_dir, (spec + year + month))
            if datadir.exists():
                for f in datadir.iterdir():
                    if f.name[3:].startswith(day):
                        if spec not in nmrdatadir_dict:
                            nmrdatadir_dict[spec] = [f]
                        else:
                            nmrdatadir_dict[spec].append(f)

            else:
                print("invalid directory", datadir)

    return nmrdatadir_dict


def excel_summary_worksheet( df: pd.DataFrame, 
                            workbook: xls.workbook ):
    pass


class BrukerSpectrometer:

    def __init__(self, specName: str):
        self.specName = specName

    def retrieve_parameters(self, dirpath: Path):

        return retrieve_parameters_from_bruker(self.specName, dirpath)

    
class VarianSpectrometer:

    def __init__(self, specName: str):
        self.specName = specName
    
    def retrieve_parameters(self,  dirpath: Path):
        
        return retrieve_parameters_from_varian(self.specName, dirpath)
    
class UnknownSpectrometer:

    def __init__(self, specName: str):
        self.specName = specName

    def retrieve_parameters(self, dirpath: Path):

        return []
    
def spectrometer(spectrometerName: str):
 
    """Factory Method"""
    localizers = {
        "a4": BrukerSpectrometer,
        "b4": BrukerSpectrometer,
        "n4": BrukerSpectrometer,
        "e7": BrukerSpectrometer,
        "c5": VarianSpectrometer,
        "p6": VarianSpectrometer

    }

    return localizers.get(spectrometerName, UnknownSpectrometer)(spectrometerName)


        
def retrieve_parameters(ss, vv, id)->list:
    if id == "bruker":
        params = retrieve_parameters_from_bruker(ss, vv)
    elif id == "varian":
        params = retrieve_parameters_from_varian(ss, vv)
    elif id == "unknown":
        params = []

    return params


def which_manufacturer(ss):
    if ss.lower() in config.data["BRUKERSPECTROMETERS"]:
        id = "bruker"
    elif ss.lower() in config.data["VARIANSPECTROMETERS"]:
        id = "varian"
    else:
        id = "unknown"

    return id

def retrieve_parameters_from_bruker(ss: str, vv: list)->list:
    """ss: spectrometer name [b4, a4, n4, e7]
       vv: list of valid Paths to bruker data directories
       
       returns list of lists of parameters for each experiment"""

    raw_data_list = []
    for v in vv:

        bruker_expt_dirs = [ d for d in v.iterdir() if d.is_dir() and d.name[-1].isnumeric()]
            
        for d in bruker_expt_dirs:
            
            exptdir = v.parts[-1]

            if ss.lower() in config.data["N4"]:
                spmnthyr = v.parts[-3]
            else:
                spmnthyr = v.parts[-2]

            expt_num = d.parts[-1]
            spectrometer = ss
            if not Path(d, 'pdata', '1', 'title').exists():
                continue
            if not Path(d, 'audita.txt').exists():
                continue

            group_id, grant_id, member_id, sample_id, title = bruker_find_group_member_grantnumber_sample(ss, d)
            date, enddate, duration = bruker_find_experiment_time( Path(d, 'audita.txt'), ss)
            sequence, solvent, nucleus = bruker_find_experiment_params_from_acqus(Path(d, 'acqus'))

            match = re.search(r"[a-zA-Z]+", group_id)
            # If-statement after search() tests if it succeeded
            if match:
                group_id = match.group()
                if group_id == 'YL':
                    group_id = 'Y3L'
            else:
                group_id = "unknown"

            match = re.search(r"[a-zA-Z]+", member_id)
            # If-statement after search() tests if it succeeded
            if match:
                member_id = match.group()
                if member_id == 'YL':
                    member_id = 'Y3L'
            else:
                member_id = "unknown"

            # look up group id in dictionary to get full name of principal investigator
                    
            group_id = config.data["pi_code_to_name_map"].get(group_id, group_id)
            
            raw_data_list.append([spmnthyr,
                                    spectrometer,
                                    exptdir,
                                    group_id.upper(),
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
                                    duration, 
                                    str(d)])
                    
    return raw_data_list


def retrieve_parameters_from_varian(ss: str, vv: list)->list:
    """ss: spectrometer name [c5, p6]
       vv: list of valid Paths to varian data directories
       
       returns list of lists of parameters for each experiment"""
                    
    raw_data_list = []
    for v in vv:

        # remove any files that have scout in the name and files that do not end in fid
        # bruker_expt_dirs = [ d for d in v.iterdir() if d.is_dir() and d.name[-1].isnumeric()]
        varian_expt_dirs = [d for d in v.iterdir() if not "scout" in d.name.lower() and d.suffix.lower() == ".fid" and d.is_dir()]
            
        for d in varian_expt_dirs:
            spectrometer = ss
            if not Path(d, 'text').exists():
                continue

            if not Path(d, 'log').exists():
                continue
            if not Path(d,'procpar').exists():
                continue

            grant_id = "unknown"
            spmnthyr, exptdir,  sequence, expt_num = varian_specdate_filename_sequence(d)
            group_id, grant_id, member_id, sample_id, title = varian_find_group_member_sample(Path(d, 'text'))
            startdate, enddate, duration = varian_find_experiment_time(Path(d, 'log'))
            solvent, nucleus = varian_find_experiment_params(Path(d,'procpar'))

            # read in the procpar file
            procpar = read_procpar(Path(d,'procpar'))
            solvent = procpar['solvent']
            nucleus = procpar['tn']

            if ss == 'p6':
                expt_time = ExperimentTime(procpar["time_run"], procpar["time_saved"])
                startdate = expt_time.start_time
                enddate = expt_time.end_time
                duration = expt_time.duration
                                
            match = re.search(r"[a-zA-Z]+", group_id)
            # If-statement after search() tests if it succeeded
            if match:
                group_id = match.group()
                if group_id == 'YL':
                    group_id = 'Y3L'
            else:
                group_id = "unknown"
            
            match = re.search(r"[a-zA-Z]+", member_id)
            # If-statement after search() tests if it succeeded
            if match:
                member_id = match.group()
                if member_id == 'YL':
                    member_id = 'Y3L'
            else:
                member_id = "unknown"

            group_id = config.data["pi_code_to_name_map"].get(group_id, group_id) 

            raw_data_list.append([spmnthyr,
                                    spectrometer,
                                    exptdir,
                                    group_id.upper(),
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
                                    duration, 
                                    str(d)])
            
    return raw_data_list

def startNMRstats():
    
    import sys
    
    _app = guidata.qapplication() # not required if a QApplication has already been created
    
    startDialog = GUInmrstats()
    ok = startDialog.edit()
    
    if ok:
    
        root_dir = startDialog.rootDir
    
        if startDialog.oneDayOnly:
            searchPeriod = pd.date_range(dateutil.parser.parse(str(startDialog.day_only_str)[:10], dayfirst=False), 
                                         dateutil.parser.parse(str(startDialog.day_only_str)[:10], dayfirst=False))
            
        else:
            searchPeriod = pd.date_range(dateutil.parser.parse(str(startDialog.date_from_str)[:10], dayfirst=False), 
                                         dateutil.parser.parse(str(startDialog.date_to_str)[:10], dayfirst=False))
        logging.info("searchPeriod", searchPeriod)
    else:
        sys.exit()
    
    print("searchPeriod\n", searchPeriod)
    nmrdatadir_dict = return_dict_of_nmr_dirs(searchPeriod, root_dir)
    print("nmrdatadir_dict\n", nmrdatadir_dict)

    print("nmrdatadir_dict\n", nmrdatadir_dict)
    logging.info("nmrdatadir_dict\n", nmrdatadir_dict)
    raw_data_list = []

    for ss, vv in nmrdatadir_dict.items():
        # id = which_manufacturer(ss)
        # raw_data_list.extend(retrieve_parameters(ss, vv, id))
        raw_data_list.extend( spectrometer(ss).retrieve_parameters(vv))
       
     
    
    df = pd.DataFrame(raw_data_list, columns=config.data["column_headers"])
    
    df['startdate'] = pd.to_datetime(df['startdate'])
    df['duration'] = df['duration'].apply(pd.Timedelta)
    
    df['years'] = df.startdate.dt.year
    df['months'] = df.startdate.dt.month
    df['month_name'] = df.startdate.dt.month_name()
    df['days'] = df.startdate.dt.day
    
    df['dys'] = df.duration/pd.Timedelta(1, unit='d')
    
    df['hrs'] = df.duration/pd.Timedelta(1, unit='h')

    df['costperhour'] = startDialog.costPerHour
    
    df['hourly_cost'] = df['hrs']*startDialog.costPerHour
    
    for c1, c2 in zip(nmr_nuclei_list, reverse_nuclei_list):
        df.obs_nuc.replace(c2, c1, inplace=True)
        

    
    return df, startDialog, raw_data_list

def produceEXCELsheets(df, startDialog):
    
    # prepare root filename for excel file and pandas dataframe csv file
    print("startDialog.date_from_str:::", str(startDialog.date_from_str), type(startDialog.date_from_str))
    print("month", startDialog.date_from_str.strftime("%b"))
    print(dir(startDialog.date_from_str))
    if startDialog.oneDayOnly:
        fn_str = str(startDialog.day_only_str)[:11].replace("-", "").strip()
        monthyear_str = startDialog.day_only_str.strftime("%b%Y")
        
        
    else:
        logging.info(str(startDialog.date_from_str))
        logging.info(str(startDialog.date_from_str)[:10])
        fn_str = str(startDialog.date_from_str)[:10].replace("-", "")
        logging.info(str(fn_str))
        fn_str += "_to_"
        logging.info(str(fn_str))
        fn_str += str(startDialog.date_to_str)[:10].replace("-", "") 
        logging.info(str(fn_str))
        monthyear_str = startDialog.date_from_str.strftime("%b%Y")
        
    # save pandas dataframe as csv file
    if startDialog.saveDataframe:
        df_csv = df.copy()
        try:
            df_csv['minutes'] = df_csv['duration'].dt.total_seconds()/60
            df_csv['seconds'] = df_csv['duration'].dt.total_seconds()
        except AttributeError:
            print("datetime conversion error")
            # print type of duration column
            print(df_csv['duration'].dtype)
            print(df_csv.head())

        df_csv.to_csv("{}.csv".format(fn_str.strip()))

        # save monthly summary as csv file
        # copy certain columns to new dataframe

        cols = list(config.data["grant_to_column_map"].values())
        df_monthly_csv = df_csv[[c for c in cols if c in df_csv.columns]].copy()
        df_monthly_csv["Service"] = "Liquid State NMR"
        df_monthly_csv["Charging model"] = "Standard per hour"
        df_monthly_csv = df_monthly_csv[cols]
        df_monthly_csv.columns = list(config.data["grant_to_column_map"].keys())

        df_monthly_csv.to_csv("liquid_state_NMR_monthly_report_{}.csv".format(monthyear_str))

        # save monthly summary as excel file
        df_monthly_csv.to_excel("liquid_state_NMR_monthly_report_{}.xlsx".format(monthyear_str), index=False)
        
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
        
        grps = df.group_id.unique().tolist()
        grps.sort()
        
        if "EXT" in grps:
            # put "EXT" at the end of the list
        
            grps.remove("EXT")
            grps.append("EXT")
        
        workbook = xls.Workbook("{}_all.xlsx".format(fn_str))
    
        worksheet = workbook.add_worksheet("summary")
        num_samples = df.file.nunique()
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
            if df1_ok:
                ri, ci = pandas_table_to_excel(workbook, worksheet, df1, ri, ci, '0')
            
            
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
            if df1_ok:
                if ci != 0:
                    ci += 2
                
                ri, ci = pandas_table_to_excel(workbook, worksheet, df1, 3, ci, '0.000')
        
        
        #
        # Add groups members worksheet summary
        worksheet = workbook.add_worksheet("MembersByGroups")
        num_samples = df.file.nunique()
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
            if df1_ok:
                ri, ci = pandas_table_to_excel(workbook, worksheet, df1, ri, ci, '0')
            
        if startDialog.reportTimes:
            df1, df1_ok = summary_table(nmrstats,
                                df,
                                rowGroup=['member_id'],
                                colGroup=['group_id', 'hrs'],
                                pivotcrosstab='pivot',
                                addTotals=True)
            if df1_ok:
                if ci != 0:
                    ci += 2
                
                ri, ci = pandas_table_to_excel(workbook, worksheet, df1, 3, ci, '0.0000')    
        
        for grp in grps:
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
    
    