################################################
#### Written By: SATYAKI DE                 ####
#### Written On: 15-May-2020                ####
####                                        ####
#### Objective: This script is a config     ####
#### file, contains all the keys for        ####
#### Machine-Learning. Application will     ####
#### process these information & perform    ####
#### various analysis on Linear-Regression. ####
################################################

import os
import platform as pl

class clsConfig(object):
    Curr_Path = os.path.dirname(os.path.realpath(__file__))

    os_det = pl.system()
    if os_det == "Windows":
        sep = '\\'
    else:
        sep = '/'

    config = {
        'APP_ID': 1,
        'ARCH_DIR': Curr_Path + sep + 'arch' + sep,
        'PROFILE_PATH': Curr_Path + sep + 'profile' + sep,
        'LOG_PATH': Curr_Path + sep + 'log' + sep,
        'REPORT_PATH': Curr_Path + sep + 'report',
        'FILE_NAME': Curr_Path + sep + 'Data' + sep + 'TradeIn.csv',
        'SRC_PATH': Curr_Path + sep + 'Data' + sep,
        'APP_DESC_1': 'H2O Wave Integration with FinHubb!',
        'DEBUG_IND': 'N',
        'INIT_PATH': Curr_Path,
        'SUBDIR' : 'data',
        'ABLY_ID': 'WWP309489.93jfkT:32kkdhdJjdued79e'
    }
