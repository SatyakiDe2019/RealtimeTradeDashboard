###############################################################
#### Template Written By: H2O Wave                         ####
#### Enhanced with Streaming Data By: Satyaki De           ####
#### Base Version Enhancement On: 20-Dec-2020              ####
#### Modified On 27-Jun-2021                               ####
####                                                       ####
#### Objective: This script will consume real-time         ####
#### streaming data coming out from a hosted API           ####
#### sources (Finnhub) using another popular third-party   ####
#### service named Ably. Ably mimics pubsub Streaming      ####
#### concept, which might be extremely useful for          ####
#### any start-ups.                                        ####
####                                                       ####
#### Note: This is an enhancement of my previous post of   ####
#### H2O Wave. In this case, the application will consume  ####
#### streaming trade data from a live host & not generated ####
#### out of the mock data. Thus, it is more useful for the ####
#### start-ups.                                            ####
###############################################################

import time
from h2o_wave import site, data, ui
from ably import AblyRest
import pandas as p
import json
import datetime
import logging
import platform as pl

from clsConfig import clsConfig as cf
import clsL as cl


# Disbling Warning
def warn(*args, **kwargs):
    pass

import warnings
warnings.warn = warn

# Lookup functions from
# Azure cloud SQL DB

var = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Global Area

## Global Class
# Initiating Log Class
l = cl.clsL()

# Global Variables
# Moving previous day log files to archive directory
log_dir = cf.config['LOG_PATH']

path = cf.config['INIT_PATH']
subdir = cf.config['SUBDIR']

## End Of Global Part

class DaSeries:
    def __init__(self, inputDf):
        self.Df = inputDf
        self.count_row = inputDf.shape[0]
        self.start_pos = 0
        self.end_pos = 0
        self.interval = 1


    def next(self):
        try:
            # Getting Individual Element & convert them to Series
            if ((self.start_pos + self.interval) <= self.count_row):
                self.end_pos = self.start_pos + self.interval
            else:
                self.end_pos = self.start_pos + (self.count_row - self.start_pos)

            split_df = self.Df.iloc[self.start_pos:self.end_pos]

            if ((self.start_pos > self.count_row) | (self.start_pos == self.count_row)):
                pass
            else:
                self.start_pos = self.start_pos + self.interval

            x = float(split_df.iloc[0]['CurrentExchange'])
            dx = float(split_df.iloc[0]['Change'])

            # Emptying the exisitng dataframe
            split_df = p.DataFrame(None)

            return x, dx
        except:
            x = 0
            dx = 0

            return x, dx

class CategoricalSeries:
    def __init__(self, sourceDf):
        self.series = DaSeries(sourceDf)
        self.i = 0

    def next(self):
        x, dx = self.series.next()
        self.i += 1
        return f'C{self.i}', x, dx


light_theme_colors = '$red $pink $purple $violet $indigo $blue $azure $cyan $teal $mint $green $amber $orange $tangerine'.split()
dark_theme_colors = '$red $pink $blue $azure $cyan $teal $mint $green $lime $yellow $amber $orange $tangerine'.split()

_color_index = -1
colors = dark_theme_colors

def next_color():
    global _color_index
    _color_index += 1
    return colors[_color_index % len(colors)]


_curve_index = -1
curves = 'linear smooth step step-after step-before'.split()


def next_curve():
    global _curve_index
    _curve_index += 1
    return curves[_curve_index % len(curves)]

def calc_p(row):
    try:
        str_calc_s1 = str(row['s_x'])
        str_calc_s2 = str(row['s_y'])

        if str_calc_s1 == str_calc_s2:
            calc_p_val = float(row['p_y'])
        else:
            calc_p_val = float(row['p_x'])

        return calc_p_val
    except:
        return 0.0

def calc_v(row):
    try:
        str_calc_s1 = str(row['s_x'])
        str_calc_s2 = str(row['s_y'])

        if str_calc_s1 == str_calc_s2:
            calc_v_val = float(row['v_y'])
        else:
            calc_v_val = float(row['v_x'])

        return calc_v_val
    except:
        return 0.0

def process_DF(inputDF, inputDFUnq):
    try:
        # Core Business logic
        # The application will show default value to any
        # trade-in stock in case that data doesn't consume
        # from the source.
        
        df_conv = inputDF
        df_unique_fin = inputDFUnq

        df_conv['max_count'] = df_conv.groupby('default_rank')['default_rank'].transform('count')
        l.logr('3. max_df.csv', 'Y', df_conv, subdir)


        # Sorting the output
        sorted_df = df_conv.sort_values(by=['default_rank','s'], ascending=True)

        # New Column List Orders
        column_order = ['s', 'default_rank', 'max_count', 'p', 't', 'v']
        df_fin = sorted_df.reindex(column_order, axis=1)

        l.logr('4. sorted_df.csv', 'Y', df_fin, subdir)

        # Now splitting the sorted df into two sets
        lkp_max_count = 4
        df_fin_na = df_fin[(df_fin['max_count'] == lkp_max_count)]

        l.logr('5. df_fin_na.csv', 'Y', df_fin_na, subdir)

        df_fin_req = df_fin[(df_fin['max_count'] != lkp_max_count)]
        l.logr('6. df_fin_req.csv', 'Y', df_fin_req, subdir)

        # Now to perform cross join, we will create
        # a key column in both the DataFrames to
        # merge on that key.
        df_unique_fin['key'] = 1
        df_fin_req['key'] = 1

        # Dropping unwanted columns
        df_unique_fin.drop(columns=['t'], axis=1, inplace=True)
        l.logr('7. df_unique_slim.csv', 'Y', df_unique_fin, subdir)

        # Padding with dummy key values
        #merge_df = p.merge(df_unique_fin,df_fin_req,on=['s'],how='left')
        merge_df = p.merge(df_unique_fin,df_fin_req,on=['key']).drop("key", 1)

        l.logr('8. merge_df.csv', 'Y', merge_df, subdir)

        # Sorting the output
        sorted_merge_df = merge_df.sort_values(by=['default_rank_y','s_x'], ascending=True)

        l.logr('9. sorted_merge_df.csv', 'Y', sorted_merge_df, subdir)

        # Calling new derived logic
        sorted_merge_df['derived_p'] = sorted_merge_df.apply(lambda row: calc_p(row), axis=1)
        sorted_merge_df['derived_v'] = sorted_merge_df.apply(lambda row: calc_v(row), axis=1)

        l.logr('10. sorted_merge_derived.csv', 'Y', sorted_merge_df, subdir)

        # Dropping unwanted columns
        sorted_merge_df.drop(columns=['default_rank_x', 'p_x', 'v_x', 's_y', 'p_y', 'v_y'], axis=1, inplace=True)

        #Renaming the columns
        sorted_merge_df.rename(columns={'s_x':'s'}, inplace=True)
        sorted_merge_df.rename(columns={'default_rank_y':'default_rank'}, inplace=True)
        sorted_merge_df.rename(columns={'derived_p':'p'}, inplace=True)
        sorted_merge_df.rename(columns={'derived_v':'v'}, inplace=True)

        l.logr('11. org_merge_derived.csv', 'Y', sorted_merge_df, subdir)

        # Aligning columns
        column_order = ['s', 'default_rank', 'max_count', 'p', 't', 'v']
        merge_fin_df = sorted_merge_df.reindex(column_order, axis=1)

        l.logr('12. merge_fin_df.csv', 'Y', merge_fin_df, subdir)

        # Finally, appending these two DataFrame (df_fin_na & merge_fin_df)
        frames = [df_fin_na, merge_fin_df]
        fin_df = p.concat(frames, keys=["s", "default_rank", "max_count"])

        l.logr('13. fin_df.csv', 'Y', fin_df, subdir)

        # Final clearance & organization
        fin_df.drop(columns=['default_rank', 'max_count'], axis=1, inplace=True)

        l.logr('14. Final.csv', 'Y', fin_df, subdir)

        # Adjusting key columns
        fin_df.rename(columns={'s':'Company'}, inplace=True)
        fin_df.rename(columns={'p':'CurrentExchange'}, inplace=True)
        fin_df.rename(columns={'v':'Change'}, inplace=True)

        l.logr('15. TransormedFinal.csv', 'Y', fin_df, subdir)

        return fin_df
    except Exception as e:
        print('$' * 120)

        x = str(e)
        print(x)

        print('$' * 120)

        df = p.DataFrame()

        return df


def create_dashboard(update_freq=0.0):
    page = site['/dashboard_finnhub']

    general_log_path = str(cf.config['LOG_PATH'])
    ably_id = str(cf.config['ABLY_ID'])

    # Enabling Logging Info
    logging.basicConfig(filename=general_log_path + 'Realtime_Stock.log', level=logging.INFO)

    os_det = pl.system()

    if os_det == "Windows":
        src_path = path + '\\' + 'data\\'
    else:
        src_path = path + '/' + 'data/'

    # Fetching the data
    client = AblyRest(ably_id)
    channel = client.channels.get('sd_channel')

    message_page = channel.history()

    # Counter Value
    cnt = 0

    # Declaring Global Data-Frame
    df_conv = p.DataFrame()

    for i in message_page.items:
        print('Last Msg: {}'.format(i.data))
        json_data = json.loads(i.data)

        # Converting JSON to Dataframe
        df = p.json_normalize(json_data)
        df.columns = df.columns.map(lambda x: x.split(".")[-1])

        if cnt == 0:
            df_conv = df
        else:
            d_frames = [df_conv, df]
            df_conv = p.concat(d_frames)

        cnt += 1

    # Resetting the Index Value
    df_conv.reset_index(drop=True, inplace=True)

    print('DF:')
    print(df_conv)
    # Writing to the file
    l.logr('1. DF_modified.csv', 'Y', df_conv, subdir)

    # Dropping unwanted columns
    df_conv.drop(columns=['c'], axis=1, inplace=True)

    df_conv['default_rank'] = df_conv.groupby(['s']).cumcount() + 1
    lkp_rank = 1
    df_unique = df_conv[(df_conv['default_rank'] == lkp_rank)]

    # New Column List Orders
    column_order = ['s', 'default_rank', 'p', 't', 'v']
    df_unique_fin = df_unique.reindex(column_order, axis=1)

    print('Rank DF Unique:')
    print(df_unique_fin)

    l.logr('2. df_unique.csv', 'Y', df_unique_fin, subdir)

    # Capturing transformed values into a DataFrame
    # Depending on your logic, you'll implement that inside
    # the process_DF functions
    fin_df = process_DF(df_conv, df_unique_fin)

    df_unq_fin = df_unique_fin.copy()

    df_unq_fin.rename(columns={'s':'Company'}, inplace=True)
    df_unq_fin.rename(columns={'p':'CurrentExchange'}, inplace=True)
    df_unq_fin.rename(columns={'v':'Change'}, inplace=True)
    df_unq_fin.drop(columns=['default_rank','key'], axis=1, inplace=True)

    l.logr('16. df_unq_fin.csv', 'Y', df_unq_fin, subdir)

    df_unq_finale = df_unq_fin.sort_values(by=['Company'], ascending=True)

    l.logr('17. df_unq_finale.csv', 'Y', df_unq_finale, subdir)


    # Final clearance for better understanding of data
    fin_df.drop(columns=['t'], axis=1, inplace=True)

    l.logr('18. CleanFinal.csv', 'Y', fin_df, subdir)

    count_row = df_unq_finale.shape[0]

    large_lines = []
    start_pos = 0
    end_pos = 0
    interval = 1

    # Converting dataframe to a desired Series
    f = CategoricalSeries(fin_df)

    for j in range(count_row):
        # Getting the series values from above
        cat, val, pc = f.next()

        # Getting Individual Element & convert them to Series
        if ((start_pos + interval) <= count_row):
            end_pos = start_pos + interval
        else:
            end_pos = start_pos + (count_row - start_pos)

        split_df = df_unq_finale.iloc[start_pos:end_pos]

        if ((start_pos > count_row) | (start_pos == count_row)):
            pass
        else:
            start_pos = start_pos + interval

        x_currency = str(split_df.iloc[0]['Company'])

        ####################################################
        ##### Debug Purpose                        #########
        ####################################################
        print('Company: ', x_currency)
        print('J: ', str(j))
        print('Cat: ', cat)
        ####################################################
        #####   End Of Debug                         #######
        ####################################################

        c = page.add(f'e{j+1}', ui.tall_series_stat_card(
            box=f'{j+1} 1 1 2',
            title=x_currency,
            value='=${{intl qux minimum_fraction_digits=2 maximum_fraction_digits=2}}',
            aux_value='={{intl quux style="percent" minimum_fraction_digits=1 maximum_fraction_digits=1}}',
            data=dict(qux=val, quux=pc),
            plot_type='area',
            plot_category='foo',
            plot_value='qux',
            plot_color=next_color(),
            plot_data=data('foo qux', -15),
            plot_zero_value=0,
            plot_curve=next_curve(),
        ))
        large_lines.append((f, c))

    page.save()

    while update_freq > 0:

        time.sleep(update_freq)

        for f, c in large_lines:
            cat, val, pc = f.next()

            print('Update Cat: ', cat)
            print('Update Val: ', val)
            print('Update pc: ', pc)
            print('*' * 160)

            c.data.qux = val
            c.data.quux = pc / 100
            c.plot_data[-1] = [cat, val]

        page.save()

if __name__ == "__main__":
    try:
        # Main Calling script
        create_dashboard(update_freq=0.25)
    except Exception as e:
        x = str(e)
        print(x)
