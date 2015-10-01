import pandas as pd
import numpy as np
import seaborn as sns
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

SIS_CLASS = ['AMPHIBIA', 'MAMMALIA', 'AVES']

class field_missing_exception(Exception):
    pass

def species_irreplaceability(x):
    """x is the percentage in decimal"""
    x = x*100

    def h(x):
    # h(x) as specified
        miu = 39
        s = 9.5
        tmp = -(x - miu)/ s
        denominator = 1 + np.exp(tmp)
        return 1/denominator
    

    return (h(x) - h(0))/(h(100) - h(0))


def pa_sr(wdpaid, ir_dataframe):
    if 'ir' not in ir_dataframe.columns or 'wdpaid' not in ir_dataframe.columns:
        print('Field doesn\'t exist')
        raise field_missing_exception

    else:
        condition = ir_dataframe['wdpaid'] == wdpaid
        return ir_dataframe[condition]['ir'].sum()


def pa_group_sr(ir_dataframe):
    # more efficient
    data_group = ir_dataframe.groupby('wdpaid')
    return data_group['ir'].sum()


def get_class_sis(df, sis_class, is_thr=False):
    sis_class = sis_class.upper()
    if sis_class not in SIS_CLASS:
        raise Exception('Class must be birds, mammals and amphibans')
    else:
        if not is_thr:
            return df[df['class'] == sis_class]
        else:
            return df[(df['class'] == sis_class)&((df['rl']=='VU')|(df['rl']=='EN')|(df['rl']=='CR'))]

def summarise_ir(df):
    # result df
    return df.groupby('wdpaid')['ir'].sum()

def get_ir(df):
    # result df
    m = pd.DataFrame(summarise_ir(get_class_sis(df, 'MAMMALIA')))
    a = pd.DataFrame(summarise_ir(get_class_sis(df, 'AMPHIBIA')))
    b = pd.DataFrame(summarise_ir(get_class_sis(df, 'AVES')))
    m_t = pd.DataFrame(summarise_ir(get_class_sis(df, 'MAMMALIA', True)))
    a_t = pd.DataFrame(summarise_ir(get_class_sis(df, 'AMPHIBIA', True)))
    b_t = pd.DataFrame(summarise_ir(get_class_sis(df, 'AVES', True)))

    m.columns = ['ir_mammal']
    a.columns = ['ir_amphbian']
    b.columns = ['ir_bird']
    m_t.columns = ['ir_mammal_thr']
    a_t.columns = ['ir_amphibian_thr']
    b_t.columns = ['ir_bird_thr']

    return m, m_t, a, a_t, b, b_t

def get_df_wdpa(df):
    # result df
    pa = pd.DataFrame(df['wdpaid'].unique(), columns=['wdpaid'])
    return pa

def con_cat(df, *sum_dfs):
    result = df
    for sum_df in sum_dfs:
        result = pd.merge(result, sum_df, how='left', left_on='wdpaid', right_index=True)

    return result


# load data and preprocess
def init():
    engine = create_engine('postgresql://postgres:gisintern@localhost/whs_v2')
    intersect_data = pd.read_sql_table('wdpa_rl_2014aug', engine, schema='ad_hoc',columns=['wdpaid', 'id_no', 'intersected_area_km', 'total_area_km'])
    intersect_data.columns=['wdpaid', 'id_no', 'intersect', 'total']
    intersect_data['per'] = intersect_data['intersect']/intersect_data['total']

    # calculating sr
    intersect_data['ir'] = species_irreplaceability(intersect_data['per'])
    
    return intersect_data



def run():
    # # get ir calculation from db
    # intersect_data = init()

    # # write to csv to save recomputing
    # intersect_data.to_csv('ir_mk2.csv')
    # intersect_data[['wdpaid', 'id_no', 'ir']].to_csv('ir_short_mk2.csv')

    # reload if already calculated
    intersect_data = pd.read_csv('ir_mk2.csv')

    # make sure only overlaps with per >0.05 are taken into consideration
    intersect_data = intersect_data[intersect_data['per']>0.05]

    # load sis attribute
    sis = load_sis()
    
    # join sis table  
    result = join_sis(intersect_data, sis)

    # get a list of df of grouped ir
    dfs = get_ir(result)

    # get a unique list of wdpaid as dataframe
    pa = get_df_wdpa(result)

    # join data frame together
    output = con_cat(pa, *dfs)

    # fill NaN with 0 (left join produces NaN if no match)
    output = output.fillna(0)

    # final grouping
    output['ir'] = output['ir_mammal'] + output['ir_amphbian'] + output['ir_bird']
    output['ir_thr'] = output['ir_mammal_thr'] + output['ir_amphibian_thr'] + output['ir_bird_thr']

    # save result
    output.to_csv('result_ir_2015_mk2.csv')




# load csv table as a result of the above process
def reload_ir_short():
    short = pd.read_csv('ir_short.csv')
    return short

def reload_ir_full():
    full_data = pd.read_csv('ir.csv')
    return full_data

def load_sis():
# load sis and filter
    sis = pd.read_csv('sis.csv')

    # only interseted in MAB: note brackets are needed
    sis = sis[(sis['class']=='AMPHIBIA')|(sis['class']=='MAMMALIA')|(sis['class']=='AVES')]
    return sis

def join_sis(short, sis):
    # join
    result = pd.merge(short, sis, how='left', on='id_no')
    result = result[(result['rl']!='DD') & (result['rl']!='EW') &(result['rl']!='EX')]

    # get rid of unwanted columns
    result = result.drop(['OID', 'scientific', 'kingdom', 'phylum', 'order_', 'family', 'genus', 'species'],
        axis = 1)

    return result

# def main():
#     short= reload_ir_short()
#     sis = load_sis()
#     result = join_sis(short, sis)

#     dfs = get_ir(result)
#     return con_cat(short, *dfs)

# add threaten columns
# group here


# # reload
# loaded_result= reload_ir_full()

# sis = load_sis()
# result = join_sis(loaded_result, sis)
# dfs = get_ir(result)

# pa = get_df_wdpa(result)
# output = con_cat(pa, *dfs)