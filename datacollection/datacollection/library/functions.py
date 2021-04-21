# Initial run
import json
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
import os


def readdata():
    with MongoClient('mongodb://collectionreader:Lacritz12345%23@85.214.144.66:27017/?authSource=dclmdb'
                     '&readPreference=primary&appname=MongoDB%20Compass&ssl=false') as client:
        filter = {}

        result = client['testdb']['A01000_collection'].find(
            filter=filter
        )

    df = pd.DataFrame()

    for i in result:
        df = df.append(i, ignore_index=True)

    return df


def clean_update_data_click(df):
    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'click'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_click = pd.read_csv(f'{path}{header}_data.csv', index_col=0)

        if len(df_click) != 0:

            df_new_click = df[df.timestamp > df[df._id == ObjectId(df_click['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_click = df.copy()

    old_shape = df_click.shape

    if len(df_new_click) == 0:
        print('Output>>    No update is found in CLICK data.. New data is same as saved one.')
    else:
        print(f'Output>>    New activities captured: {len(df_new_click)} ')

        for index, content in df_new_click.iterrows():
            if isinstance(content.click, list):
                for click_ in content.click:
                    d = {'_id': content['_id'], 'click_time': click_.get('time'), 'click_event': click_.get('event'),
                         'click_data_target': click_.get('data').get('target'),
                         'click_data_x': click_.get('data').get('x'),
                         'click_data_y': click_.get('data').get('y'), 'click_data_eX': click_.get('data').get('eX'),
                         'click_data_eY': click_.get('data').get('eY'),
                         'click_data_button': click_.get('data').get('button'),
                         'click_data_reaction': click_.get('data').get('reaction'),
                         'click_data_context': click_.get('data').get('context'),
                         'click_data_text': click_.get('data').get('text'),
                         'click_data_link': click_.get('data').get('link'),
                         'click_data_hash': click_.get('data').get('hash'),
                         'click_data_region': click_.get('data').get('region')}

                    df_click = df_click.append(d, ignore_index=True)
            else:

                d = {'_id': content['_id'], 'click_time': 'nan', 'click_event': 'nan',
                     'click_data_target': 'nan', 'click_data_x': 'nan',
                     'click_data_y': 'nan', 'click_data_eX': 'nan',
                     'click_data_eY': 'nan', 'click_data_button': 'nan',
                     'click_data_reaction': 'nan',
                     'click_data_context': 'nan',
                     'click_data_text': 'nan', 'click_data_link': 'nan',
                     'click_data_hash': 'nan', 'click_data_region': 'nan'
                     }

                df_click = df_click.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_click.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_click.to_csv(f'{path}{header}_data.csv')

        print(f'Output>>   Update completed.. New rows added:  {df_click.shape[0] - old_shape[0]} ')


def clean_update_data_nv(df):
    # This will find out the recent updated rows into the database and update df_pointer table for new data.( it will
    # save unnecessary processing)

    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'nv'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_nv = pd.read_csv(f'{path}{header}_data.csv', index_col=0)

        if len(df_nv) != 0:

            df_new_nv = df[df.timestamp > df[df._id == ObjectId(df_nv['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_nv = df.copy()
    else:
        df_new_nv = df.copy()
        old_shape = df_new_nv.shape
        df_nv = pd.DataFrame(columns=['_id', 'nv_time', 'nv_event', 'nv_data_fetchstart', 'nv_data_connectStart',
                                      'nv_data_connectEnd', 'nv_data_requestStart', 'nv_data_responseStart',
                                      'nv_data_responseEnd', 'nv_data_domInteractive', 'nv_data_domComplete',
                                      'nv_data_loadEventStart', 'nv_data_loadEventEnd', 'nv_data_redirectCount',
                                      'nv_data_size', 'nv_data_type', 'nv_data_protocol', 'nv_data_encodedSize',
                                      'nv_data_decodedSize'])

    old_shape = df_nv.shape

    if len(df_new_nv) == 0:
        print('Output>>    No update is found in NAVIGATION data.. New data is same as saved one.')
    else:
        print(f'Output>>    New activities captured: {len(df_new_nv)} ')

        for index, content in df_new_nv.iterrows():
            if isinstance(content.navigation, list):
                for nv_ in content.navigation:
                    d = {'_id': content['_id'], 'nv_time': nv_.get('time'), 'nv_event': nv_.get('event'),
                         'nv_data_fetchstart': nv_.get('data').get('fetchStart'),
                         'nv_data_connectStart': nv_.get('data').get('connectStart'),
                         'nv_data_connectEnd': nv_.get('data').get('connectEnd'),
                         'nv_data_requestStart': nv_.get('data').get('requestStart'),
                         'nv_data_responseStart': nv_.get('data').get('responseStart'),
                         'nv_data_responseEnd': nv_.get('data').get('responseEnd'),
                         'nv_data_domInteractive': nv_.get('data').get('domInteractive'),
                         'nv_data_domComplete': nv_.get('data').get('domComplete'),
                         'nv_data_loadEventStart': nv_.get('data').get('loadEventStart'),
                         'nv_data_loadEventEnd': nv_.get('data').get('loadEventEnd'),
                         'nv_data_redirectCount': nv_.get('data').get('redirectCount'),
                         'nv_data_size': nv_.get('data').get('size'),
                         'nv_data_type': nv_.get('data').get('type'),
                         'nv_data_protocol': nv_.get('data').get('protocol'),
                         'nv_data_encodedSize': nv_.get('data').get('encodedSize'),
                         'nv_data_decodedSize': nv_.get('data').get('decodedSize')
                         }
                    df_nv = df_nv.append(d, ignore_index=True)
            else:

                d = {'_id': content['_id'], 'nv_time': 'nan',
                     'nv_data_fetchstart': 'nan',
                     'nv_data_connectStart': 'nan',
                     'nv_data_connectEnd': 'nan',
                     'nv_data_requestStart': 'nan',
                     'nv_data_responseStart': 'nan',
                     'nv_data_responseEnd': 'nan',
                     'nv_data_domInteractive': 'nan',
                     'nv_data_domComplete': 'nan',
                     'nv_data_loadEventStart': 'nan',
                     'nv_data_loadEventEnd': 'nan',
                     'nv_data_redirectCount': 'nan',
                     'nv_data_size': 'nan',
                     'nv_data_type': 'nan',
                     'nv_data_protocol': 'nan',
                     'nv_data_encodedSize': 'nan',
                     'nv_data_decodedSize': 'nan'
                     }
                df_nv = df_nv.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_nv.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_nv.to_csv(f'{path}{header}_data.csv')
        print(f'Output>>   Update completed.. New rows added: {df_nv.shape[0] - old_shape[0]} ')


def clean_update_data_pointer(df):
    # This will find out the recent updated rows into the database and update df_pointer table for new data.( it will
    # save unnecessary processing)

    global df_pointer
    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'pointer'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_pointer = pd.read_csv(f'{path}{header}_data.csv', index_col=0)

        if len(df_pointer) != 0:

            df_new_pointer = df[df.timestamp > df[df._id == ObjectId(df_pointer['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_pointer = df.copy()

    old_shape = df_pointer.shape

    if len(df_new_pointer) == 0:
        print('Output>>    No update is found in POINTER data.. New data is same as saved one.')
    else:
        print(f'Output>>    New activities captured:{len(df_new_pointer)} ')

        for index, content in df_new_pointer.iterrows():
            if isinstance(content.pointer, list):
                for pointer_ in content.pointer:
                    d = {'_id': content['_id'],
                         'pointer_time': pointer_.get('time'),
                         'pointer_event': pointer_.get('event'),
                         'pointer_data_target': pointer_.get('data').get('target'),
                         'pointer_data_x': pointer_.get('data').get('x'),
                         'pointer_data_y': pointer_.get('data').get('y')}

                    df_pointer = df_pointer.append(d, ignore_index=True)
            else:

                d = {'_id': content['_id'],
                     'pointer_time': 'nan',
                     'pointer_event': 'nan',
                     'pointer_data_target': 'nan',
                     'pointer_data_x': 'nan',
                     'pointer_data_y': 'nan'}
                df_pointer = df_pointer.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_pointer.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_pointer.to_csv(f'{path}{header}_data.csv')
        print(f'Output>>   Update completed.. New rows added: {df_pointer.shape[0] - old_shape[0]} ')


def clean_update_data_env(df):
    # This will find out the recent updated rows into the database and update df_pointer table for new data.( it will save unnecessary processing)

    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'env'

    df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_env = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_env.shape

        if len(df_env) != 0:

            df_new_env = df[df.timestamp > df[df._id == ObjectId(df_env['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_env = df.copy()
    else:
        df_new_env = df.copy()
        old_shape = df_new_env.shape
        df_env = pd.DataFrame(columns=['_id', 'env_version', 'env_sequence', 'env_start', 'env_duration',
                                       'env_projectId', 'env_userId', 'env_sessionId',
                                       'env_pageNum', 'env_unload', 'env_end'])

    if len(df_new_env) == 0:
        print('Output>>    No update is found in ENVELOPE data.. New data is same as saved one.')
    else:
        print(f'Output>>    New activities captured: {len(df_new_env)} ')

        for index, content in df_new_env.iterrows():
            if isinstance(content.envelope, dict):
                # for env_ in content.envelope:

                d = {'_id': content['_id'],
                     'env_version': content.envelope.get('version'),
                     'env_sequence': content.envelope.get('sequence'),
                     'env_start': content.envelope.get('start'),
                     'env_duration': content.envelope.get('duration'),
                     'env_projectId': content.envelope.get('projectId'),
                     'env_userId': content.envelope.get('userId'),
                     'env_sessionId': content.envelope.get('sessionId'),
                     'env_pageNum': content.envelope.get('pageNum'),
                     'env_unload': content.envelope.get('upload'),
                     'env_end': content.envelope.get('end')

                     }
                df_env = df_env.append(d, ignore_index=True)
            else:
                d = {'_id': content['_id'],
                     'env_version': float("NaN"),
                     'env_sequence': float("NaN"),
                     'env_start': float("NaN"),
                     'env_duration': float("NaN"),
                     'env_projectId': float("NaN"),
                     'env_userId': float("NaN"),
                     'env_sessionId': float("NaN"),
                     'env_pageNum': float("NaN"),
                     'env_unload': float("NaN"),
                     'env_end': float("NaN")

                     }
                df_env = df_env.append(d, ignore_index=True)
        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_env.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_env.to_csv(f'{path}{header}_data.csv')

    print(f'Output>>   Update completed.. New rows added:  {df_env.shape[0] - old_shape[0]} ')


def clean_update_data_dm(df):
    # This will find out the recent updated rows into the database and update df_pointer table for new data.( it will
    # save unnecessary processing)

    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'dm'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_dm = pd.read_csv(f'{path}{header}_data.csv', index_col=0)

        if len(df_dm) != 0:

            df_new_dm = df[df.timestamp > df[df._id == ObjectId(df_dm['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_dm = df.copy()
    else:
        df_new_dm = df.copy()
        old_shape = df_new_dm.shape
        df_dm = pd.DataFrame(columns=['_id', 'dm_time', 'dm_event', 'dm_data_UserAgent_0', 'dm_data_url_1',
                                      'dm_data_referrer_2', 'dm_data_pagetitle_3', 'dm_data_NetworkHosts_4',
                                      'dm_data_SchemaType_5', 'dm_data_ProductBrand_6', 'dm_data_ProductAvailability_7',
                                      'dm_data_AuthorName_8'])

    old_shape = df_dm.shape

    if len(df_new_dm) == 0:
        print('Output>>    No update is found in DIMENSION data.. New data is same as saved one.')
    else:
        print(f'Output>>    New activities captured: {len(df_new_dm)} ')

        for index, content in df_new_dm.iterrows():
            if isinstance(content.dimension, list):
                for dm_ in content.dimension:
                    d = {'_id': content['_id'],
                         'dm_time': dm_.get('time'),
                         'dm_event': dm_.get('event'),
                         'dm_data_UserAgent_0': dm_.get('data').get('0'),
                         'dm_data_url_1': dm_.get('data').get('1'),
                         'dm_data_referrer_2': dm_.get('data').get('2'),
                         'dm_data_pagetitle_3': dm_.get('data').get('3'),
                         'dm_data_NetworkHosts_4': dm_.get('data').get('4'),
                         'dm_data_SchemaType_5': dm_.get('data').get('5'),
                         'dm_data_ProductBrand_6': dm_.get('data').get('6'),
                         'dm_data_ProductAvailability_7': dm_.get('data').get('7'),
                         'dm_data_AuthorName_8': dm_.get('data').get('8')
                         }
                    df_dm = df_dm.append(d, ignore_index=True)
            else:
                d = {'_id': content['_id'],
                     'dm_time': 'nan',
                     'dm_event': 'nan',
                     'dm_data_UserAgent_0': 'nan',
                     'dm_data_url_1': 'nan',
                     'dm_data_referrer_2': 'nan',
                     'dm_data_pagetitle_3': 'nan',
                     'dm_data_NetworkHosts_4': 'nan',
                     'dm_data_SchemaType_5': 'nan',
                     'dm_data_ProductBrand_6': 'nan',
                     'dm_data_ProductAvailability_7': 'nan',
                     'dm_data_AuthorName_8': 'nan'
                     }
                df_dm = df_dm.append(d, ignore_index=True)
        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_dm.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_dm.to_csv(f'{path}{header}_data.csv')

        print(f'Output>>   Update completed.. New rows added:  {df_dm.shape[0] - old_shape[0]} ')


def clean_update_data_dom(df):
    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'dom'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_dom = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_dom.shape

        if len(df_dom) != 0:

            df_new_dom = df[df.timestamp > df[df._id == ObjectId(df_dom['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_dom = df.copy()
    else:
        df_new_dom = df.copy()
        old_shape = df_new_dom.shape
        df_dom = pd.DataFrame(columns=['_id', 'dom_time', 'dom_event', 'dom_data_id', 'dom_data_parent',
                                       'dom_data_previous', 'dom_data_tag', 'dom_data_position',
                                       'dom_data_selector', 'dom_data_hash', 'dom_data_attributes'])

    if len(df_new_dom) == 0:
        print('Output>>    No update is found in DOM data.. New data is same as saved one.')
    else:
        print(f'Output>>    New activities captured: {len(df_new_dom)} ')
        print('Output>>    Updating the current data.... Please wait...')
        for index, content in df_new_dom.iterrows():
            if isinstance(content.dom, list):

                for dom_ in content.dom:
                    if 'data' in dom_.keys():
                        for dom_id_ in dom_['data']:
                            d = {'_id': content['_id'],
                                 'dom_time': dom_.get('time'),
                                 'dom_event': dom_.get('event'),
                                 'dom_data_id': dom_id_.get('id'),
                                 'dom_data_parent': dom_id_.get('parent'),
                                 'dom_data_previous': dom_id_.get('previous'),
                                 'dom_data_tag': dom_id_.get('tag'),
                                 'dom_data_position': dom_id_.get('position'),
                                 'dom_data_selector': dom_id_.get('selector'),
                                 'dom_data_hash': dom_id_.get('hash'),
                                 'dom_data_attributes': dom_id_.get('attributes')
                                 }

                            df_dom = df_dom.append(d, ignore_index=True)
                            # print(f'Not Null: {index}')
            else:

                d = {'_id': content['_id'],
                     'dom_time': float("NaN"),
                     'dom_event': float("NaN"),
                     'dom_data_id': float("NaN"),
                     'dom_data_parent': float("NaN"),
                     'dom_data_previous': float("NaN"),
                     'dom_data_tag': float("NaN"),
                     'dom_data_position': float("NaN"),
                     'dom_data_selector': float("NaN"),
                     'dom_data_hash': float("NaN"),
                     'dom_data_attributes': float("NaN")
                     }

                df_dom = df_dom.append(d, ignore_index=True)
                # print(f'Null: {index}')

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_dom.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_dom.to_csv(f'{path}{header}_data.csv')

        print(f'Output>>   Update completed.. New rows added:  {df_dom.shape[0] - old_shape[0]} ')


def clean_update_data_cn(df):
    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'cn'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_cn = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_cn.shape

        if len(df_cn) != 0:

            df_new_cn = df[df.timestamp > df[df._id == ObjectId(df_cn['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_cn = df.copy()
    else:
        df_new_cn = df.copy()
        old_shape = df_new_cn.shape
        df_cn = pd.DataFrame(columns=['_id', 'cn_time', 'cn_event', 'cn_data_downlink', 'cn_data_rtt',
                                      'cn_data_saveData', 'cn_data_type'])

    if len(df_new_cn) == 0:
        print('Output>>    No update is found in CONNECTION data.. New data is same as saved one')
    else:
        print(f'Output>>    New activities captured: {len(df_new_cn)} ')

        # iterating on rows of dataframe
        for index, content in df_new_cn.iterrows():
            if isinstance(content.connection, list):

                # iterating on list (in connection col)
                for cn_ in content.connection:

                    # chcecking if dictionary has 'data' key
                    if 'data' in cn_.keys():

                        # when all data is available
                        d = {'_id': content['_id'],
                             'cn_time': cn_.get('time'),
                             'cn_event': cn_.get('event'),
                             'cn_data_downlink': cn_.get('data').get('downlink'),
                             'cn_data_rtt': cn_.get('data').get('rtt'),
                             'cn_data_saveData': cn_.get('data').get('saveData'),
                             'cn_data_type': cn_.get('data').get('type'),

                             }

                        df_cn = df_cn.append(d, ignore_index=True)
                    else:
                        # when data key does not exist
                        d = {'_id': content['_id'],
                             'cn_time': cn_.get('time'),
                             'cn_event': cn_.get('event'),
                             'cn_data_downlink': float("NaN"),
                             'cn_data_rtt': float("NaN"),
                             'cn_data_saveData': float("NaN"),
                             'cn_data_type': float("NaN"),

                             }
                        df_cn = df_cn.append(d, ignore_index=True)

            else:
                # when complete row is NaN
                d = {'_id': content['_id'],
                     'cn_time': float("NaN"),
                     'cn_event': float("NaN"),
                     'cn_data_downlink': float("NaN"),
                     'cn_data_rtt': float("NaN"),
                     'cn_data_saveData': float("NaN"),
                     'cn_data_type': float("NaN"),

                     }

                df_cn = df_cn.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_cn.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_cn.to_csv(f'{path}{header}_data.csv')

        print(f'New rows added:  {df_cn.shape[0] - old_shape[0]} ')


def clean_update_data_tm(df):
    # This will find out the recent updated rows into the database and update df_pointer table for new data.( it will save unnecessary processing)

    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'tm'

    df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_tm = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_tm.shape

        if len(df_tm) != 0:

            df_new_tm = df[df.timestamp > df[df._id == ObjectId(df_tm['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_tm = df.copy()
    else:
        df_new_tm = df.copy()
        old_shape = df_new_tm.shape
        df_tm = pd.DataFrame(columns=['_id', 'tm_date', 'tm_time'])

    if len(df_new_tm) == 0:
        print('Output>>    No update is found in TIMESTAMP data.. New data is same as saved one.')
    else:
        print(f'Output>>    New activities captured: {len(df_new_tm)} ')

        for index, content in df_new_tm.iterrows():
            date_time = datetime.fromtimestamp(content.timestamp / 1000)

            d = {'_id': content['_id'],
                 'tm_date': date_time.date(),
                 'tm_time': date_time.time()

                 }
            df_tm = df_tm.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_tm.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_tm.to_csv(f'{path}{header}_data.csv')

    print(f'Output>>   Update completed.. New rows added:  {df_tm.shape[0] - old_shape[0]} ')


def clean_update_data_geo(df):
    # This will find out the recent updated rows into the database and update df_pointer table for new data.( it will save unnecessary processing)

    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'geo'

    df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_geo = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_geo.shape

        if len(df_geo) != 0:

            df_new_geo = df[df.timestamp > df[df._id == ObjectId(df_geo['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_geo = df.copy()
    else:
        df_new_geo = df.copy()
        old_shape = df_new_geo.shape

        df_geo = pd.DataFrame(columns=['_id', 'geo_city', 'geo_region', 'geo_country', 'geo_loc', 'geo_org'])

    if len(df_new_geo) == 0:
        print('Output>>    No update is found in GEO location data.. New data is same as saved one.')
    else:
        print(f'Output>>    New activities captured: {len(df_new_geo)} ')

        for index, content in df_new_geo.iterrows():
            t_ip = content.ip.lstrip(':f')
            t_d = ipInfo(t_ip)
            d = {'_id': content['_id'],
                 'geo_city': t_d.get('city'),
                 'geo_region': t_d.get('region'),
                 'geo_country': t_d.get('country'),
                 'geo_loc': t_d.get('loc'),
                 'geo_org': t_d.get('org')

                 }
            df_geo = df_geo.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_geo.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_geo.to_csv(f'{path}{header}_data.csv')

    print(f'Output>>   Update completed.. New rows added:  {df_geo.shape[0] - old_shape[0]} ')


def ipInfo(addr=''):
    from urllib.request import urlopen
    from json import load
    if addr == '':
        url = 'https://ipinfo.io/json'
    else:
        url = 'https://ipinfo.io/' + addr + '/json'
    res = urlopen(url)
    # response from url(if res==None then check connection)
    data = load(res)
    # will load the json response into data
    return data


def clean_and_update(df):
    # df = readdata()
    # df = df.loc[:370]

    print('Info>>    DIMENSION header table is being updated...')
    clean_update_data_dm(df)

    print('Info>>    NAVIGATION header table is being updated...')
    clean_update_data_nv(df)

    print('Info>>    POINTER header table is being updated...')
    clean_update_data_pointer(df)

    print('Info>>    CLICK header table is being updated...')
    clean_update_data_click(df)

    print('Info>>    DOM header table is being updated...')
    clean_update_data_dom(df)

    print('Info>>    CONNECTION header table is being updated...')
    clean_update_data_cn(df)

    print('Info>>    ENVELOPE header table is being updated...')
    clean_update_data_env(df)

    print('Info>>    TIMESTAMP header table is being updated...')
    clean_update_data_tm(df)

    print('Info>>    GEO Location header table is being updated...')
    clean_update_data_geo(df)

    print('All header tables are updated successfully with current mongodb data.')
