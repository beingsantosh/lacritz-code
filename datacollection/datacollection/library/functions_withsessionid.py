# Initial run
import json
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
import os
import numpy as np


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
        old_shape = df_click.shape

        if len(df_click) != 0:

            df_new_click = df[df.timestamp > df[df._id == ObjectId(df_click['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_click = df.copy()
    else:
        df_new_click = df.copy()
        old_shape = np.array([0, 0])
        df_click = pd.DataFrame(
            columns=['session_id', 'click_time', 'click_event', 'click_data_target', 'click_data_x', 'click_data_y',
                     'click_data_eX', 'click_data_eY', 'click_data_button', 'click_data_reaction', 'click_data_context',
                     'click_data_text', 'click_data_link', 'click_data_hash', 'click_data_region', '_id'])

    old_shape = df_click.shape

    if len(df_new_click) == 0:
        print('Output>>    No update is found in CLICK data.. New data is same as saved one.')
    else:
        print(f'Output>>    New activities captured: {len(df_new_click)} ')

        for index, content in df_new_click.iterrows():
            if isinstance(content.click, list):
                for click_ in content.click:
                    d = {'session_id': content.envelope.get('sessionId'), 'click_time': click_.get('time'),
                         'click_event': click_.get('event'),
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
                         'click_data_region': click_.get('data').get('region'),
                         '_id': content['_id']}

                    df_click = df_click.append(d, ignore_index=True)
            else:

                d = {'session_id': content.envelope.get('sessionId'),
                     'click_time': float("NaN"), 'click_event': float("NaN"),
                     'click_data_target': float("NaN"), 'click_data_x': float("NaN"),
                     'click_data_y': float("NaN"), 'click_data_eX': float("NaN"),
                     'click_data_eY': float("NaN"), 'click_data_button': float("NaN"),
                     'click_data_reaction': float("NaN"),
                     'click_data_context': float("NaN"),
                     'click_data_text': float("NaN"), 'click_data_link': float("NaN"),
                     'click_data_hash': float("NaN"), 'click_data_region': float("NaN"),
                     '_id': content['_id']
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
        old_shape = df_nv.shape

        if len(df_nv) != 0:

            df_new_nv = df[df.timestamp > df[df._id == ObjectId(df_nv['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_nv = df.copy()
    else:
        df_new_nv = df.copy()
        old_shape = np.array([0, 0])
        df_nv = pd.DataFrame(columns=['session_id', 'nv_time', 'nv_event', 'nv_data_fetchstart', 'nv_data_connectStart',
                                      'nv_data_connectEnd', 'nv_data_requestStart', 'nv_data_responseStart',
                                      'nv_data_responseEnd', 'nv_data_domInteractive', 'nv_data_domComplete',
                                      'nv_data_loadEventStart', 'nv_data_loadEventEnd', 'nv_data_redirectCount',
                                      'nv_data_size', 'nv_data_type', 'nv_data_protocol', 'nv_data_encodedSize',
                                      'nv_data_decodedSize', '_id'])

    if len(df_new_nv) == 0:
        print('Output>>    No update is found in NAVIGATION data.. New data is same as saved one.')
    else:
        print(f'Output>>    New activities captured: {len(df_new_nv)} ')

        for index, content in df_new_nv.iterrows():
            if isinstance(content.navigation, list):
                for nv_ in content.navigation:
                    d = {'session_id': content.envelope.get('sessionId'), 'nv_time': nv_.get('time'),
                         'nv_event': nv_.get('event'),
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
                         'nv_data_decodedSize': nv_.get('data').get('decodedSize'),
                         '_id': content['_id']
                         }
                    df_nv = df_nv.append(d, ignore_index=True)
            else:

                d = {'session_id': content.envelope.get('sessionId'),
                     'nv_time': float("NaN"),
                     'nv_data_fetchstart': float("NaN"),
                     'nv_data_connectStart': float("NaN"),
                     'nv_data_connectEnd': float("NaN"),
                     'nv_data_requestStart': float("NaN"),
                     'nv_data_responseStart': float("NaN"),
                     'nv_data_responseEnd': float("NaN"),
                     'nv_data_domInteractive': float("NaN"),
                     'nv_data_domComplete': float("NaN"),
                     'nv_data_loadEventStart': float("NaN"),
                     'nv_data_loadEventEnd': float("NaN"),
                     'nv_data_redirectCount': float("NaN"),
                     'nv_data_size': float("NaN"),
                     'nv_data_type': float("NaN"),
                     'nv_data_protocol': float("NaN"),
                     'nv_data_encodedSize': float("NaN"),
                     'nv_data_decodedSize': float("NaN"),
                     '_id': content['_id']
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
        old_shape = df_pointer.shape
        if len(df_pointer) != 0:

            df_new_pointer = df[df.timestamp > df[df._id == ObjectId(df_pointer['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_pointer = df.copy()
    else:
        df_new_pointer = df.copy()
        old_shape = np.array([0, 0])
        df_pointer = pd.DataFrame(
            columns=['session_id', 'pointer_time', 'pointer_event', 'pointer_data_target', 'pointer_data_x',
                     'pointer_data_y', '_id'])

    if len(df_new_pointer) == 0:
        print('Output>>    No update is found in POINTER data.. New data is same as saved one.')
    else:
        print(f'Output>>    New activities captured:{len(df_new_pointer)} ')

        for index, content in df_new_pointer.iterrows():
            if isinstance(content.pointer, list):
                for pointer_ in content.pointer:
                    d = {'session_id': content.envelope.get('sessionId'),
                         'pointer_time': pointer_.get('time'),
                         'pointer_event': pointer_.get('event'),
                         'pointer_data_target': pointer_.get('data').get('target'),
                         'pointer_data_x': pointer_.get('data').get('x'),
                         'pointer_data_y': pointer_.get('data').get('y'),
                         '_id': content['_id']}

                    df_pointer = df_pointer.append(d, ignore_index=True)
            else:

                d = {'session_id': content.envelope.get('sessionId'),
                     'pointer_time': float("NaN"),
                     'pointer_event': float("NaN"),
                     'pointer_data_target': float("NaN"),
                     'pointer_data_x': float("NaN"),
                     'pointer_data_y': float("NaN"),
                     '_id': content['_id']}
                df_pointer = df_pointer.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_pointer.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_pointer.to_csv(f'{path}{header}_data.csv')
        print(f'Output>>   Update completed.. New rows added: {df_pointer.shape[0] - old_shape[0]} ')


def clean_update_data_log(df):
    # This will find out the recent updated rows into the database and update df_log table for new data.( it will
    # save unnecessary processing)

    global df_log
    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'log'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_log = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_log.shape
        if len(df_log) != 0:

            df_new_log = df[df.timestamp > df[df._id == ObjectId(df_log['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_log = df.copy()
    else:
        df_new_log = df.copy()
        old_shape = np.array([0, 0])
        df_log = pd.DataFrame(
            columns=['session_id', 'log_time', 'log_event', 'log_data_code', 'log_data_name',
                     'log_data_message','log_data_stack','log_data_severity', '_id'])

    if len(df_new_log) == 0:
        print('Output>>    No update is found in LOG data.. New data is same as saved one.')
    else:
        print(f'Output>>    New activities captured:{len(df_new_log)} ')

        for index, content in df_new_log.iterrows():
            if isinstance(content.log, list):
                for log_ in content.log:
                    d = {'session_id': content.envelope.get('sessionId'),
                         'log_time': log_.get('time'),
                         'log_event': log_.get('event'),
                         'log_data_code': log_.get('data').get('code'),
                         'log_data_name': log_.get('data').get('name'),
                         'log_data_message': log_.get('data').get('message'),
                         'log_data_stack': log_.get('data').get('stack'),
                         'log_data_severity': log_.get('data').get('severity'),
                         '_id': content['_id']}

                    df_log = df_log.append(d, ignore_index=True)
            else:

                d = {'session_id': content.envelope.get('sessionId'),
                     'log_time': float("NaN"),
                     'log_event': float("NaN"),
                     'log_data_code': float("NaN"),
                     'log_data_name': float("NaN"),
                     'log_data_message': float("NaN"),
                     'log_data_stack': float("NaN"),
                     'log_data_severity': float("NaN"),
                     '_id': content['_id']}
                df_log = df_log.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_log.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_log.to_csv(f'{path}{header}_data.csv')
        print(f'Output>>   Update completed.. New rows added: {df_log.shape[0] - old_shape[0]} ')


def clean_update_data_scroll(df):
    # This will find out the recent updated rows into the database and update df_scroll table for new data.( it will
    # save unnecessary processing)

    global df_scroll
    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'scroll'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_scroll = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_scroll.shape
        if len(df_scroll) != 0:

            df_new_scroll = df[df.timestamp > df[df._id == ObjectId(df_scroll['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_scroll = df.copy()
    else:
        df_new_scroll = df.copy()
        old_shape = np.array([0, 0])
        df_scroll = pd.DataFrame(
            columns=['session_id', 'scroll_time', 'scroll_event', 'scroll_data_target', 'scroll_data_x',
                     'scroll_data_y', 'scroll_data_region', '_id'])

    if len(df_new_scroll) == 0:
        print('Output>>    No update is found in SCROLL data.. New data is same as saved one.')
    else:
        print(f'Output>>    New activities captured:{len(df_new_scroll)} ')

        for index, content in df_new_scroll.iterrows():
            if isinstance(content.scroll, list):
                for scroll_ in content.scroll:
                    d = {'session_id': content.envelope.get('sessionId'),
                         'scroll_time': scroll_.get('time'),
                         'scroll_event': scroll_.get('event'),
                         'scroll_data_target': scroll_.get('data').get('target'),
                         'scroll_data_x': scroll_.get('data').get('x'),
                         'scroll_data_y': scroll_.get('data').get('y'),
                         'scroll_data_region': scroll_.get('data').get('region'),
                         '_id': content['_id']}

                    df_scroll = df_scroll.append(d, ignore_index=True)
            else:

                d = {'session_id': content.envelope.get('sessionId'),
                     'scroll_time': float("NaN"),
                     'scroll_event': float("NaN"),
                     'scroll_data_target': float("NaN"),
                     'scroll_data_x': float("NaN"),
                     'scroll_data_y': float("NaN"),
                     'scroll_data_region': float("NaN"),
                     '_id': content['_id']}
                df_scroll = df_scroll.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_scroll.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_scroll.to_csv(f'{path}{header}_data.csv')
        print(f'Output>>   Update completed.. New rows added: {df_scroll.shape[0] - old_shape[0]} ')


def clean_update_data_env(df):
    # This will find out the recent updated rows into the database and update df_pointer table for new data.( it will save unnecessary processing)

    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'env'

    if os.path.isfile(f'{path}{header}_data.csv'):
        df_env = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_env.shape

        if len(df_env) != 0:

            df_new_env = df[df.timestamp > df[df._id == ObjectId(df_env['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_env = df.copy()
    else:
        df_new_env = df.copy()
        old_shape = np.array([0, 0])
        df_env = pd.DataFrame(columns=['session_id', 'env_version', 'env_sequence', 'env_start', 'env_duration',
                                       'env_projectId', 'env_userId',
                                       # 'env_sessionId',
                                       'env_pageNum', 'env_upload', 'env_end', '_id'])

    if len(df_new_env) == 0:
        print('Output>>    No update is found in ENVELOPE data.. New data is same as saved one.')
    else:
        print(f'Output>>    New activities captured: {len(df_new_env)} ')

        for index, content in df_new_env.iterrows():
            if isinstance(content.envelope, dict):
                # for env_ in content.envelope:

                d = {'session_id': content.envelope.get('sessionId'),
                     'env_version': content.envelope.get('version'),
                     'env_sequence': content.envelope.get('sequence'),
                     'env_start': content.envelope.get('start'),
                     'env_duration': content.envelope.get('duration'),
                     'env_projectId': content.envelope.get('projectId'),
                     'env_userId': content.envelope.get('userId'),
                     # 'env_sessionId': content.envelope.get('sessionId'),
                     'env_pageNum': content.envelope.get('pageNum'),
                     'env_unload': content.envelope.get('upload'),
                     'env_end': content.envelope.get('end'),
                     '_id': content['_id']

                     }
                df_env = df_env.append(d, ignore_index=True)
            else:
                d = {'session_id': content.envelope.get('sessionId'),
                     'env_version': float("NaN"),
                     'env_sequence': float("NaN"),
                     'env_start': float("NaN"),
                     'env_duration': float("NaN"),
                     'env_projectId': float("NaN"),
                     'env_userId': float("NaN"),
                     # 'env_sessionId': float("NaN"),
                     'env_pageNum': float("NaN"),
                     'env_unload': float("NaN"),
                     'env_end': float("NaN"),
                     '_id': content['_id']

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
        old_shape = df_dm.shape
        if len(df_dm) != 0:

            df_new_dm = df[df.timestamp > df[df._id == ObjectId(df_dm['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_dm = df.copy()
    else:
        df_new_dm = df.copy()
        old_shape = np.array([0, 0])
        df_dm = pd.DataFrame(columns=['session_id', 'dm_time', 'dm_event', 'dm_data_UserAgent_0', 'dm_data_url_1',
                                      'dm_data_referrer_2', 'dm_data_pagetitle_3', 'dm_data_NetworkHosts_4',
                                      'dm_data_SchemaType_5', 'dm_data_ProductBrand_6', 'dm_data_ProductAvailability_7',
                                      'dm_data_AuthorName_8', 'dm_data_Language_9', '_id'])

    if len(df_new_dm) == 0:
        print('Output>>    No update is found in DIMENSION data.. New data is same as saved one.')
    else:
        print(f'Output>>    New activities captured: {len(df_new_dm)} ')

        for index, content in df_new_dm.iterrows():
            if isinstance(content.dimension, list):
                for dm_ in content.dimension:
                    d = {'session_id': content.envelope.get('sessionId'),
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
                         'dm_data_AuthorName_8': dm_.get('data').get('8'),
                         'dm_data_Language_9': dm_.get('data').get('9'),
                         '_id': content['_id']
                         }
                    df_dm = df_dm.append(d, ignore_index=True)
            else:
                d = {'session_id': content.envelope.get('sessionId'),
                     'dm_time': float("NaN"),
                     'dm_event': float("NaN"),
                     'dm_data_UserAgent_0': float("NaN"),
                     'dm_data_url_1': float("NaN"),
                     'dm_data_referrer_2': float("NaN"),
                     'dm_data_pagetitle_3': float("NaN"),
                     'dm_data_NetworkHosts_4': float("NaN"),
                     'dm_data_SchemaType_5': float("NaN"),
                     'dm_data_ProductBrand_6': float("NaN"),
                     'dm_data_ProductAvailability_7': float("NaN"),
                     'dm_data_AuthorName_8': float("NaN"),
                     'dm_data_Language_9': float("NaN"),
                     '_id': content['_id']
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
        old_shape = np.array([0, 0])
        df_dom = pd.DataFrame(columns=['session_id', 'dom_time', 'dom_event', 'dom_data_id', 'dom_data_parent',
                                       'dom_data_previous', 'dom_data_tag', 'dom_data_position',
                                       'dom_data_selector', 'dom_data_hash', 'dom_data_attributes', '_id'])

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
                            d = {'session_id': content.envelope.get('sessionId'),
                                 'dom_time': dom_.get('time'),
                                 'dom_event': dom_.get('event'),
                                 'dom_data_id': dom_id_.get('id'),
                                 'dom_data_parent': dom_id_.get('parent'),
                                 'dom_data_previous': dom_id_.get('previous'),
                                 'dom_data_tag': dom_id_.get('tag'),
                                 'dom_data_position': dom_id_.get('position'),
                                 'dom_data_selector': dom_id_.get('selector'),
                                 'dom_data_hash': dom_id_.get('hash'),
                                 'dom_data_attributes': dom_id_.get('attributes'),
                                 '_id': content['_id']
                                 }

                            df_dom = df_dom.append(d, ignore_index=True)
                            # print(f'Not Null: {index}')
            else:

                d = {'session_id': content.envelope.get('sessionId'),
                     'dom_time': float("NaN"),
                     'dom_event': float("NaN"),
                     'dom_data_id': float("NaN"),
                     'dom_data_parent': float("NaN"),
                     'dom_data_previous': float("NaN"),
                     'dom_data_tag': float("NaN"),
                     'dom_data_position': float("NaN"),
                     'dom_data_selector': float("NaN"),
                     'dom_data_hash': float("NaN"),
                     'dom_data_attributes': float("NaN"),
                     '_id': content['_id']
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
        old_shape = np.array([0, 0])
        df_cn = pd.DataFrame(columns=['session_id', 'cn_time', 'cn_event', 'cn_data_downlink', 'cn_data_rtt',
                                      'cn_data_saveData', 'cn_data_type', 'id'])

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
                        d = {'session_id': content.envelope.get('sessionId'),
                             'cn_time': cn_.get('time'),
                             'cn_event': cn_.get('event'),
                             'cn_data_downlink': cn_.get('data').get('downlink'),
                             'cn_data_rtt': cn_.get('data').get('rtt'),
                             'cn_data_saveData': cn_.get('data').get('saveData'),
                             'cn_data_type': cn_.get('data').get('type'),
                             '_id': content['_id']

                             }

                        df_cn = df_cn.append(d, ignore_index=True)
                    else:
                        # when data key does not exist
                        d = {'session_id': content.envelope.get('sessionId'),
                             'cn_time': cn_.get('time'),
                             'cn_event': cn_.get('event'),
                             'cn_data_downlink': float("NaN"),
                             'cn_data_rtt': float("NaN"),
                             'cn_data_saveData': float("NaN"),
                             'cn_data_type': float("NaN"),
                             '_id': content['_id']

                             }
                        df_cn = df_cn.append(d, ignore_index=True)

            else:
                # when complete row is NaN
                d = {'session_id': content.envelope.get('sessionId'),
                     'cn_time': float("NaN"),
                     'cn_event': float("NaN"),
                     'cn_data_downlink': float("NaN"),
                     'cn_data_rtt': float("NaN"),
                     'cn_data_saveData': float("NaN"),
                     'cn_data_type': float("NaN"),
                     '_id': content['_id']

                     }

                df_cn = df_cn.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_cn.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_cn.to_csv(f'{path}{header}_data.csv')

        print(f'Output>>   Update completed.. New rows added:  {df_cn.shape[0] - old_shape[0]} ')


def clean_update_data_pn(df):
    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'pn'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_pn = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_pn.shape

        if len(df_pn) != 0:

            df_new_pn = df[df.timestamp > df[df._id == ObjectId(df_pn['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_pn = df.copy()
    else:
        df_new_pn = df.copy()
        old_shape = np.array([0, 0])
        df_pn = pd.DataFrame(columns=['session_id', 'pn_time', 'pn_event', 'pn_data_gap', '_id'])

    if len(df_new_pn) == 0:
        print('Output>>    No update is found in PING data.. New data is same as saved one')
    else:
        print(f'Output>>    New activities captured: {len(df_new_pn)} ')

        # iterating on rows of dataframe
        for index, content in df_new_pn.iterrows():
            if isinstance(content.ping, list):

                # iterating on list (in connection col)
                for pn_ in content.ping:

                    # chcecking if dictionary has 'data' key
                    if 'data' in pn_.keys():

                        # when all data is available
                        d = {'session_id': content.envelope.get('sessionId'),
                             'pn_time': pn_.get('time'),
                             'pn_event': pn_.get('event'),
                             'pn_data_gap': pn_.get('data').get('gap'),
                             '_id': content['_id']

                             }

                        df_pn = df_pn.append(d, ignore_index=True)
                    else:
                        # when data key does not exist
                        d = {'session_id': content.envelope.get('sessionId'),
                             'pn_time': pn_.get('time'),
                             'pn_event': pn_.get('event'),
                             'pn_data_gap': float("NaN"),
                             '_id': content['_id']

                             }
                        df_pn = df_pn.append(d, ignore_index=True)

            else:
                # when complete row is NaN
                d = {'session_id': content.envelope.get('sessionId'),
                     'pn_time': float("NaN"),
                     'pn_event': float("NaN"),
                     'pn_data_gap': float("NaN"),
                     '_id': content['_id']

                     }

                df_pn = df_pn.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_pn.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_pn.to_csv(f'{path}{header}_data.csv')

        print(f'Output>>   Update completed.. New rows added:  {df_pn.shape[0] - old_shape[0]} ')


def clean_update_data_vs(df):
    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'vs'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_vs = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_vs.shape

        if len(df_vs) != 0:

            df_new_vs = df[df.timestamp > df[df._id == ObjectId(df_vs['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_vs = df.copy()
    else:
        df_new_vs = df.copy()
        old_shape = np.array([0, 0])
        df_vs = pd.DataFrame(columns=['session_id', 'vs_time', 'vs_event', 'vs_data_visible', '_id'])

    if len(df_new_vs) == 0:
        print('Output>>    No update is found in PING data.. New data is same as saved one')
    else:
        print(f'Output>>    New activities captured: {len(df_new_vs)} ')

        # iterating on rows of dataframe
        for index, content in df_new_vs.iterrows():
            if isinstance(content.visibility, list):

                # iterating on list (in connection col)
                for vs_ in content.visibility:

                    # chcecking if dictionary has 'data' key
                    if 'data' in vs_.keys():

                        # when all data is available
                        d = {'session_id': content.envelope.get('sessionId'),
                             'vs_time': vs_.get('time'),
                             'vs_event': vs_.get('event'),
                             'vs_data_visible': vs_.get('data').get('visible'),
                             '_id': content['_id']

                             }

                        df_vs = df_vs.append(d, ignore_index=True)
                    else:
                        # when data key does not exist
                        d = {'session_id': content.envelope.get('sessionId'),
                             'vs_time': vs_.get('time'),
                             'vs_event': vs_.get('event'),
                             'vs_data_visible': float("NaN"),
                             '_id': content['_id']

                             }
                        df_vs = df_vs.append(d, ignore_index=True)

            else:
                # when complete row is NaN
                d = {'session_id': content.envelope.get('sessionId'),
                     'vs_time': float("NaN"),
                     'vs_event': float("NaN"),
                     'vs_data_visible': float("NaN"),
                     '_id': content['_id']

                     }

                df_vs = df_vs.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_vs.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_vs.to_csv(f'{path}{header}_data.csv')

        print(f'Output>>   Update completed.. New rows added:  {df_vs.shape[0] - old_shape[0]} ')


def clean_update_data_lm(df):
    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'lm'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_lm = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_lm.shape

        if len(df_lm) != 0:

            df_new_lm = df[df.timestamp > df[df._id == ObjectId(df_lm['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_lm = df.copy()
    else:
        df_new_lm = df.copy()
        old_shape = np.array([0, 0])
        df_lm = pd.DataFrame(columns=['session_id', 'lm_time', 'lm_event', 'lm_data_check', '_id'])

    if len(df_new_lm) == 0:
        print('Output>>    No update is found in LIMIT data.. New data is same as saved one')
    else:
        print(f'Output>>    New activities captured: {len(df_new_lm)} ')

        # iterating on rows of dataframe
        for index, content in df_new_lm.iterrows():
            if isinstance(content.limit, list):

                # iterating on list (in connection col)
                for lm_ in content.limit:

                    # chcecking if dictionary has 'data' key
                    if 'data' in lm_.keys():

                        # when all data is available
                        d = {'session_id': content.envelope.get('sessionId'),
                             'lm_time': lm_.get('time'),
                             'lm_event': lm_.get('event'),
                             'lm_data_check': lm_.get('data').get('check'),
                             '_id': content['_id']

                             }

                        df_lm = df_lm.append(d, ignore_index=True)
                    else:
                        # when data key does not exist
                        d = {'session_id': content.envelope.get('sessionId'),
                             'lm_time': lm_.get('time'),
                             'lm_event': lm_.get('event'),
                             'lm_data_check': float("NaN"),
                             '_id': content['_id']

                             }
                        df_lm = df_lm.append(d, ignore_index=True)

            else:
                # when complete row is NaN
                d = {'session_id': content.envelope.get('sessionId'),
                     'lm_time': float("NaN"),
                     'lm_event': float("NaN"),
                     'lm_data_check': float("NaN"),
                     '_id': content['_id']

                     }

                df_lm = df_lm.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_lm.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_lm.to_csv(f'{path}{header}_data.csv')

        print(f'Output>>   Update completed.. New rows added:  {df_lm.shape[0] - old_shape[0]} ')


def clean_update_data_upl(df):
    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'upl'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_upl = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_upl.shape

        if len(df_upl) != 0:

            df_new_upl = df[df.timestamp > df[df._id == ObjectId(df_upl['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_upl = df.copy()
    else:
        df_new_upl = df.copy()
        old_shape = np.array([0, 0])
        df_upl = pd.DataFrame(
            columns=['session_id', 'upl_time', 'upl_event', 'upl_data_sequence', 'upl_data_attempts', 'upl_data_status',
                     '_id'])

    if len(df_new_upl) == 0:
        print('Output>>    No update is found in UPLOAD data.. New data is same as saved one')
    else:
        print(f'Output>>    New activities captured: {len(df_new_upl)} ')

        # iterating on rows of dataframe
        for index, content in df_new_upl.iterrows():
            if isinstance(content.upload, list):

                # iterating on list (in connection col)
                for upl_ in content.upload:

                    # chcecking if dictionary has 'data' key
                    if 'data' in upl_.keys():

                        # when all data is available
                        d = {'session_id': content.envelope.get('sessionId'),
                             'upl_time': upl_.get('time'),
                             'upl_event': upl_.get('event'),
                             'upl_data_sequence': upl_.get('data').get('sequence'),
                             'upl_data_attempts': upl_.get('data').get('attempts'),
                             'upl_data_status': upl_.get('data').get('status'),
                             '_id': content['_id']

                             }

                        df_upl = df_upl.append(d, ignore_index=True)
                    else:
                        # when data key does not exist
                        d = {'session_id': content.envelope.get('sessionId'),
                             'upl_time': upl_.get('time'),
                             'upl_event': upl_.get('event'),
                             'upl_data_sequence': float("NaN"),
                             'upl_data_attempts': float("NaN"),
                             'upl_data_status': float("NaN"),
                             '_id': content['_id']

                             }
                        df_upl = df_upl.append(d, ignore_index=True)

            else:
                # when complete row is NaN
                d = {'session_id': content.envelope.get('sessionId'),
                     'upl_time': float("NaN"),
                     'upl_event': float("NaN"),
                     'upl_data_sequence': float("NaN"),
                     'upl_data_attempts': float("NaN"),
                     'upl_data_status': float("NaN"),
                     '_id': content['_id']

                     }

                df_upl = df_upl.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_upl.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_upl.to_csv(f'{path}{header}_data.csv')

        print(f'Output>>   Update completed.. New rows added:  {df_upl.shape[0] - old_shape[0]} ')


def clean_update_data_sm(df):
    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'sm'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_sm = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_sm.shape

        if len(df_sm) != 0:

            df_new_sm = df[df.timestamp > df[df._id == ObjectId(df_sm['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_sm = df.copy()
    else:
        df_new_sm = df.copy()
        old_shape = np.array([0, 0])
        df_sm = pd.DataFrame(columns=['session_id', 'sm_time', 'sm_event', 'sm_data_6', '_id'])

    if len(df_new_sm) == 0:
        print('Output>>    No update is found in SUMMARY data.. New data is same as saved one')
    else:
        print(f'Output>>    New activities captured: {len(df_new_sm)} ')

        # iterating on rows of dataframe
        for index, content in df_new_sm.iterrows():
            if isinstance(content.summary, list):

                # iterating on list (in connection col)
                for sm_ in content.summary:

                    # chcecking if dictionary has 'data' key
                    if 'data' in sm_.keys():
                        # print(sm_.get('data').get('6'),sm_.get('time'))
                        # when all data is available
                        d = {'session_id': content.envelope.get('sessionId'),
                             'sm_time': sm_.get('time'),
                             'sm_event': sm_.get('event'),
                             'sm_data_6': sm_.get('data').get('6'),
                             '_id': content['_id']

                             }

                        df_sm = df_sm.append(d, ignore_index=True)
                    else:
                        # when data key does not exist
                        d = {'session_id': content.envelope.get('sessionId'),
                             'sm_time': sm_.get('time'),
                             'sm_event': sm_.get('event'),
                             'sm_data_6': float("NaN"),
                             '_id': content['_id']

                             }
                        df_sm = df_sm.append(d, ignore_index=True)

            else:
                # when complete row is NaN
                d = {'session_id': content.envelope.get('sessionId'),
                     'sm_time': float("NaN"),
                     'sm_event': float("NaN"),
                     'sm_data_6': float("NaN"),
                     '_id': content['_id']

                     }

                df_sm = df_sm.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_sm.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_sm.to_csv(f'{path}{header}_data.csv')

        print(f'Output>>   Update completed.. New rows added:  {df_sm.shape[0] - old_shape[0]} ')


def clean_update_data_bs(df):
    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'bs'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_bs = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_bs.shape

        if len(df_bs) != 0:

            df_new_bs = df[df.timestamp > df[df._id == ObjectId(df_bs['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_bs = df.copy()
    else:
        df_new_bs = df.copy()
        old_shape = np.array([0, 0])
        df_bs = pd.DataFrame(columns=['session_id', 'bs_time', 'bs_event', 'bs_data_visible', 'bs_data_docWidth',
                                      'bs_data_docHeight', 'bs_data_screenWidth', 'bs_data_screenHeight',
                                      'bs_data_scrollX', 'bs_data_scrollY', 'bs_data_pointerX', 'bs_data_pointerY',
                                      'bs_data_activityTime', '_id'])

    if len(df_new_bs) == 0:
        print('Output>>    No update is found in BASELINE data.. New data is same as saved one')
    else:
        print(f'Output>>    New activities captured: {len(df_new_bs)} ')

        # iterating on rows of dataframe
        for index, content in df_new_bs.iterrows():
            if isinstance(content.baseline, list):

                # iterating on list (in connection col)
                for bs_ in content.baseline:

                    # chcecking if dictionary has 'data' key
                    if 'data' in bs_.keys():

                        # when all data is available
                        d = {'session_id': content.envelope.get('sessionId'),
                             'bs_time': bs_.get('time'),
                             'bs_event': bs_.get('event'),
                             'bs_data_visible': bs_.get('data').get('visible'),
                             'bs_data_docWidth': bs_.get('data').get('docWidth'),
                             'bs_data_docHeight': bs_.get('data').get('docHeight'),
                             'bs_data_screenWidth': bs_.get('data').get('screenWidth'),
                             'bs_data_screenHeight': bs_.get('data').get('screenHeight'),
                             'bs_data_scrollX': bs_.get('data').get('scrollX'),
                             'bs_data_scrollY': bs_.get('data').get('scrollY'),
                             'bs_data_pointerX': bs_.get('data').get('pointerX'),
                             'bs_data_pointerY': bs_.get('data').get('pointerY'),
                             'bs_data_activityTime': bs_.get('data').get('activityTime'),
                             '_id': content['_id']

                             }

                        df_bs = df_bs.append(d, ignore_index=True)
                    else:
                        # when data key does not exist
                        d = {'session_id': content.envelope.get('sessionId'),
                             'bs_time': bs_.get('time'),
                             'bs_event': bs_.get('event'),
                             'bs_data_visible': float("NaN"),
                             'bs_data_docWidth': float("NaN"),
                             'bs_data_docHeight': float("NaN"),
                             'bs_data_screenWidth': float("NaN"),
                             'bs_data_screenHeight': float("NaN"),
                             'bs_data_scrollX': float("NaN"),
                             'bs_data_scrollY': float("NaN"),
                             'bs_data_pointerX': float("NaN"),
                             'bs_data_pointerY': float("NaN"),
                             'bs_data_activityTime': float("NaN"),
                             '_id': content['_id']

                             }
                        df_bs = df_bs.append(d, ignore_index=True)

            else:
                # when complete row is NaN
                d = {'session_id': content.envelope.get('sessionId'),
                     'bs_time': float("NaN"),
                     'bs_event': float("NaN"),
                     'bs_data_visible': float("NaN"),
                     'bs_data_docWidth': float("NaN"),
                     'bs_data_docHeight': float("NaN"),
                     'bs_data_screenWidth': float("NaN"),
                     'bs_data_screenHeight': float("NaN"),
                     'bs_data_scrollX': float("NaN"),
                     'bs_data_scrollY': float("NaN"),
                     'bs_data_pointerX': float("NaN"),
                     'bs_data_pointerY': float("NaN"),
                     'bs_data_activityTime': float("NaN"),
                     '_id': content['_id']

                     }

                df_bs = df_bs.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_bs.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_bs.to_csv(f'{path}{header}_data.csv')

        print(f'Output>>   Update completed.. New rows added:  {df_bs.shape[0] - old_shape[0]} ')


def clean_update_data_tm(df):
    # This will find out the recent updated rows into the database and update df_pointer table for new data.( it will save unnecessary processing)

    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'tm'

    if os.path.isfile(f'{path}{header}_data.csv'):
        df_tm = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_tm.shape

        if len(df_tm) != 0:

            df_new_tm = df[df.timestamp > df[df._id == ObjectId(df_tm['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_tm = df.copy()
    else:
        df_new_tm = df.copy()
        old_shape = np.array([0, 0])
        df_tm = pd.DataFrame(columns=['session_id', 'tm_date', 'tm_time', '_id'])

    if len(df_new_tm) == 0:
        print('Output>>    No update is found in TIMESTAMP data.. New data is same as saved one.')
    else:
        print(f'Output>>    New activities captured: {len(df_new_tm)} ')

        for index, content in df_new_tm.iterrows():
            date_time = datetime.fromtimestamp(content.timestamp / 1000)

            d = {'session_id': content.envelope.get('sessionId'),
                 'tm_date': date_time.date(),
                 'tm_time': date_time.time(),
                 '_id': content['_id']

                 }
            df_tm = df_tm.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_tm.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_tm.to_csv(f'{path}{header}_data.csv')

        print(f'Output>>   Update completed.. New rows added:  {df_tm.shape[0] - old_shape[0]} ')


def clean_update_data_rs(df):
    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'rs'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_rs = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_rs.shape

        if len(df_rs) != 0:

            df_new_rs = df[df.timestamp > df[df._id == ObjectId(df_rs['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_rs = df.copy()
    else:
        df_new_rs = df.copy()
        old_shape = np.array([0, 0])
        df_rs = pd.DataFrame(columns=['session_id', 'rs_time', 'rs_event', 'rs_data_width', 'rs_data_height', '_id'])

    if len(df_new_rs) == 0:
        print('Output>>    No update is found in RESIZE data.. New data is same as saved one')
    else:
        print(f'Output>>    New activities captured: {len(df_new_rs)} ')

        # iterating on rows of dataframe
        for index, content in df_new_rs.iterrows():
            if isinstance(content.resize, list):

                # iterating on list (in connection col)
                for rs_ in content.resize:

                    # chcecking if dictionary has 'data' key
                    if 'data' in rs_.keys():

                        # when all data is available
                        d = {'session_id': content.envelope.get('sessionId'),
                             'rs_time': rs_.get('time'),
                             'rs_event': rs_.get('event'),
                             'rs_data_width': rs_.get('data').get('width'),
                             'rs_data_height': rs_.get('data').get('height'),
                             '_id': content['_id']

                             }

                        df_rs = df_rs.append(d, ignore_index=True)
                    else:
                        # when data key does not exist
                        d = {'session_id': content.envelope.get('sessionId'),
                             'rs_time': rs_.get('time'),
                             'rs_event': rs_.get('event'),
                             'rs_data_width': float("NaN"),
                             'rs_data_height': float("NaN"),
                             '_id': content['_id']

                             }
                        df_rs = df_rs.append(d, ignore_index=True)

            else:
                # when complete row is NaN
                d = {'session_id': content.envelope.get('sessionId'),
                     'rs_time': float("NaN"),
                     'rs_event': float("NaN"),
                     'rs_data_width': float("NaN"),
                     'rs_data_height': float("NaN"),
                     '_id': content['_id']

                     }

                df_rs = df_rs.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_rs.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_rs.to_csv(f'{path}{header}_data.csv')

        print(f'Output>>   Update completed.. New rows added:  {df_rs.shape[0] - old_shape[0]} ')


def clean_update_data_doc(df):
    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'doc'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_doc = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_doc.shape

        if len(df_doc) != 0:

            df_new_doc = df[df.timestamp > df[df._id == ObjectId(df_doc['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_doc = df.copy()
    else:
        df_new_doc = df.copy()
        old_shape = np.array([0, 0])
        df_doc = pd.DataFrame(
            columns=['session_id', 'doc_time', 'doc_event', 'doc_data_width', 'doc_data_height', '_id'])

    if len(df_new_doc) == 0:
        print('Output>>    No update is found in DOC data.. New data is same as saved one')
    else:
        print(f'Output>>    New activities captured: {len(df_new_doc)} ')

        # iterating on rows of dataframe
        for index, content in df_new_doc.iterrows():
            if isinstance(content.doc, list):

                # iterating on list (in connection col)
                for doc_ in content.doc:

                    # chcecking if dictionary has 'data' key
                    if 'data' in doc_.keys():

                        # when all data is available
                        d = {'session_id': content.envelope.get('sessionId'),
                             'doc_time': doc_.get('time'),
                             'doc_event': doc_.get('event'),
                             'doc_data_width': doc_.get('data').get('width'),
                             'doc_data_height': doc_.get('data').get('height'),
                             '_id': content['_id']

                             }

                        df_doc = df_doc.append(d, ignore_index=True)
                    else:
                        # when data key does not exist
                        d = {'session_id': content.envelope.get('sessionId'),
                             'doc_time': doc_.get('time'),
                             'doc_event': doc_.get('event'),
                             'doc_data_width': float("NaN"),
                             'doc_data_height': float("NaN"),
                             '_id': content['_id']

                             }
                        df_doc = df_doc.append(d, ignore_index=True)

            else:
                # when complete row is NaN
                d = {'session_id': content.envelope.get('sessionId'),
                     'doc_time': float("NaN"),
                     'doc_event': float("NaN"),
                     'doc_data_width': float("NaN"),
                     'doc_data_height': float("NaN"),
                     '_id': content['_id']

                     }

                df_doc = df_doc.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_doc.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_doc.to_csv(f'{path}{header}_data.csv')

        print(f'Output>>   Update completed.. New rows added:  {df_doc.shape[0] - old_shape[0]} ')


def clean_update_data_upg(df):
    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'upg'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_upg = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_upg.shape

        if len(df_upg) != 0:

            df_new_upg = df[df.timestamp > df[df._id == ObjectId(df_upg['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_upg = df.copy()
    else:
        df_new_upg = df.copy()
        old_shape = np.array([0, 0])
        df_upg = pd.DataFrame(columns=['session_id', 'upg_time', 'upg_event', 'upg_data_key', '_id'])

    if len(df_new_upg) == 0:
        print('Output>>    No update is found in UPGRADE data.. New data is same as saved one')
    else:
        print(f'Output>>    New activities captured: {len(df_new_upg)} ')

        # iterating on rows of dataframe
        for index, content in df_new_upg.iterrows():
            if isinstance(content.upgrade, list):

                # iterating on list (in connection col)
                for upg_ in content.upgrade:

                    # chcecking if dictionary has 'data' key
                    if 'data' in upg_.keys():

                        # when all data is available
                        d = {'session_id': content.envelope.get('sessionId'),
                             'upg_time': upg_.get('time'),
                             'upg_event': upg_.get('event'),
                             'upg_data_key': upg_.get('data').get('key'),
                             '_id': content['_id']

                             }

                        df_upg = df_upg.append(d, ignore_index=True)
                    else:
                        # when data key does not exist
                        d = {'session_id': content.envelope.get('sessionId'),
                             'upg_time': upg_.get('time'),
                             'upg_event': upg_.get('event'),
                             'upg_data_key': float("NaN"),
                             '_id': content['_id']

                             }
                        df_upg = df_upg.append(d, ignore_index=True)

            else:
                # when complete row is NaN
                d = {'session_id': content.envelope.get('sessionId'),
                     'upg_time': float("NaN"),
                     'upg_event': float("NaN"),
                     'upg_data_key': float("NaN"),
                     '_id': content['_id']

                     }

                df_upg = df_upg.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_upg.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_upg.to_csv(f'{path}{header}_data.csv')

        print(f'Output>>   Update completed.. New rows added:  {df_upg.shape[0] - old_shape[0]} ')


def clean_update_data_sel(df):
    # This will find out the recent updated rows into the database and update df_sel table for new data.( it will
    # save unnecessary processing)

    global df_sel
    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'sel'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_sel = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_sel.shape
        if len(df_sel) != 0:

            df_new_sel = df[df.timestamp > df[df._id == ObjectId(df_sel['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_sel = df.copy()
    else:
        df_new_sel = df.copy()
        old_shape = np.array([0, 0])
        df_sel = pd.DataFrame(
            columns=['session_id', 'sel_time', 'sel_event', 'sel_data_start', 'sel_data_startOffset',
                     'sel_data_end', 'sel_data_endOffset', '_id'])

    if len(df_new_sel) == 0:
        print('Output>>    No update is found in SELECTION data.. New data is same as saved one.')
    else:
        print(f'Output>>    New activities captured:{len(df_new_sel)} ')

        for index, content in df_new_sel.iterrows():
            if isinstance(content.selection, list):
                for sel_ in content.selection:
                    d = {'session_id': content.envelope.get('sessionId'),
                         'sel_time': sel_.get('time'),
                         'sel_event': sel_.get('event'),
                         'sel_data_start': sel_.get('data').get('start'),
                         'sel_data_startOffset': sel_.get('data').get('startOffset'),
                         'sel_data_end': sel_.get('data').get('end'),
                         'sel_data_endOffset': sel_.get('data').get('endOffset'),
                         '_id': content['_id']}

                    df_sel = df_sel.append(d, ignore_index=True)
            else:

                d = {'session_id': content.envelope.get('sessionId'),
                     'sel_time': float("NaN"),
                     'sel_event': float("NaN"),
                     'sel_data_start': float("NaN"),
                     'sel_data_startOffset': float("NaN"),
                     'sel_data_end': float("NaN"),
                     'sel_data_endOffset': float("NaN"),
                     '_id': content['_id']}
                df_sel = df_sel.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_sel.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_sel.to_csv(f'{path}{header}_data.csv')
        print(f'Output>>   Update completed.. New rows added: {df_sel.shape[0] - old_shape[0]} ')


def clean_update_data_tml(df):
    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'tml'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_tml = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_tml.shape

        if len(df_tml) != 0:

            df_new_tml = df[df.timestamp > df[df._id == ObjectId(df_tml['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_tml = df.copy()
    else:
        df_new_tml = df.copy()
        old_shape = np.array([0, 0])
        df_tml = pd.DataFrame(
            columns=['session_id', 'tml_time', 'tml_event', 'tml_data_type', 'tml_data_hash', 'tml_data_x',
                     'tml_data_y', 'tml_data_reaction', 'tml_data_context', '_id'])

    if len(df_new_tml) == 0:
        print('Output>>    No update is found in TIMELINE data.. New data is same as saved one')
    else:
        print(f'Output>>    New activities captured: {len(df_new_tml)} ')

        # iterating on rows of dataframe
        for index, content in df_new_tml.iterrows():
            if isinstance(content.timeline, list):

                # iterating on list (in connection col)
                for tml_ in content.timeline:

                    # chcecking if dictionary has 'data' key
                    if 'data' in tml_.keys():

                        # when all data is available
                        d = {'session_id': content.envelope.get('sessionId'),
                             'tml_time': tml_.get('time'),
                             'tml_event': tml_.get('event'),
                             'tml_data_type': tml_.get('data').get('type'),
                             'tml_data_hash': tml_.get('data').get('hash'),
                             'tml_data_x': tml_.get('data').get('x'),
                             'tml_data_y': tml_.get('data').get('y'),
                             'tml_data_reaction': tml_.get('data').get('reaction'),
                             'tml_data_context': tml_.get('data').get('context'),
                             '_id': content['_id']

                             }

                        df_tml = df_tml.append(d, ignore_index=True)
                    else:
                        # when data key does not exist
                        d = {'session_id': content.envelope.get('sessionId'),
                             'tml_time': tml_.get('time'),
                             'tml_event': tml_.get('event'),
                             'tml_data_type': float("NaN"),
                             'tml_data_hash': float("NaN"),
                             'tml_data_x': float("NaN"),
                             'tml_data_y': float("NaN"),
                             'tml_data_reaction': float("NaN"),
                             'tml_data_context': float("NaN"),
                             '_id': content['_id']

                             }
                        df_tml = df_tml.append(d, ignore_index=True)

            else:
                # when complete row is NaN
                d = {'session_id': content.envelope.get('sessionId'),
                     'tml_time': float("NaN"),
                     'tml_event': float("NaN"),
                     'tml_data_type': float("NaN"),
                     'tml_data_hash': float("NaN"),
                     'tml_data_x': float("NaN"),
                     'tml_data_y': float("NaN"),
                     'tml_data_reaction': float("NaN"),
                     'tml_data_context': float("NaN"),
                     '_id': content['_id']

                     }

                df_tml = df_tml.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_tml.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_tml.to_csv(f'{path}{header}_data.csv')

        print(f'Output>>   Update completed.. New rows added:  {df_tml.shape[0] - old_shape[0]} ')


def clean_update_data_img(df):
    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'img'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_img = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_img.shape

        if len(df_img) != 0:

            df_new_img = df[df.timestamp > df[df._id == ObjectId(df_img['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_img = df.copy()
    else:
        df_new_img = df.copy()
        old_shape = np.array([0, 0])
        df_img = pd.DataFrame(
            columns=['session_id', 'img_time', 'img_event', 'img_data_source', 'img_data_target', '_id'])

    if len(df_new_img) == 0:
        print('Output>>    No update is found in IMAGE data.. New data is same as saved one')
    else:
        print(f'Output>>    New activities captured: {len(df_new_img)} ')

        # iterating on rows of dataframe
        for index, content in df_new_img.iterrows():
            if isinstance(content.image, list):

                # iterating on list (in connection col)
                for img_ in content.image:

                    # chcecking if dictionary has 'data' key
                    if 'data' in img_.keys():

                        # when all data is available
                        d = {'session_id': content.envelope.get('sessionId'),
                             'img_time': img_.get('time'),
                             'img_event': img_.get('event'),
                             'img_data_source': img_.get('data').get('source'),
                             'img_data_target': img_.get('data').get('target'),
                             '_id': content['_id']

                             }

                        df_img = df_img.append(d, ignore_index=True)
                    else:
                        # when data key does not exist
                        d = {'session_id': content.envelope.get('sessionId'),
                             'img_time': img_.get('time'),
                             'img_event': img_.get('event'),
                             'img_data_source': float("NaN"),
                             'img_data_target': float("NaN"),
                             '_id': content['_id']

                             }
                        df_img = df_img.append(d, ignore_index=True)

            else:
                # when complete row is NaN
                d = {'session_id': content.envelope.get('sessionId'),
                     'img_time': float("NaN"),
                     'img_event': float("NaN"),
                     'img_data_source': float("NaN"),
                     'img_data_target': float("NaN"),
                     '_id': content['_id']

                     }

                df_img = df_img.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_img.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_img.to_csv(f'{path}{header}_data.csv')


        print(f'Output>>   Update completed.. New rows added:  {df_img.shape[0] - old_shape[0]} ')


def clean_update_data_unl(df):
    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'unl'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_unl = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_unl.shape

        if len(df_unl) != 0:

            df_new_unl = df[df.timestamp > df[df._id == ObjectId(df_unl['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_unl = df.copy()
    else:
        df_new_unl = df.copy()
        old_shape = np.array([0, 0])
        df_unl = pd.DataFrame(columns=['session_id', 'unl_time', 'unl_event', 'unl_data_name', '_id'])

    if len(df_new_unl) == 0:
        print('Output>>    No update is found in UNLOAD data.. New data is same as saved one')
    else:
        print(f'Output>>    New activities captured: {len(df_new_unl)} ')

        # iterating on rows of dataframe
        for index, content in df_new_unl.iterrows():
            if isinstance(content.unload, list):

                # iterating on list (in connection col)
                for unl_ in content.unload:

                    # chcecking if dictionary has 'data' key
                    if 'data' in unl_.keys():

                        # when all data is available
                        d = {'session_id': content.envelope.get('sessionId'),
                             'unl_time': unl_.get('time'),
                             'unl_event': unl_.get('event'),
                             'unl_data_name': unl_.get('data').get('name'),
                             '_id': content['_id']

                             }

                        df_unl = df_unl.append(d, ignore_index=True)
                    else:
                        # when data key does not exist
                        d = {'session_id': content.envelope.get('sessionId'),
                             'unl_time': unl_.get('time'),
                             'unl_event': unl_.get('event'),
                             'unl_data_name': float("NaN"),
                             '_id': content['_id']

                             }
                        df_unl = df_unl.append(d, ignore_index=True)

            else:
                # when complete row is NaN
                d = {'session_id': content.envelope.get('sessionId'),
                     'unl_time': float("NaN"),
                     'unl_event': float("NaN"),
                     'unl_data_name': float("NaN"),
                     '_id': content['_id']

                     }

                df_unl = df_unl.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_unl.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_unl.to_csv(f'{path}{header}_data.csv')

        print(f'Output>>   Update completed.. New rows added:  {df_unl.shape[0] - old_shape[0]} ')


def clean_update_data_mt(df):
    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'mt'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_mt = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_mt.shape

        if len(df_mt) != 0:

            df_new_mt = df[df.timestamp > df[df._id == ObjectId(df_mt['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_mt = df.copy()
    else:
        df_new_mt = df.copy()
        old_shape = np.array([0, 0])
        df_mt = pd.DataFrame(
            columns=['session_id', 'mt_time', 'mt_event', 'mt_data_ClientTimestamp_0', 'mt_data_Playback_1',
                     'mt_data_TotalBytes_2',
                     'mt_data_LayoutCost_3', 'mt_data_TotalCost_4', 'mt_data_InvokeCount_5',
                     'mt_data_ThreadBlockedTime_6', 'mt_data_LongTaskCount_7', 'mt_data_LargestPaint_8',
                     'mt_data_CumulativeLayoutShift_9', 'mt_data_FirstInputDelay_10', 'mt_data_RatingValue_11',
                     'mt_data_RatingCount_12', 'mt_data_ProductPrice_13', '_id'])

    if len(df_new_mt) == 0:
        print('Output>>    No update is found in METRIC data.. New data is same as saved one')
    else:
        print(f'Output>>    New activities captured: {len(df_new_mt)} ')

        # iterating on rows of dataframe
        for index, content in df_new_mt.iterrows():
            if isinstance(content.metric, list):

                # iterating on list (in connection col)
                for mt_ in content.metric:

                    # chcecking if dictionary has 'data' key
                    if 'data' in mt_.keys():

                        # when all data is available
                        d = {'session_id': content.envelope.get('sessionId'),
                             'mt_time': mt_.get('time'),
                             'mt_event': mt_.get('event'),
                             'mt_data_ClientTimestamp_0': mt_.get('data').get('0'),
                             'mt_data_Playback_1': mt_.get('data').get('1'),
                             'mt_data_TotalBytes_2': mt_.get('data').get('2'),
                             'mt_data_LayoutCost_3': mt_.get('data').get('3'),
                             'mt_data_TotalCost_4': mt_.get('data').get('4'),
                             'mt_data_InvokeCount_5': mt_.get('data').get('5'),
                             'mt_data_ThreadBlockedTime_6': mt_.get('data').get('6'),
                             'mt_data_LongTaskCount_7': mt_.get('data').get('7'),
                             'mt_data_LargestPaint_8': mt_.get('data').get('8'),
                             'mt_data_CumulativeLayoutShift_9': mt_.get('data').get('9'),
                             'mt_data_FirstInputDelay_10': mt_.get('data').get('10'),
                             'mt_data_RatingValue_11': mt_.get('data').get('11'),
                             'mt_data_RatingCount_12': mt_.get('data').get('12'),
                             'mt_data_ProductPrice_13': mt_.get('data').get('13'),
                             '_id': content['_id']

                             }

                        df_mt = df_mt.append(d, ignore_index=True)
                    else:
                        # when data key does not exist
                        d = {'session_id': content.envelope.get('sessionId'),
                             'mt_time': mt_.get('time'),
                             'mt_event': mt_.get('event'),
                             'mt_data_ClientTimestamp_0': float("NaN"),
                             'mt_data_Playback_1': float("NaN"),
                             'mt_data_TotalBytes_2': float("NaN"),
                             'mt_data_LayoutCost_3': float("NaN"),
                             'mt_data_TotalCost_4': float("NaN"),
                             'mt_data_InvokeCount_5': float("NaN"),
                             'mt_data_ThreadBlockedTime_6': float("NaN"),
                             'mt_data_LongTaskCount_7': float("NaN"),
                             'mt_data_LargestPaint_8': float("NaN"),
                             'mt_data_CumulativeLayoutShift_9': float("NaN"),
                             'mt_data_FirstInputDelay_10': float("NaN"),
                             'mt_data_RatingValue_11': float("NaN"),
                             'mt_data_RatingCount_12': float("NaN"),
                             'mt_data_ProductPrice_13': float("NaN"),
                             '_id': content['_id']

                             }
                        df_mt = df_mt.append(d, ignore_index=True)

            else:
                # when complete row is NaN
                d = {'session_id': content.envelope.get('sessionId'),
                     'mt_time': float("NaN"),
                     'mt_event': float("NaN"),
                     'mt_data_ClientTimestamp_0': float("NaN"),
                     'mt_data_Playback_1': float("NaN"),
                     'mt_data_TotalBytes_2': float("NaN"),
                     'mt_data_LayoutCost_3': float("NaN"),
                     'mt_data_TotalCost_4': float("NaN"),
                     'mt_data_InvokeCount_5': float("NaN"),
                     'mt_data_ThreadBlockedTime_6': float("NaN"),
                     'mt_data_LongTaskCount_7': float("NaN"),
                     'mt_data_LargestPaint_8': float("NaN"),
                     'mt_data_CumulativeLayoutShift_9': float("NaN"),
                     'mt_data_FirstInputDelay_10': float("NaN"),
                     'mt_data_RatingValue_11': float("NaN"),
                     'mt_data_RatingCount_12': float("NaN"),
                     'mt_data_ProductPrice_13': float("NaN"),
                     '_id': content['_id']

                     }

                df_mt = df_mt.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_mt.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_mt.to_csv(f'{path}{header}_data.csv')

        print(f'Output>>   Update completed.. New rows added:  {df_mt.shape[0] - old_shape[0]} ')


def clean_update_data_cus(df):
    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'cus'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_cus = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_cus.shape

        if len(df_cus) != 0:

            df_new_cus = df[df.timestamp > df[df._id == ObjectId(df_cus['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_cus = df.copy()
    else:
        df_new_cus = df.copy()
        old_shape = np.array([0, 0])
        df_cus = pd.DataFrame(columns=['session_id', 'cus_time', 'cus_event', 'cus_data_key', 'cus_data_value', '_id'])

    if len(df_new_cus) == 0:
        print('Output>>    No update is found in CUSTOM data.. New data is same as saved one')
    else:
        print(f'Output>>    New activities captured: {len(df_new_cus)} ')

        # iterating on rows of dataframe
        for index, content in df_new_cus.iterrows():
            if isinstance(content.custom, list):

                # iterating on list (in connection col)
                for cus_ in content.custom:

                    # chcecking if dictionary has 'data' key
                    if 'data' in cus_.keys():

                        # when all data is available
                        d = {'session_id': content.envelope.get('sessionId'),
                             'cus_time': cus_.get('time'),
                             'cus_event': cus_.get('event'),
                             'cus_data_key': cus_.get('data').get('key'),
                             'cus_data_value': cus_.get('data').get('value'),
                             '_id': content['_id']

                             }

                        df_cus = df_cus.append(d, ignore_index=True)
                    else:
                        # when data key does not exist
                        d = {'session_id': content.envelope.get('sessionId'),
                             'cus_time': cus_.get('time'),
                             'cus_event': cus_.get('event'),
                             'cus_data_key': float("NaN"),
                             'cus_data_value': float("NaN"),
                             '_id': content['_id']

                             }
                        df_cus = df_cus.append(d, ignore_index=True)

            else:
                # when complete row is NaN
                d = {'session_id': content.envelope.get('sessionId'),
                     'cus_time': float("NaN"),
                     'cus_event': float("NaN"),
                     'cus_data_key': float("NaN"),
                     'cus_data_value': float("NaN"),
                     '_id': content['_id']

                     }

                df_cus = df_cus.append(d, ignore_index=True)

        t = datetime.now()
        timestamp = str(t.year) + '_' + str(t.month) + '_' + str(t.day) + '_' + str(t.hour) + '_' + str(
            t.minute) + '_' + str(t.second)
        df_cus.to_csv(f'{path}backup\\{header}_data_{timestamp}.csv')

        df_cus.to_csv(f'{path}{header}_data.csv')

        print(f'Output>>   Update completed.. New rows added:  {df_cus.shape[0] - old_shape[0]} ')


def clean_update_data_geo(df):
    # This will find out the recent updated rows into the database and update df_pointer table for new data.( it will save unnecessary processing)

    path = 'D:\DataWorld\lacritz\data\clarity\\'
    header = 'geo'

    # df = readdata()
    if os.path.isfile(f'{path}{header}_data.csv'):
        df_geo = pd.read_csv(f'{path}{header}_data.csv', index_col=0)
        old_shape = df_geo.shape

        if len(df_geo) != 0:

            df_new_geo = df[df.timestamp > df[df._id == ObjectId(df_geo['_id'].iloc[-1])].timestamp.values[0]]
        else:

            df_new_geo = df.copy()
    else:
        df_new_geo = df.copy()
        old_shape = np.array([0, 0])

        df_geo = pd.DataFrame(columns=['session_id', 'geo_ip', 'geo_city', 'geo_region', 'geo_country',
                                       'geo_loc', 'geo_continent', '_id'])

    if len(df_new_geo) == 0:
        print('Output>>    No update is found in GEO location data.. New data is same as saved one.')
    else:
        print(f'Output>>    New activities captured: {len(df_new_geo)} ')

        # fetching geo location for each ip address
        print('api call started', datetime.now())

        df_tmp = pd.DataFrame(columns=['geo_city', 'geo_region', 'geo_country', 'geo_loc', 'geo_continent'])
        for ip_add in df.ip.str.lstrip('f[]:').unique():
            t_d = ipInfo(ip_add)
            d = {'geo_ip': ip_add,
                 'geo_city': t_d.get('city'),
                 'geo_region': t_d.get('region_name'),
                 'geo_country': t_d.get('country_name'),
                 'geo_loc': (t_d.get('latitude'), t_d.get('longitude')),
                 'geo_continent': t_d.get('continent_name')
                 # 'geo_ip': ip_add
                 }
            df_tmp = df_tmp.append(d, ignore_index=True)
        df_tmp.set_index('geo_ip', inplace=True)
        print('api call done', datetime.now())

        for index, content in df_new_geo.iterrows():
            t_ip = content.ip.lstrip(':f')

            d = dict(df_tmp.loc[t_ip])
            d['session_id'] = content.envelope.get('sessionId')
            d['_id'] = content['_id']
            d['geo_ip'] = t_ip

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
    url = 'http://api.ipstack.com/' + addr + '?access_key=fadd729c90cc4c29f31b90120d7e6442&format=1'
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

    print('Info>>    CONNECTION header table is being updated...')
    clean_update_data_cn(df)

    print('Info>>    ENVELOPE header table is being updated...')
    clean_update_data_env(df)

    print('Info>>    TIMESTAMP header table is being updated...')
    clean_update_data_tm(df)

    print('Info>>    GEO Location header table is being updated...')
    clean_update_data_geo(df)

    print('Info>>    DOM header table is being updated...')
    clean_update_data_dom(df)

    print('Info>>    PING header table is being updated...')
    clean_update_data_pn(df)

    print('Info>>    LIMIT header table is being updated...')
    clean_update_data_lm(df)

    print('Info>>    UPLOAD header table is being updated...')
    clean_update_data_upl(df)

    print('Info>>    VISIBILITY header table is being updated...')
    clean_update_data_vs(df)

    print('Info>>    SUMMARY header table is being updated...')
    clean_update_data_sm(df)

    print('Info>>    BASELINE header table is being updated...')
    clean_update_data_bs(df)

    print('Info>>    SCROLL header table is being updated...')
    clean_update_data_scroll(df)

    print('Info>>    RESIZE header table is being updated...')
    clean_update_data_rs(df)

    print('Info>>    UNLOAD header table is being updated...')
    clean_update_data_unl(df)

    print('Info>>    METRIC header table is being updated...')
    clean_update_data_mt(df)

    print('Info>>    CUSTOM header table is being updated...')
    clean_update_data_cus(df)

    print('Info>>    DOC header table is being updated...')
    clean_update_data_doc(df)

    print('Info>>    UPGRADE header table is being updated...')
    clean_update_data_upg(df)

    print('Info>>    SELECTION header table is being updated...')
    clean_update_data_sel(df)

    print('Info>>    TIMELINE header table is being updated...')
    clean_update_data_tml(df)

    print('Info>>    IMAGE header table is being updated...')
    clean_update_data_img(df)

    print('Info>>    LOG header table is being updated...')
    clean_update_data_log(df)

    print('All header tables are updated successfully with current mongodb data.')
