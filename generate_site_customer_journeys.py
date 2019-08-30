import json
import boto3
import math
import os
from baseline_service.client import BaselineService
from transaction_service.clientV2 import TransactionService
from merchant_service.client import MerchantService
from upside_core.services import create_private_service_with_retries
from pyathena import connect
import pandas as pd
import numpy as np
from tqdm import *
import subprocess

tqdm.pandas()

USER_CARDS_FOUND_RESPONSE_CODE = 17210
USER_CARDS_NOT_FOUND_RESPONSE_CODE = 17414
USER_SITE_METADATA_FOUND_RESPONSE_CODE = 17220
BASELINE_BUCKET = 'prod-baselines.upside-services.com'
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

Grid = []
for Year in range(2010, 2025):
    for Month in range(1, 13):
        Period = Year * 100 + Month
        Grid = Grid + [Period]

conn = connect(aws_access_key_id=AWS_ACCESS_KEY_ID,
               aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
               s3_staging_dir='s3://aws-athena-query-results-337068080576-us-east-1/',
               region_name='us-east-1')

baseline_service = create_private_service_with_retries('prod', BaselineService)
transaction_service = create_private_service_with_retries('prod', TransactionService)

s3 = boto3.resource('s3')


def get_list_if_string(object):
    """
    Converts a string to a list
    """
    if type(object) == str:
        object = [object]
    return object


def get_list_for_query(list_to_prepare):
    """
    Prepares a list so that it can be inserted into an Athena query
    """
    list_to_prepare = get_list_if_string(list_to_prepare)

    if len(list_to_prepare) == 1:
        query_syntax = "('" + list_to_prepare[0] + "')"
    else:
        query_syntax = str(tuple(list_to_prepare))
    return query_syntax


def get_crawledtxuuid_by_usertxuuid(user_txuuid, site_uuid):
    """
    Returns the crawledTxUuid from the transaction_service for a userTxUuid.
    Returns none if no crawledTxUuid has been associated with a userTxUuid from RMP.
    """
    try:
        crawledTxUuid = \
        transaction_service.tx_info_get_by_utx_uuid_and_site(user_transaction_uuid=user_txuuid, site_uuid=site_uuid)[
            'payload']['transaction']['crawledTxUuid']
    except Exception:
        crawledTxUuid = None
    return crawledTxUuid


def get_fee_calc_json(site_uuid, usertx_uuid, calc_detail_only=False):
    """
    Get the fee calc for a usertx_uuid
    """
    path = f'fee-calculations/pSiteUuid={site_uuid}/{usertx_uuid}.json'

    content_object = s3.Object("prod-fee-calculations.upside-services.com", path)
    file_content = json.loads(content_object.get()['Body'].read().decode('utf-8'))

    if calc_detail_only == True:
        try:
            file_content = json.loads(file_content['calculationDetail']['data'])
        except Exception:
            file_content = None

    return file_content


def get_df_from_athena_query(query, conn):
    cursor = conn.cursor()
    result = cursor.execute(query)
    outdir = './'
    command = 'aws s3 cp s3://aws-athena-query-results-337068080576-us-east-1/' + result.query_id + '.csv  ' + outdir
    os.system(command)
    outdf = pd.read_csv(outdir + result.query_id + '.csv').reset_index(drop=True)
    os.system("rm " + outdir + result.query_id + '.csv')
    return outdf


def get_incremental(site_uuid, source_terminal_filter=None, user_uuid_filter=None, add_crawledtx_uuid=False):
    """
    Queries Athena to get incremental for site_uuid(s)

    Keyword arguments:
    site_uuid -- a site_uuid or list of site_uuids
    source_terminal_filter -- the source_terminal(s) of the incremental transactions (list or string) (default None)
    user_uuid_filter -- option to filter results to specific users (default None)
    add_crawledtx_uuid -- option to add a column for the crawledTxUuid (if available) (default False)
    """
    site_uuid_list = get_list_for_query(site_uuid)

    query = f"SELECT * " \
            f" FROM prod_incremental.incremental_reviewed " \
            f" WHERE siteuuid in {site_uuid_list} " \
            f" ORDER BY date "

    dataframe_result = get_df_from_athena_query(query, conn)

    if source_terminal_filter is not None:
        source_terminal_filter = get_list_if_string(source_terminal_filter)
        dataframe_result = dataframe_result.query("sourceterminal in @source_terminal_filter").reset_index(drop=True)

    if user_uuid_filter is not None:
        user_uuid_filter = get_list_if_string(user_uuid_filter)
        dataframe_result = dataframe_result.query("useruuid in @user_uuid_filter").reset_index(drop=True)

    if add_crawledtx_uuid == True:
        dataframe_result['crawledTxUuid'] = dataframe_result.progress_apply(
            lambda x: get_crawledtxuuid_by_usertxuuid(x['usertxuuid'], x['siteuuid']), axis=1)
    return dataframe_result


def get_all_user_uuid_from_incremental(site_uuid, source_terminal_filter=None, incremental=None):
    """
    Get all useruuid that have a transaction in incremental at a site_uuid

    Keyword arguments:
    site_uuid -- a site_uuid or list of site_uuids
    source_terminal -- the source_terminal of the incremental transactions (list or string)
    incremental -- rather than run a query, pass the incremental dataframe in
    """
    if incremental is None:
        incremental = get_incremental(site_uuid, source_terminal_filter)

    return list(incremental.useruuid.unique())


def get_user_card_ids(user_uuid):
    """
    Get all cardIds associated with a user_uuid from the baseline service

    Keyword arguments:
    user_uuid: a single user_uuid
    """
    user_cards_response = baseline_service.get_user_cards_by_user(user_uuid)
    if user_cards_response['upsideCode'] != USER_CARDS_FOUND_RESPONSE_CODE:
        return []
    return [c['cardId'] for c in user_cards_response['payload']]


def get_user_card_site_shadows(site_uuid, user_uuid, card_id):
    """
    Return shadow json for user cardId at a site

    Keyword arguments:
    site_uuid: a single site_uuid
    user_uuid: a single user_uuid
    card_id: a single card_id belonging to the user_uuid
    """
    card_id = card_id.replace(' ', '_')
    path = f'v2/shadows/pUserUuid={user_uuid}/pSiteUuid={site_uuid}/pTransactionType=GAS/{card_id}.json'
    content_object = s3.Object(BASELINE_BUCKET, path)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    return json.loads(file_content)


def get_shadow_card_ids(site_uuid, user_uuid, card_id):
    """
    Return list of shadow cardids for user cardId at a site

    Keyword arguments:
    site_uuid: a single site_uuid
    user_uuid: a single user_uuid
    card_id: a single card_id belonging to the user_uuid
    """
    shadow_objects = get_user_card_site_shadows(site_uuid, user_uuid, card_id)
    return [s['shadowCardId'] for s in shadow_objects['shadows']]


def get_user_card_to_shadows_map(site_uuid, user_uuid, user_card_ids):
    """
    Return a dict with keys of user_card_ids and values the shadows
    associated with the cardId for the user_uuid at the site_uuid

    Keyword arguments:
    site_uuid: a single site_uuid
    user_uuid: a single user_uuid
    card_id: a list of cardIds belonging to user

    """
    user_card_to_shadows = {}
    for card_id in user_card_ids:
        user_card_to_shadows[card_id] = []
        try:
            shadow_card_ids = get_shadow_card_ids(site_uuid, user_uuid, card_id)
            user_card_to_shadows[card_id] = shadow_card_ids
        except Exception:
            pass
    return user_card_to_shadows


def get_crawled_txns(site_uuid, source_terminal_filter=None, card_id_filter=None, F6L4_filter=None):
    """
    Queries Athena to be crawled transactions for site_uuid(s)

    Keyword arguments:
    site_uuid -- a site_uuid or list of site_uuids
    source_terminal_filter -- the source_terminal(s) of the incremental transactions (list or string) (default None)
    card_id_filter -- option to filter results to specfic card_id(s) (default None)
    F6L4_filter -- option to filter results to specific F6L4 (e.g. '123456-1234') (list or string) (default None)
    """
    site_uuid_list = get_list_for_query(site_uuid)

    query = f"SELECT siteuuid as siteUuid, " \
            f"  CAST(From_iso8601_timestamp(transactiontimestamp) AS timestamp) as transactionTimestamp, " \
            f"  year(CAST(From_iso8601_timestamp(transactiontimestamp) AS timestamp))*100 + month(CAST(From_iso8601_timestamp(transactiontimestamp) AS timestamp)) as TranYearMonth," \
            f"  cardid as cardId, " \
            f"  cardtype as cardType, " \
            f"  cardlastfour as cardLastFour, " \
            f"  cardfirstsix as cardFirstSix, " \
            f"  total.amount as totalAmount, " \
            f"  tax.amount as taxamount, " \
            f"  transactionuuid as transactionUuid, " \
            f"  sourceterminal as sourceTerminal, " \
            f"  status, " \
            f"  datasource, " \
            f"  pdatasource, " \
            f"  addedattimestamp as addedAtTimeStamp " \
            f" FROM prod_transaction.refined " \
            f" WHERE psiteuuid in {site_uuid_list} " \
            f" AND status not in ('DECLINED') "

    if source_terminal_filter is not None:
        source_terminal_filter = get_list_for_query(source_terminal_filter)
        query = f'{query} AND sourceterminal in {source_terminal_filter} '

    if card_id_filter is not None:
        card_id_filter = get_list_for_query(card_id_filter)
        query = f'{query} AND cardid in {card_id_filter} '

    if F6L4_filter is not None:
        F6L4_filter = get_list_for_query(F6L4_filter)
        query = f"{query} AND CONCAT(cardfirstsix,'-',cardlastfour) in {F6L4_filter} "

    dataframe_result = get_df_from_athena_query(query, conn)
    return dataframe_result


def get_first_crawled_txn_timestamp(site_crawled_txns):
    """
    Return the earliest crawled transaction in datalake
    """
    return pd.to_datetime(site_crawled_txns['transactionTimestamp']).min()


def get_last_crawled_txn_timestamp(site_crawled_txns):
    """
    Return the last crawled transaction in datalake
    """
    return pd.to_datetime(site_crawled_txns['transactionTimestamp']).max()


def get_crawled_txns_for_site_cards(site_crawled_txns, card_ids):
    """
    Filter a crawled transaction dataframe to specific card_ids
    """
    card_ids = get_list_if_string(card_ids)
    dataframe_result = site_crawled_txns.query("cardId in @card_ids")
    return dataframe_result.to_dict('records')


def get_user_site_enrollment_timestamp(user_uuid, site_uuid):
    """
    Get the timestamp for user's site enrollment (utc aware)
    """
    user_site_metadata_response = baseline_service.get_user_site_metadata_by_user_sites(user_uuid, [site_uuid])
    if user_site_metadata_response['upsideCode'] != USER_SITE_METADATA_FOUND_RESPONSE_CODE:
        return None
    return user_site_metadata_response['payload'][0]['enrollTimestamp']


def get_month_difference_in_two_yearmonths(start_yearmonth, end_yearmonth):
    """
    Helper function to get the number of month's difference between two
    year months.  e.g. get_month_difference_in_two_yearmonths(201901, 201812) == 1
    """
    start_year = np.floor(start_yearmonth / 100)
    end_year = np.floor(end_yearmonth / 100)
    start_month = np.mod(start_yearmonth, 100)
    end_month = np.mod(end_yearmonth, 100)
    diff = (start_year - end_year) * 12 + (start_month - end_month)
    return diff


def get_yearmonth_after_adding_months(start_period, diff, grid=Grid):
    """
    Helper function to add or subtract months from a yearmonth
    e.g. get_yearmonth_after_adding_months(201812,1) = 201901
    """
    PeriodIndex = grid.index(start_period)
    Location = PeriodIndex + diff
    return grid[Location]


def get_relative_month(crawled_txn_timestamp, enrollment_timestamp):
    """
    Get the number of calendar months before or after an enrollment timestamp
    for crawled_txn_timestamp
    """

    crawled_txn_timestamp = pd.to_datetime(crawled_txn_timestamp).tz_localize('UTC')
    enrollment_timestamp = pd.to_datetime(enrollment_timestamp)

    crawled_txn_yearmonth = crawled_txn_timestamp.year * 100 + crawled_txn_timestamp.month
    enrollment_yearmonth = enrollment_timestamp.year * 100 + enrollment_timestamp.month

    month_diff = get_month_difference_in_two_yearmonths(crawled_txn_yearmonth, enrollment_yearmonth)
    return int(month_diff)


def get_user_relative_month_range(first_crawled_txn_timestamp,
                                  last_crawled_txn_timestamp,
                                  user_site_enrollment_timestamp):
    """

    """

    relative_month_first_crawled_tx = max(get_relative_month(first_crawled_txn_timestamp,
                                                             user_site_enrollment_timestamp), -12)

    relative_month_last_crawled_tx = get_relative_month(last_crawled_txn_timestamp,
                                                        user_site_enrollment_timestamp)

    return range(relative_month_first_crawled_tx, relative_month_last_crawled_tx + 1)


def get_year_month(timestamp):
    """
    Convert a timestamp to a yearmonth
    """
    timestamp = pd.to_datetime(timestamp)
    return timestamp.year * 100 + timestamp.month


def get_initialized_pre_post_map(relative_month_range):
    pre_post_map = dict((x, {'sales': 0,
                             'visits': 0}) for x in relative_month_range)
    return pre_post_map


def get_pre_post_data(crawled_txns, user_site_enrollment_timestamp, relative_month_range, card_count):
    pre_post_map = get_initialized_pre_post_map(relative_month_range)

    for crawled_txn in crawled_txns:
        relative_month = get_relative_month(crawled_txn['transactionTimestamp'], user_site_enrollment_timestamp)
        if relative_month in pre_post_map:
            pre_post_map[relative_month]['sales'] += crawled_txn['totalAmount']
            pre_post_map[relative_month]['visits'] += 1

    for relative_month, values in pre_post_map.items():
        if card_count > 0:
            pre_post_map[relative_month]['sales'] = values['sales'] / card_count
            pre_post_map[relative_month]['visits'] = values['visits'] / card_count
    return pre_post_map


def get_stats_by_month(site_uuid,
                       user_site_enrollment_timestamp,
                       user_card_to_shadows,
                       site_crawled_txns,
                       relative_month_range):
    user_stats_by_month = get_initialized_pre_post_map(relative_month_range)

    shadow_stats_by_month = get_initialized_pre_post_map(relative_month_range)

    for card_id, shadows in user_card_to_shadows.items():
        user_crawled_txns = get_crawled_txns_for_site_cards(site_crawled_txns, get_list_if_string(card_id))
        user_data = get_pre_post_data(user_crawled_txns, user_site_enrollment_timestamp, relative_month_range, 1)
        for month, values in user_data.items():
            user_stats_by_month[month]['sales'] += values['sales']
            user_stats_by_month[month]['visits'] += values['visits']

        shadow_crawled_txns = get_crawled_txns_for_site_cards(site_crawled_txns, get_list_if_string(shadows))
        shadow_data = get_pre_post_data(shadow_crawled_txns, user_site_enrollment_timestamp, relative_month_range,
                                        len(shadows))
        for month, values in shadow_data.items():
            shadow_stats_by_month[month]['sales'] += values['sales']
            shadow_stats_by_month[month]['visits'] += values['visits']

    return user_stats_by_month, shadow_stats_by_month


def get_no_shadows_flag(user_card_to_shadows_map):
    return [shadows for shadows in user_card_to_shadows_map.values() if shadows != []] == []


def generate_user_shadow_panel(site_uuid,
                               user_uuid,
                               source_terminal,
                               site_crawled_txns=None,
                               first_crawled_txn_timestamp=None,
                               last_crawled_txn_timestamp=None):
    # 1 Get site crawled transaction data
    if site_crawled_txns is None:
        site_crawled_txns = get_crawled_txns(site_uuid, source_terminal)

    if first_crawled_txn_timestamp is None:
        first_crawled_txn_timestamp = get_first_crawled_txn_timestamp(site_crawled_txns)

    if last_crawled_txn_timestamp is None:
        last_crawled_txn_timestamp = get_last_crawled_txn_timestamp(site_crawled_txns)

    # 2a. Get the user's cardIds
    user_card_ids = get_user_card_ids(user_uuid)

    # 2b. Get the shadows associated with a user card at a given site
    user_card_to_shadows = get_user_card_to_shadows_map(site_uuid, user_uuid, user_card_ids)

    # Chck there are any shadows
    if get_no_shadows_flag(user_card_to_shadows) == True:
        return None

    # 3. Get user site enrollment timestamp
    user_site_enrollment_timestamp = get_user_site_enrollment_timestamp(user_uuid, site_uuid)

    user_relative_month_range = get_user_relative_month_range(first_crawled_txn_timestamp,
                                                              last_crawled_txn_timestamp,
                                                              user_site_enrollment_timestamp)

    # 4. Get crawled transactions for cardsIds and create map of month:totalAmountSpent
    # Average the amount spent for each card in a shadow set
    # If a user has multiple cards, sum their spend for each card and sum the average monthly spend for each shadow set
    user_stats_by_month, shadow_stats_by_month = get_stats_by_month(site_uuid,
                                                                    user_site_enrollment_timestamp,
                                                                    user_card_to_shadows,
                                                                    site_crawled_txns,
                                                                    user_relative_month_range)

    dataframe_user_stats_by_month = pd.DataFrame.from_dict(user_stats_by_month, orient='index') \
        .reset_index() \
        .rename(columns={'sales': 'userTotalAmount',
                         'visits': 'userTotalVisits',
                         'index': 'relativeMonth'})

    dataframe_shadow_stats_by_month = pd.DataFrame.from_dict(shadow_stats_by_month, orient='index') \
        .reset_index() \
        .rename(columns={'sales': 'shadowTotalAmount',
                         'visits': 'shadowTotalVisits',
                         'index': 'relativeMonth'})

    user_shadow_panel = pd.merge(dataframe_user_stats_by_month,
                                 dataframe_shadow_stats_by_month,
                                 how='left',
                                 on=['relativeMonth'])

    user_shadow_panel['siteUuid'] = site_uuid
    user_shadow_panel['userUuid'] = user_uuid
    user_shadow_panel['sourceTerminal'] = source_terminal
    user_shadow_panel['siteEnrollmentTimestamp'] = user_site_enrollment_timestamp

    user_shadow_panel['yearMonth'] = user_shadow_panel.apply(
        lambda x: get_yearmonth_after_adding_months(start_period=get_year_month(user_site_enrollment_timestamp),
                                                    diff=x['relativeMonth']), axis=1)

    user_shadow_panel['relativeMonth'] = np.where(user_shadow_panel['relativeMonth'] >= 0,
                                                  user_shadow_panel['relativeMonth'] + 1.0,
                                                  user_shadow_panel['relativeMonth'])

    return user_shadow_panel


def generate_all_site_user_shadow_panels(site_uuid,
                                         source_terminal,
                                         incremental=None,
                                         crawled_data_end_date=None):
    # 1 Get all user_uuid from incremental
    all_site_users = get_all_user_uuid_from_incremental(site_uuid, source_terminal, incremental)

    # 2 Get site crawled transaction data
    site_crawled_txns = get_crawled_txns(site_uuid, source_terminal)
    first_crawled_txn_timestamp = get_first_crawled_txn_timestamp(site_crawled_txns)
    last_crawled_txn_timestamp = get_last_crawled_txn_timestamp(site_crawled_txns)

    # Curtail transaction data if needed
    if crawled_data_end_date is not None:
        site_crawled_txns = site_crawled_txns[pd.to_datetime(site_crawled_txns.transactionTimestamp) <
                                              crawled_data_end_date]

    panel_dict = {}
    for user_uuid in tqdm(all_site_users):
        try:
            panel_dict[f'{site_uuid}--{user_uuid}--{source_terminal}'] = generate_user_shadow_panel(site_uuid,
                                                                                                    user_uuid,
                                                                                                    source_terminal,
                                                                                                    site_crawled_txns,
                                                                                                    first_crawled_txn_timestamp,
                                                                                                    last_crawled_txn_timestamp)
        except Exception:
            print("Skipping ", user_uuid)

    return panel_dict


def get_user_incremental_txns(site_uuid,
                              user_uuid,
                              source_terminal,
                              incremental=None):
    """
    Get incremental txns for a user either from incremental dataframe
    or from query.
    """

    if incremental is None:
        user_incremental = get_incremental(site_uuid, source_terminal, user_uuid_filter=user_uuid)
    else:
        user_incremental = incremental.query("useruuid == @user_uuid")

    return user_incremental


# def generate_all_user_incremental_data(site_uuid, source_terminal, incremental = None):

#     if incremental is None:
#         incremental = get_incremental(site_uuid, source_terminal)

#     incremental_dict = {}
#     for user_uuid in incremental.useruuid.unique():
#         incremental_dict[user_uuid] = get_user_incremental_txns(site_uuid,
#                                                                 user_uuid,
#                                                                 source_terminal,
#                                                                 incremental)
#     return incremental_dict


def generate_pre_post_from_user_panels(panel_dict, modify_month_minus_1=True):
    df_list = []
    for key, user_panel in panel_dict.items():
        if user_panel is not None:
            if get_outlier_flag(user_panel) == False:
                df_list += [user_panel]

    pre_post_data = pd.concat(df_list, sort=False) \
        .reset_index(drop=True) \
        .groupby(['relativeMonth'], as_index=False) \
        .agg({'userTotalAmount': 'mean',
              'shadowTotalAmount': 'mean',
              'userTotalVisits': 'mean',
              'shadowTotalVisits': 'mean',
              'userUuid': lambda x: x.nunique()})

    if modify_month_minus_1 == True:
        pre_post_data = get_modified_month_minus_1(pre_post_data)

    return pre_post_data


def generate_site_pre_post_data(site_uuid, source_terminal, modify_month_minus_1=True, incremental=None):
    site_uuid = get_list_if_string(site_uuid)

    panel_dict = {}
    for site in site_uuid:
        if incremental is not None:
            site_incremental = incremental.query("siteuuid == @site")
        else:
            site_incremental = None

        panel_dict.update(generate_all_site_user_shadow_panels(site, source_terminal, site_incremental))

    pre_post_data = generate_pre_post_from_user_panels(panel_dict, modify_month_minus_1)

    return pre_post_data


def get_modified_month_minus_1(pre_post_panel):
    pre_post_panel['shadowTotalAmount'] = np.where(pre_post_panel['relativeMonth'] == -1,
                                                   pre_post_panel['userTotalAmount'],
                                                   pre_post_panel['shadowTotalAmount'])

    pre_post_panel['shadowTotalVisits'] = np.where(pre_post_panel['relativeMonth'] == -1,
                                                   pre_post_panel['userTotalVisits'],
                                                   pre_post_panel['shadowTotalVisits'])
    return pre_post_panel


def generate_site_calendar_time_data(panel_dict, site_uuid, source_terminal, modify_month_minus_1=True):
    df_list = []
    for key, user_panel in panel_dict.items():
        if user_panel is not None:
            if get_outlier_flag(user_panel) == False:
                user_panel = get_modified_month_minus_1(user_panel)
                df_list += [user_panel]

    site_calendar_time_data = pd.concat(df_list, sort=False) \
        .reset_index(drop=True) \
        .groupby(['yearMonth', 'siteUuid'], as_index=False) \
        .agg({'userTotalAmount': 'mean',
              'shadowTotalAmount': 'mean',
              'userTotalVisits': 'mean',
              'shadowTotalVisits': 'mean',
              'userUuid': lambda x: x.nunique()})
    return site_calendar_time_data


def serialize_panels(panel_dict, site_uuid, source_terminal, output_dir='.'):
    for user_uuid, user_panel in panel_dict.items():
        path = f'{output_dir}/pSiteUuid={site_uuid}/pUserUuid={user_uuid}/pSourceTerminal={source_terminal}/'
        os.makedirs(path, exist_ok=True)
        user_panel.to_csv(f'{path}{user_uuid}-{source_terminal}-pre_post_panel.csv', index=False)

    return


def serialize_user_incremental(df_dict, site_uuid, source_terminal):
    for user_uuid, user_inc in df_dict.items():
        path = f'./pSiteUuid={site_uuid}/pUserUuid={user_uuid}/pSourceTerminal={source_terminal}'
        os.makedirs(path, exist_ok=True)
        user_inc.to_csv(f'{path}/{user_uuid}-{source_terminal}-incremental.csv', index=False)

    return


def serialize_site_pre_post_data(site_pre_post_data, site_uuid, source_terminal, output_dir='.'):
    site_pre_post_data.to_csv(f'{output_dir}/pSiteUuid={site_uuid}/{site_uuid}-{source_terminal}-pre_post_data.csv',
                              index=False)
    return


def get_outlier_flag(user_panel, max_avg_pre_total_amount_diff=200):
    user_pre_avg_total_amount = user_panel.query("relativeMonth < 0") \
        .userTotalAmount \
        .mean()

    shadow_pre_avg_total_amount = user_panel.query("relativeMonth < 0") \
        .shadowTotalAmount \
        .mean()

    return abs(user_pre_avg_total_amount - shadow_pre_avg_total_amount) > max_avg_pre_total_amount_diff


def get_selected_users_pre_post_panels(source_terminal, site_and_user_filepath, output_dir='.'):
    os.makedirs(output_dir, exist_ok=True)
    site_user_uuid_df = pd.read_csv(site_and_user_filepath)

    for site_uuid in site_user_uuid_df.siteuuid.unique():
        site_crawled_txns = get_crawled_txns(site_uuid, source_terminal)

        for user_uuid in site_user_uuid_df.query("siteuuid == @site_uuid").useruuid.unique():
            user_panel = generate_user_shadow_panel(site_uuid=site_uuid,
                                                    user_uuid=user_uuid,
                                                    source_terminal=source_terminal,
                                                    site_crawled_txns=site_crawled_txns,
                                                    first_crawled_txn_timestamp=None,
                                                    last_crawled_txn_timestamp=None)
            if user_panel is not None:
                user_panel.to_csv(f'{output_dir}/{site_uuid}--{user_uuid}-{source_terminal}-pre_post_panel.csv',
                                  index=False)
    return


if __name__ == '__main__':
    #     generate_pre_post_graph()
    #
    pass