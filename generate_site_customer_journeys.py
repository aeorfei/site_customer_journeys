import json
import boto3
import math
import os
from baseline_service.client import BaselineService
from upside_core.services import create_private_service_with_retries
from pyathena import connect
import pandas as pd
import numpy as np
from tqdm import *

USER_CARDS_FOUND_RESPONSE_CODE = 17210
USER_CARDS_NOT_FOUND_RESPONSE_CODE = 17414
USER_SITE_METADATA_FOUND_RESPONSE_CODE = 17220
BASELINE_BUCKET = 'prod-baselines.upside-services.com'
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

conn = connect(aws_access_key_id=AWS_ACCESS_KEY_ID,
               aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
               s3_staging_dir='s3://aws-athena-query-results-337068080576-us-east-1/',
               region_name='us-east-1')

baseline_service = create_private_service_with_retries('prod', BaselineService)
s3 = boto3.resource('s3')


def get_site_incremental(site_uuid):
    query = f"SELECT * " \
            f" FROM prod_incremental.incremental_reviewed " \
            f" WHERE sourceterminal = 'OUTSIDE' " \
            f"   AND siteuuid = '{site_uuid}' " \
            f" ORDER BY date "
    dataframe_result = pd.read_sql(query, conn)
    return dataframe_result


def get_all_user_uuid_from_incremental(site_uuid, sourceterminal='OUTSIDE'):
    return list(get_site_incremental(site_uuid) \
                .query("sourceterminal == @sourceterminal") \
                .useruuid \
                .unique())


def get_user_card_ids(user_uuid):
    user_cards_response = baseline_service.get_user_cards_by_user(user_uuid)
    if user_cards_response['upsideCode'] != USER_CARDS_FOUND_RESPONSE_CODE:
        return []
    return [c['cardId'] for c in user_cards_response['payload']]


def get_user_card_site_shadows(user_uuid, card_id, site_uuid):
    card_id = card_id.replace(' ', '_')
    path = f'v2/shadows/pUserUuid={user_uuid}/pSiteUuid={site_uuid}/pTransactionType=GAS/{card_id}.json'
    content_object = s3.Object(BASELINE_BUCKET, path)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    return json.loads(file_content)


def get_shadow_card_ids(user_uuid, card_id, site_uuid):
    shadow_objects = get_user_card_site_shadows(user_uuid, card_id, site_uuid)
    return [s['shadowCardId'] for s in shadow_objects['shadows']]


def df_from_athena_query(query, conn):
    cursor = conn.cursor()
    result = cursor.execute(query)
    outdir = './'
    command = 'aws s3 cp s3://aws-athena-query-results-337068080576-us-east-1/' + result.query_id + '.csv  ' + outdir
    os.system(command)
    outdf = pd.read_csv(outdir + result.query_id + '.csv').reset_index(drop=True)
    os.system("rm " + outdir + result.query_id + '.csv')
    return outdf


def get_site_crawled_txns(site_uuid):
    query = f"SELECT siteuuid as siteUuid, " \
            f"  CAST(From_iso8601_timestamp(transactiontimestamp) AS timestamp) as transactionTimestamp, " \
            f"  year(CAST(From_iso8601_timestamp(transactiontimestamp) AS timestamp))*100 + month(CAST(From_iso8601_timestamp(transactiontimestamp) AS timestamp)) as TranYearMonth," \
            f"  cardid as cardId, " \
            f"  cardtype as cardType, " \
            f"  cardlastfour as cardLastFour, " \
            f"  cardFirstSix as cardFirstSix, " \
            f"  total.amount as totalAmount, " \
            f"  tax.amount as taxamount, " \
            f"  transactionuuid as transactionUuid, " \
            f"  sourceterminal as sourceTerminal, " \
            f"  status, " \
            f"  datasource, " \
            f"  pdatasource, " \
            f"  addedattimestamp as addedAtTimeStamp " \
            f" FROM prod_transaction.refined " \
            f" WHERE sourceterminal = 'OUTSIDE' " \
            f" AND status not in ('DECLINED') " \
            f" AND psiteuuid = '{site_uuid}' "
    dataframe_result = df_from_athena_query(query, conn)
    return dataframe_result


def get_first_crawled_txn_timestamp(site_crawled_txns):
    return pd.to_datetime(site_crawled_txns['transactionTimestamp']).min()


def get_last_crawled_txn_timestamp(site_crawled_txns):
    return pd.to_datetime(site_crawled_txns['transactionTimestamp']).max()


def get_list_if_string(object):
    if type(object) == str:
        object = [object]
    return object


def get_crawled_txns_for_site_cards(site_crawled_txns, card_ids):
    card_ids = get_list_if_string(card_ids)
    dataframe_result = site_crawled_txns.query("cardId in @card_ids")
    return dataframe_result.to_dict('records')


def get_user_site_enrollment_timestamp(user_uuid, site_uuid):
    user_site_metadata_response = baseline_service.get_user_site_metadata_by_user_sites(user_uuid, [site_uuid])
    if user_site_metadata_response['upsideCode'] != USER_SITE_METADATA_FOUND_RESPONSE_CODE:
        return None
    return user_site_metadata_response['payload'][0]['enrollTimestamp']


def get_month_difference_in_two_yearmonths(start_yearmonth, end_yearmonth):
    StartYear = np.floor(start_yearmonth / 100)
    EndYear = np.floor(end_yearmonth / 100)
    StartMonth = np.mod(start_yearmonth, 100)
    EndMonth = np.mod(end_yearmonth, 100)
    Diff = (StartYear - EndYear) * 12 + (StartMonth - EndMonth)
    return Diff


def get_relative_month(crawled_txn_timestamp, enrollment_timestamp):
    crawled_txn_timestamp = pd.to_datetime(crawled_txn_timestamp).tz_localize('UTC')
    enrollment_timestamp = pd.to_datetime(enrollment_timestamp)

    crawled_txn_yearmonth = crawled_txn_timestamp.year * 100 + crawled_txn_timestamp.month
    enrollment_yearmonth = enrollment_timestamp.year * 100 + enrollment_timestamp.month

    month_diff = get_month_difference_in_two_yearmonths(crawled_txn_yearmonth, enrollment_yearmonth)
    if month_diff >= 0:
        month_diff += 1
    return int(month_diff)


def get_user_relative_month_range(first_crawled_tx_timestamp,
                                  last_crawled_tx_timestamp,
                                  user_site_enrollment_timestamp):
    relative_month_first_crawled_tx = max(get_relative_month(first_crawled_tx_timestamp,
                                                             user_site_enrollment_timestamp), -12)

    relative_month_last_crawled_tx = get_relative_month(last_crawled_tx_timestamp,
                                                        user_site_enrollment_timestamp)

    return range(relative_month_first_crawled_tx, relative_month_last_crawled_tx + 1)


def get_pre_post_data(crawled_txns, user_site_enrollment_timestamp, relative_month_range, card_count):
    pre_post_map = dict((x, 0) for x in relative_month_range)
    del pre_post_map[0]

    for crawled_txn in crawled_txns:
        relative_month = get_relative_month(crawled_txn['transactionTimestamp'], user_site_enrollment_timestamp)
        if relative_month in pre_post_map:
            pre_post_map[relative_month] = pre_post_map[relative_month] + crawled_txn['totalAmount']
    for relative_month, total_amount in pre_post_map.items():
        if card_count > 0:
            pre_post_map[relative_month] = total_amount / card_count
    return pre_post_map


def get_user_card_to_shadows_map(user_uuid, site_uuid, user_card_ids):
    user_card_to_shadows = {}
    for card_id in user_card_ids:
        user_card_to_shadows[card_id] = []
        try:
            shadow_card_ids = get_shadow_card_ids(user_uuid, card_id, site_uuid)
            #         print(f"\nFound the following shadow cards for cardId '{card_id}': {shadow_card_ids}")
            user_card_to_shadows[card_id] = shadow_card_ids
        except Exception:
            pass
    return user_card_to_shadows


def get_spend_by_month(site_uuid,
                       user_site_enrollment_timestamp,
                       user_card_to_shadows,
                       site_crawled_txns,
                       relative_month_range):
    user_spend_by_month = dict((x, 0) for x in relative_month_range)
    del user_spend_by_month[0]

    shadow_spend_by_month = dict((x, 0) for x in relative_month_range)
    del shadow_spend_by_month[0]

    for card_id, shadows in user_card_to_shadows.items():
        user_crawled_txns = get_crawled_txns_for_site_cards(site_crawled_txns, get_list_if_string(card_id))
        user_data = get_pre_post_data(user_crawled_txns, user_site_enrollment_timestamp, relative_month_range, 1)
        for month, amount in user_data.items():
            user_spend_by_month[month] = user_spend_by_month[month] + amount
        shadow_crawled_txns = get_crawled_txns_for_site_cards(site_crawled_txns, get_list_if_string(shadows))
        shadow_data = get_pre_post_data(shadow_crawled_txns, user_site_enrollment_timestamp, relative_month_range,
                                        len(shadows))
        for month, amount in shadow_data.items():
            shadow_spend_by_month[month] = shadow_spend_by_month[month] + amount
    return user_spend_by_month, shadow_spend_by_month


def generate_user_shadow_panel(site_uuid,
                               user_uuid,
                               site_crawled_txns=None,
                               first_crawled_tx_timestamp=None,
                               last_crawled_tx_timestamp=None):
    # 1 Get site crawled transaction data
    if site_crawled_txns is None:
        site_crawled_txns = get_site_crawled_txns(site_uuid)

    if first_crawled_tx_timestamp is None:
        first_crawled_tx_timestamp = get_first_crawled_txn_timestamp(site_crawled_txns)

    if last_crawled_tx_timestamp is None:
        last_crawled_tx_timestamp = get_last_crawled_txn_timestamp(site_crawled_txns)

    # 2a. Get the user's cardIds
    user_card_ids = get_user_card_ids(user_uuid)
    #     print(f"\nFound the following cardIds for user '{user_uuid}': {user_card_ids}")

    # 2b. Get the shadows associated with a user card at a given site
    user_card_to_shadows = get_user_card_to_shadows_map(user_uuid, site_uuid, user_card_ids)

    # 3. Get user site enrollment timestamp
    user_site_enrollment_timestamp = get_user_site_enrollment_timestamp(user_uuid, site_uuid)

    user_relative_month_range = get_user_relative_month_range(first_crawled_tx_timestamp,
                                                              last_crawled_tx_timestamp,
                                                              user_site_enrollment_timestamp)

    # 4. Get crawled transactions for cardsIds and create map of month:totalAmountSpent
    # Average the amount spent for each card in a shadow set
    # If a user has multiple cards, sum their spend for each card and sum the average monthly spend for each shadow set
    user_spend_by_month, shadow_spend_by_month = get_spend_by_month(site_uuid,
                                                                    user_site_enrollment_timestamp,
                                                                    user_card_to_shadows,
                                                                    site_crawled_txns,
                                                                    user_relative_month_range)

    dataframe_user_spend_by_month = pd.DataFrame.from_dict(user_spend_by_month, orient='index') \
        .reset_index() \
        .rename(columns={0: 'UserTotalAmount',
                         'index': 'RelativeMonth'})

    dataframe_shadow_spend_by_month = pd.DataFrame.from_dict(shadow_spend_by_month, orient='index') \
        .reset_index() \
        .rename(columns={0: 'ShadowTotalAmount',
                         'index': 'RelativeMonth'})

    user_shadow_panel = pd.merge(dataframe_user_spend_by_month,
                                 dataframe_shadow_spend_by_month,
                                 how='left',
                                 on='RelativeMonth')

    user_shadow_panel['siteuuid'] = site_uuid
    user_shadow_panel['useruuid'] = user_uuid

    return user_shadow_panel


def generate_all_site_user_shadow_panels(site_uuid, crawled_data_end_timestamp=None):
    # 1 Get all user_uuid from incremental
    all_site_users = get_all_user_uuid_from_incremental(site_uuid)

    # 2 Get site crawled transaction data
    site_crawled_txns = get_site_crawled_txns(site_uuid)
    first_crawled_tx_timestamp = get_first_crawled_txn_timestamp(site_crawled_txns)
    last_crawled_tx_timestamp = get_last_crawled_txn_timestamp(site_crawled_txns)

    # Curtail transaction data if needed
    if crawled_data_end_timestamp is not None:
        site_crawled_txns = site_crawled_txns[pd.to_datetime(site_crawled_txns.transactionTimestamp) <
                                              crawled_data_end_timestamp]

    panel_dict = {}
    for user_uuid in tqdm(all_site_users):
        panel_dict[user_uuid] = generate_user_shadow_panel(site_uuid,
                                                           user_uuid,
                                                           site_crawled_txns,
                                                           first_crawled_tx_timestamp,
                                                           last_crawled_tx_timestamp)
    #         except:
    #             print("Skipping ", user_uuid)

    return panel_dict


def serialize_panels(panel_dict, site_uuid, outdir="./"):
    for user_uuid, user_panel in panel_dict.items():
        path = outdir + 'pSiteUuid=' + site_uuid + '/'
        os.makedirs(path, exist_ok=True)
        user_panel.to_csv(path + user_uuid + '.csv', index=False)
    return


def generate_site_pre_post_data(panel_dict, site_uuid, outdir="./"):
    df_list = []
    for user_uuid, user_panel in panel.items():
        df_list += [user_panel]

    site_pre_post_data = pd.concat(df_list, sort=False) \
        .reset_index(drop=True) \
        .groupby(['RelativeMonth'], as_index=False) \
        .agg({'UserTotalAmount': 'mean',
              'ShadowTotalAmount': 'mean',
              'useruuid': lambda x: x.nunique()}) \
        .assign(ShadowTotalAmount=lambda x: np.where(x['RelativeMonth'] == -1,
                                                     x['UserTotalAmount'],
                                                     x['ShadowTotalAmount']))

    site_pre_post_data.to_csv(outdir + site_uuid + '_pre_post_data.csv', index=False)

    return site_pre_post_data


if __name__ == '__main__':
    #     generate_pre_post_graph()
    pass