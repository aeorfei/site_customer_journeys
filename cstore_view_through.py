
from generate_site_customer_journeys import get_crawledtxuuid_by_usertxuuid
from generate_site_customer_journeys import get_user_card_ids
from generate_site_customer_journeys import get_list_if_string
from generate_site_customer_journeys import get_incremental
from generate_site_customer_journeys import get_crawled_txns
from datetime import timedelta
import pandas as pd
from tqdm import *

def get_user_unique_f6l4_cards(user_uuid):
    user_card_ids = get_user_card_ids(user_uuid)
    unique_fsl4 = list(set([x.split('-')[1] + '-' + x.split('-')[2] for x in user_card_ids]))
    return unique_fsl4

# 1d0df9c3-452c-4d9c-b23a-9c81f8387ce5--f0cdf94e-a192-4890-b3e2-2af1c0377365--OUTSIDE

def get_usertxuuid_viewthrough(site_uuid,
                                   user_uuid,
                                   usertx_uuid,
                                   site_crawledtx_uuids,
                                   viewthrough_source_terminal,
                                   timewindow_minutes,
                                   site_crawled_txns):

    viewthrough_source_terminal = get_list_if_string(viewthrough_source_terminal)
    crawledtx_uuid = get_crawledtxuuid_by_usertxuuid(usertx_uuid, site_uuid)
    usertx_uuid_timestamp = site_crawled_txns.query("transactionUuid == @crawledtx_uuid")['transactionTimestamp'].reset_index(drop=True)[0]

    start_window = usertx_uuid_timestamp - timedelta(minutes=timewindow_minutes)
    end_window = usertx_uuid_timestamp + timedelta(minutes=timewindow_minutes)

    user_f6l4 = get_user_unique_f6l4_cards(user_uuid)

    viewthrough_result = site_crawled_txns.query("F6L4 in @user_f6l4 \
                                                  and sourceTerminal in @viewthrough_source_terminal \
                                                  and transactionTimestamp >= @start_window \
                                                  and transactionTimestamp <= @end_window \
                                                  and transactionUuid not in @site_crawledtx_uuids")

    return viewthrough_result

def get_viewthrough_for_site(site_uuid):
    site_incremental = get_incremental(site_uuid,
                                          source_terminal_filter='OUTSIDE',
                                          user_uuid_filter=None,
                                          add_crawledtx_uuid=True)\
                         .reset_index(drop=True)

    site_crawledtx_uuids = list(site_incremental.crawledTxUuid.unique())

    site_crawled_txns = get_crawled_txns(site_uuid=site_uuid) \
                        .assign(transactionTimestamp=lambda x: pd.to_datetime(x['transactionTimestamp']))

    viewthrough_dict = {}
    for index, row in tqdm(site_incremental.iterrows()):
        viewthrough_dict[row['id']] = get_usertxuuid_viewthrough(site_uuid=row['siteuuid'],
                                                            user_uuid=row['useruuid'],
                                                            usertx_uuid=row['id'],
                                                            site_crawledtx_uuids=site_crawledtx_uuids,
                                                            viewthrough_source_terminal='INSIDE',
                                                            timewindow_minutes=30,
                                                            site_crawled_txns=site_crawled_txns)

        # try:
        #     usertxuuid_viewthrough = usertxuuid_viewthrough[~usertxuuid_viewthrough.transactionUuid.isin(viewthrough_df.transactionUuid)].copy()
        # except:
        #     pass

        # viewthrough_df = pd.concat([viewthrough_df, usertxuuid_viewthrough]).reset_index(drop=True)

    return viewthrough_dict
















