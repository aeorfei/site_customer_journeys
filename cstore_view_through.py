
from generate_site_customer_journeys import get_crawledtxuuid_by_usertxuuid
from generate_site_customer_journeys import get_user_card_ids
from generate_site_customer_journeys import get_list_if_string
from generate_site_customer_journeys import get_incremental
from generate_site_customer_journeys import get_crawled_txns
from datetime import timedelta
import pandas as pd
from tqdm import *
import os
from utilities import get_sites_from_merchant_service

ASSUMED_INSIDE_MARGIN = .30

def get_user_unique_f6l4_cards(user_uuid):
    user_card_ids = get_user_card_ids(user_uuid)
    unique_fsl4 = list(set([x.split('-')[1] + '-' + x.split('-')[2] for x in user_card_ids]))
    return unique_fsl4

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
                         .query("crawledTxUuid == crawledTxUuid")\
                         .query("totalincrementalrevenue > 0")\
                         .sort_values('date')\
                         .reset_index(drop=True)

    site_crawledtx_uuids = list(site_incremental.crawledTxUuid.unique())

    site_crawled_txns = get_crawled_txns(site_uuid=site_uuid) \
                        .assign(transactionTimestamp=lambda x: pd.to_datetime(x['transactionTimestamp']))

    viewthrough_df = pd.DataFrame()
    for index, row in tqdm(site_incremental.iterrows()):
        try:
            usertxuuid_viewthrough = get_usertxuuid_viewthrough(site_uuid=row['siteuuid'],
                                                                user_uuid=row['useruuid'],
                                                                usertx_uuid=row['id'],
                                                                site_crawledtx_uuids=site_crawledtx_uuids,
                                                                viewthrough_source_terminal='INSIDE',
                                                                timewindow_minutes=30,
                                                                site_crawled_txns=site_crawled_txns)
            usertxuuid_viewthrough['userTxUuid'] = row['id']
            usertxuuid_viewthrough['userUuid'] = row['useruuid']

            try:
                usertxuuid_viewthrough = usertxuuid_viewthrough[~usertxuuid_viewthrough.transactionUuid.isin(viewthrough_df.transactionUuid)].copy()
            except:
                pass

            viewthrough_df = pd.concat([viewthrough_df, usertxuuid_viewthrough]).reset_index(drop=True)
        except Exception:
            print('Skipping ',row['id'])

    return {'site_uuid' : site_uuid,
            'viewthrough_df' : viewthrough_df,
            'incremental_tx_scanned' : site_incremental}


def generate_site_viewthrough_stats(viewthrough_dict, ProdMerchantService=None, outdir=None):
    site_uuid = viewthrough_dict['site_uuid']
    out_dict = {}
    out_dict['siteUuid'] = site_uuid
    out_dict['AssumedInsideMargin'] = ASSUMED_INSIDE_MARGIN
    out_dict['BasketSize'] = viewthrough_dict['viewthrough_df'].totalAmount.mean()
    out_dict['IncInsideProfit'] = viewthrough_dict['viewthrough_df'].totalAmount.sum() * out_dict['AssumedInsideMargin']
    out_dict['IncOutsideProfit'] = viewthrough_dict['incremental_tx_scanned'].totalincrementalprofit.sum()
    out_dict['StartDate'] = viewthrough_dict['incremental_tx_scanned'].date.min()
    out_dict['EndDate'] = viewthrough_dict['incremental_tx_scanned'].date.max()
    out_dict['IncInsideTx'] = viewthrough_dict['viewthrough_df'].transactionUuid.nunique()
    out_dict['IncInsideVisit'] = viewthrough_dict['viewthrough_df'].userTxUuid.nunique()
    out_dict['IncOutsideTx'] = viewthrough_dict['incremental_tx_scanned'].id.nunique()
    out_dict['PercentInsideTx'] = out_dict['IncInsideTx'] / out_dict['IncOutsideTx']
    out_dict['PercentInsideVisit'] = out_dict['IncInsideVisit'] / out_dict['IncOutsideTx']
    out_dict['PercentInsideProfit'] = out_dict['IncInsideProfit'] / out_dict['IncOutsideProfit']

    out_df = pd.DataFrame.from_dict(out_dict, orient='index').transpose()
    if ProdMerchantService is None:
        ProdMerchantService = get_sites_from_merchant_service()

    out_df = pd.merge(out_df,
                      ProdMerchantService.reset_index(drop=True),
                      how='left',
                      on='siteUuid')

    if outdir is not None:
        os.makedirs(outdir, exist_ok=True)
        out_df.to_csv(f'{outdir}/{site_uuid}--viewthrough-statistics.csv', index=False)

    return out_df

def generate_multi_site_viewthrough_analysis(site_uuid, outdir):
    site_uuid = get_list_if_string(site_uuid)
    os.makedirs(outdir, exist_ok=True)
    ProdMerchantService = get_sites_from_merchant_service()

    master_df = pd.DataFrame()
    for site in site_uuid:
        os.makedirs(f'{outdir}/pSiteUuid={site}', exist_ok=True)
        site_result = get_viewthrough_for_site(site)
        site_result['viewthrough_df'].to_csv(f'{outdir}/pSiteUuid={site}/{site}--viewthrough-transactions.csv', index=False)
        site_stats = generate_site_viewthrough_stats(site_result, ProdMerchantService, outdir = f'{outdir}/pSiteUuid={site}')
        master_df = pd.concat([master_df, site_stats])

    master_df.to_csv(f'{outdir}/summary--viewthrough-statistics.csv', index=False)
    return master_df
























