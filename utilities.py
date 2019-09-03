from upside_core.services import create_private_service_with_retries
from merchant_service.client import MerchantService
from tqdm import *
from pandas.io.json import json_normalize

ms = create_private_service_with_retries('prod', MerchantService)

def get_sites_from_merchant_service():

    AllSites = ms.get_sites_and_addresses(visibility="ANY")

    AllMerchants = ms.get_all_merchants()

    MerchantDict = {}
    for merchant in AllMerchants:
        MerchantDict[merchant['uuid']] = merchant


    total = []
    for site in tqdm(AllSites):
        site_dict = {}
        try:
            site_dict['siteUuid'] = site[0]['uuid']
        except Exception:
            pass

        try:
            site_dict['merchantUuid'] = site[0]['merchantUuid']
        except Exception:
            pass

        try:
            site_dict['merchant'] = MerchantDict[site[0]['merchantUuid']]['name']
        except Exception:
            pass

        try:
            site_dict['visibility'] = site[0]['visibility']
        except Exception:
            pass

        try:
            site_dict['territory'] = site[0]['additionalProperties']['territory']
        except Exception:
            pass

        try:
            site_dict['siteIdentifier'] = site[0]['additionalProperties']['siteIdentifier']
        except Exception:
            pass

        try:
            site_dict['opisIndexPrices'] = site[0]['additionalProperties']['opisIndexPrices']
        except Exception:
            pass

        try:
            site_dict['opisRackBrand'] = site[0]['additionalProperties']['opisRackBrand']
        except Exception:
            pass

        try:
            site_dict['opisRackTerminal'] = site[0]['additionalProperties']['opisRackTerminal']
        except Exception:
            pass

        try:
            site_dict['opisRackType'] = site[0]['additionalProperties']['opisRackType']
        except Exception:
            pass

        try:
            site_dict['terminals'] = site[0]['additionalProperties']['terminals']
        except Exception:
            pass


        try:
            site_dict['baselineVersion'] = site[0]['settings']['baselineVersion']
        except Exception:
            pass

        try:
            site_dict['timezone'] = site[0]['timezone']
        except Exception:
            pass

        try:
            site_dict['address1'] = site[1]['address1']
            site_dict['locality'] = site[1]['locality']
            site_dict['region'] = site[1]['region']
            site_dict['postCode'] = site[1]['postCode']
            site_dict['latitude'] = site[1]['latitude']
            site_dict['longitude'] = site[1]['longitude']

        except Exception:
            pass

        try:
            site_dict['corporation'] = site[0]['additionalProperties']['corporation']
        except Exception:
            pass

        total = total + [site_dict]

    return json_normalize(total).set_index('siteUuid', drop=False)