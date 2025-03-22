import re

def clean_column_name(concept_name: str) -> str:
    """
    Cleans the concept name to be used as a column name in a DataFrame.
    It drops the 'us-gaap:' prefix and replaces the ':' with an underscore.

    Args:
        concept_name: The concept name to be cleaned.
    returns:
        The cleaned concept name.
    """
    cleaned_name = re.sub(f"us-gaap_", "", concept_name) # simply remove the 'us-gaap:' prefix
    cleaned_name = re.sub(r"([A-Z])", r"_\1", cleaned_name).lower().strip("_")# replace ':' with '_'
    cleaned name = cleaned_name.replace("and", "_").replace("netof", "_").replace("net", "_").replace("including", "_").replace("with", "_") #replace common words.

    return cleaned_name


def map_column_name(concept_name: str) -> str:
    """
    It first looks up for the mapping and if it exists, it returns the mapped concept name.
    otherwise it will call the function clean_column_name to clean the concept name.

    Args:
        concept_name: The concept name to be mapped.
    returns:
        The mapped concept name.
    """
    if concept_name in name_mapping:
        return name_mapping[concept_name]
    else:
        return clean_column_name(concept_name)


name_mapping = {
    "us-gaap_AccumulatedOtherComprehensiveIncomeLossNetOfTax": "accumulated_oci_loss_net_of_tax",
    "us-gaap_PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetAfterAccumulatedDepreciationAndAmortization": "net_ppe_finance_lease_assets",
    "us-gaap_AccumulatedOtherComprehensiveIncomeLossAvailableForSaleSecuritiesAdjustmentNetOfTax": "accumulated_oci_available_sale_securities_net_of_tax",
    "us-gaap_AccumulatedOtherComprehensiveIncomeLossDefinedBenefitPensionAndOtherPostretirementPlansNetOfTax": "accumulated_oci_pension_postretirement_net_of_tax",
    "us-gaap_AccumulatedOtherComprehensiveIncomeLossForeignCurrencyTranslationAdjustmentNetOfTax": "accumulated_oci_foreign_currency_net_of_tax",
    "v_VECoveredLossProtectionfromthePlanRelatingtoLiabilitieswheretheClaimRelatestoInterregionalMultilateralInterchangeFeeRates": "covered_loss_protection_interchange_fees",
    "us-gaap_StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest": "stockholders_equity_noncontrolling_interest"
}