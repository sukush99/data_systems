import re
import pandas as pd
from azure.storage.blob import BlobServiceClient
from config import config
from loguru import logger

NAME_MAPPING = {
    "us-gaap_AccumulatedOtherComprehensiveIncomeLossNetOfTax": "accumulated_oci_loss_net_of_tax",
    "us-gaap_PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetAfterAccumulatedDepreciationAndAmortization": "net_ppe_finance_lease_assets",
    "us-gaap_AccumulatedOtherComprehensiveIncomeLossAvailableForSaleSecuritiesAdjustmentNetOfTax": "accumulated_oci_available_sale_securities_net_of_tax",
    "us-gaap_AccumulatedOtherComprehensiveIncomeLossDefinedBenefitPensionAndOtherPostretirementPlansNetOfTax": "accumulated_oci_pension_postretirement_net_of_tax",
    "us-gaap_AccumulatedOtherComprehensiveIncomeLossForeignCurrencyTranslationAdjustmentNetOfTax": "accumulated_oci_foreign_currency_net_of_tax",
    "v_VECoveredLossProtectionfromthePlanRelatingtoLiabilitieswheretheClaimRelatestoInterregionalMultilateralInterchangeFeeRates": "covered_loss_protection_interchange_fees",
    "us-gaap_StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest": "stockholders_equity_noncontrolling_interest",
    "abnb_ChangeInFundsPayableAndAmountsPayableToCustomers": "change_funds_payable_customers",
    "abnb_IncreaseDecreaseInOperatingLeaseLiabilities": "change_operating_lease_liabilities",
    "abnb_IncreaseDecreaseInOperatingLeaseRightOfUseAssets": "change_operating_lease_right_use_assets",
    "abnb_InvestmentImpairmentCharges": "investment_impairment_charges",
    "abnb_PaymentForCappedCallsRelatedToConvertibleSeniorNotes": "payment_capped_calls_convertible_notes",
    "abnb_ProceedsFromIssuanceOfLongTermDebtAndWarrantsNetOfIssuanceCosts": "proceeds_longterm_debt_warrants",
    "crm_BusinessCombinationConsiderationTransferredFairValueOfEquityAwardsAssumed": "business_combination_equity_awards",
    "crm_DeferredIncomeTaxExpenseBenefitFromIntraEntityTransferOfIntangibleProperty": "deferred_tax_intra_entity_intangible",
    "crm_FairValueAdjustmentLossOnRemainingPerformanceObligationsOfAgreements": "fair_value_adjustment_performance_obligations",
    "crm_IncreaseDecreaseInCapitalizedContractCosts": "change_capitalized_contract_costs",
    "crm_IncreaseDecreaseInOperatingLeaseLiabilities": "change_operating_lease_liabilities",
    "crm_NoncashOrPartNoncashAcquisitionNoncashFinancialOrEquityInstrumentConsideration": "noncash_acquisition_consideration",
    "crm_RepaymentsOfProceedsFromConvertibleDebtNetOfCappedCalls": "repayments_convertible_debt_capped_calls",
    "v_AmortizationOfClientIncentives": "amortization_client_incentives",
    "v_AmortizationOfVolumeAndSupportIncentives": "amortization_volume_support_incentives",
    "v_IncomeLossFromEquityMethodInvestmentsAndFairMarketValueAlternativeInvestments": "equity_investments_fair_value_income",
    "v_IncreaseDecreaseInAccruedLitigation": "change_accrued_litigation",
    "v_IncreaseDecreaseInClientIncentives": "change_client_incentives",
    "v_IncreaseDecreaseInSettlementPayable": "change_settlement_payable",
    "v_IncreaseDecreaseInSettlementReceivable": "change_settlement_receivable",
    "v_IncreaseDecreaseInVolumeAndSupportIncentives": "change_volume_support_incentives",
    "v_NoncashContributionExpenseSupplemental": "noncash_contribution_expense",
    "v_ProceedsFromSettledSharebasedCompensationforTaxes": "proceeds_sharebased_tax_compensation",
    "v_VETerritoryCoveredLossesIncurredPostAcquisition": "territory_covered_losses_post_acquisition",
    "aapl_OtherComprehensiveIncomeLossDerivativeInstrumentGainLossReclassificationAfterTax": "oci_derivative_reclassification_after_tax",
    "aapl_OtherComprehensiveIncomeLossDerivativeInstrumentGainLossafterReclassificationandTax": "oci_derivative_after_reclassification_tax",
    "aapl_OtherComprehensiveIncomeLossDerivativeInstrumentGainLossbeforeReclassificationafterTax": "oci_derivative_before_reclassification_after_tax",
    "v_OtherComprehensiveIncomeLossCashFlowHedgeAndNetInvestmentHedgeGainLossBeforeReclassificationAndTax": "oci_cashflow_hedge_investment_before_reclassification_tax",
    "v_OtherComprehensiveIncomeLossCashFlowHedgeAndNetInvestmentHedgeGainLossBeforeReclassificationTax": "oci_cashflow_hedge_investment_before_reclassification_tax",
    "v_OtherComprehensiveIncomeLossCashFlowHedgeGainLoss": "oci_cashflow_hedge",
    "v_OtherComprehensiveIncomeLossCashFlowHedgeGainLossTax": "oci_cashflow_hedge_tax",
    "abnb_OperationsAndSupportExpense": "operations_support_expense","us-gaap_CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents": "cash_equivalents_restricted_cash",
    "us-gaap_CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect": "cash_equivalents_restricted_cash_change_exchange",
    "us-gaap_IncreaseDecreaseInContractWithCustomerLiability": "change_contract_customer_liability",
    "us-gaap_IncreaseDecreaseInOtherOperatingLiabilities": "change_other_operating_liabilities",
    "us-gaap_PaymentsRelatedToTaxWithholdingForShareBasedCompensation": "payments_tax_withholding_share_compensation",
    "us-gaap_PaymentsToAcquireAvailableForSaleSecuritiesDebts": "payments_acquire_available_sale_debt",
    "us-gaap_PaymentsToAcquireBusinessesNetOfCashAcquired": "payments_acquire_businesses_net_cash",
    "us-gaap_ProceedsFromMaturitiesPrepaymentsAndCallsOfAvailableForSaleSecurities": "proceeds_maturities_prepayments_available_sale",
    "us-gaap_ProceedsFromPaymentsForOtherFinancingActivities": "proceeds_other_financing_activities",
    "us-gaap_ProceedsFromSaleAndMaturityOfOtherInvestments": "proceeds_sale_maturity_other_investments",
    "us-gaap_ProceedsFromSaleOfAvailableForSaleSecuritiesDebts": "proceeds_sale_available_sale_debt",
    "us-gaap_EffectOfExchangeRateOnCashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents": "exchange_rate_effect_cash_equivalents_restricted_cash",
    "us-gaap_ForeignCurrencyTransactionGainLossBeforeTax": "foreign_currency_transaction_gain_loss",
    "us-gaap_IncreaseDecreaseInPrepaidDeferredExpenseAndOtherAssets": "change_prepaid_deferred_expense_assets",
    "us-gaap_ProceedsFromIssuanceOfSharesUnderIncentiveAndShareBasedCompensationPlans": "proceeds_issuance_shares_incentive_compensation",
    "us-gaap_ProceedsFromIssuanceOfSharesUnderIncentiveAndShareBasedCompensationPlansIncludingStockOptions": "proceeds_issuance_shares_incentive_compensation_options",
    "us-gaap_ProceedsFromMaturitiesPrepaymentsAndCallsOfShorttermInvestments": "proceeds_maturities_prepayments_shortterm_investments",
    "us-gaap_ProceedsFromSaleAndMaturityOfMarketableSecurities": "proceeds_sale_maturity_marketable_securities",
    "us-gaap_IncreaseDecreaseInAccountsPayableAndAccruedLiabilities": "change_accounts_payable_accrued_liabilities",
    "us-gaap_EffectOfExchangeRateOnCashAndCashEquivalents": "exchange_rate_effect_cash_equivalents",
    "us-gaap_IncreaseDecreaseInAccruedLiabilitiesAndOtherOperatingLiabilities": "change_accrued_liabilities_operating_liabilities",
    "us-gaap_PaymentForContingentConsiderationLiabilityFinancingActivities": "payment_contingent_consideration_liability_financing",
    "us-gaap_PaymentsForDerivativeInstrumentFinancingActivities": "payments_derivative_instrument_financing",
    "us-gaap_PaymentsForProceedsFromDerivativeInstrumentInvestingActivities": "payments_proceeds_derivative_instrument_investing",
    "us-gaap_ProceedsFromSaleAndMaturityOfAvailableForSaleSecurities": "proceeds_sale_maturity_available_sale",
    "us-gaap_OtherComprehensiveIncomeLossAvailableForSaleSecuritiesAdjustmentNetOfTax": "oci_available_sale_securities_net_tax",
    "us-gaap_OtherComprehensiveIncomeLossCashFlowHedgeGainLossReclassificationAfterTax": "oci_cashflow_hedge_reclassification_after_tax",
    "us-gaap_OtherComprehensiveIncomeLossDerivativesQualifyingAsHedgesNetOfTax": "oci_derivatives_hedges_net_tax",
    "us-gaap_OtherComprehensiveIncomeLossForeignCurrencyTransactionAndTranslationAdjustmentNetOfTax": "oci_foreign_currency_translation_net_tax",
    "us-gaap_OtherComprehensiveIncomeLossNetOfTaxPortionAttributableToParent": "oci_net_tax_parent",
    "us-gaap_OtherComprehensiveIncomeLossReclassificationAdjustmentFromAOCIForSaleOfSecuritiesNetOfTax": "oci_reclassification_sale_securities_net_tax",
    "us-gaap_OtherComprehensiveIncomeLossReclassificationAdjustmentFromAOCIOnDerivativesNetOfTax": "oci_reclassification_derivatives_net_tax",
    "us-gaap_OtherComprehensiveIncomeUnrealizedGainLossOnDerivativesArisingDuringPeriodNetOfTax": "oci_unrealized_derivatives_net_tax",
    "us-gaap_OtherComprehensiveIncomeUnrealizedHoldingGainLossOnSecuritiesArisingDuringPeriodNetOfTax": "oci_unrealized_securities_net_tax",
    "us-gaap_OtherComprehensiveIncomeLossCashFlowHedgeGainLossAfterReclassificationAndTaxParent": "oci_cashflow_hedge_after_reclassification_tax_parent",
    "us-gaap_OtherComprehensiveIncomeForeignCurrencyTransactionAndTranslationAdjustmentNetOfTaxPortionAttributableToParent": "oci_foreign_currency_translation_net_tax_parent",
    "us-gaap_OtherComprehensiveIncomeForeignCurrencyTransactionAndTranslationAdjustmentBeforeTaxPortionAttributableToParent": "oci_foreign_currency_translation_before_tax_parent",
    "us-gaap_OtherComprehensiveIncomeLossBeforeTaxPortionAttributableToParent": "oci_before_tax_parent",
    "us-gaap_OtherComprehensiveIncomeLossTaxPortionAttributableToParent1": "oci_tax_parent",
    "us-gaap_OtherComprehensiveIncomeUnrealizedHoldingGainLossOnSecuritiesArisingDuringPeriodBeforeTax": "oci_unrealized_securities_before_tax",
    "us-gaap_ComprehensiveIncomeNetOfTaxIncludingPortionAttributableToNoncontrollingInterest": "comprehensive_income_net_tax_noncontrolling",
    "us-gaap_OtherComprehensiveIncomeLossAmortizationAdjustmentFromAOCIPensionAndOtherPostretirementBenefitPlansForNetPriorServiceCostCreditBeforeTax": "oci_pension_amortization_prior_service_before_tax",
    "us-gaap_OtherComprehensiveIncomeLossAmortizationAdjustmentFromAOCIPensionAndOtherPostretirementBenefitPlansForNetPriorServiceCostCreditTax": "oci_pension_amortization_prior_service_tax",
    "us-gaap_OtherComprehensiveIncomeLossCashFlowHedgeGainLossBeforeReclassificationAndTax": "oci_cashflow_hedge_before_reclassification_tax",
    "us-gaap_OtherComprehensiveIncomeLossCashFlowHedgeGainLossBeforeReclassificationTax": "oci_cashflow_hedge_before_reclassification_tax",
    "us-gaap_OtherComprehensiveIncomeLossCashFlowHedgeGainLossReclassificationBeforeTax": "oci_cashflow_hedge_reclassification_before_tax",
    "us-gaap_OtherComprehensiveIncomeLossCashFlowHedgeGainLossReclassificationTax": "oci_cashflow_hedge_reclassification_tax",
    "us-gaap_OtherComprehensiveIncomeLossForeignCurrencyTransactionAndTranslationAdjustmentBeforeTax": "oci_foreign_currency_translation_before_tax",
    "us-gaap_OtherComprehensiveIncomeLossForeignCurrencyTranslationAdjustmentTax": "oci_foreign_currency_translation_tax",
    "us-gaap_OtherComprehensiveIncomeLossPensionAndOtherPostretirementBenefitPlansAdjustmentBeforeReclassificationAdjustmentsAndTax": "oci_pension_postretirement_before_adjustments_tax",
    "us-gaap_OtherComprehensiveIncomeLossPensionAndOtherPostretirementBenefitPlansBeforeReclassificationAdjustmentsTax": "oci_pension_postretirement_before_adjustments_tax",
    "us-gaap_OtherComprehensiveIncomeLossReclassificationAdjustmentFromAOCIForSaleOfSecuritiesBeforeTax": "oci_reclassification_sale_securities_before_tax",
    "us-gaap_OtherComprehensiveIncomeLossReclassificationAdjustmentFromAOCIForSaleOfSecuritiesTax": "oci_reclassification_sale_securities_tax",
    "us-gaap_OtherComprehensiveIncomeUnrealizedHoldingGainLossOnSecuritiesArisingDuringPeriodTax": "oci_unrealized_securities_tax",
    "us-gaap_IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest": "income_continuing_operations_before_tax",
    "us-gaap_RevenueFromContractWithCustomerExcludingAssessedTax": "revenue_contract_customer_excluding_tax",
    "us-gaap_WeightedAverageNumberOfDilutedSharesOutstanding": "weighted_average_diluted_shares",
    "us-gaap_WeightedAverageNumberOfSharesOutstandingBasic": "weighted_average_basic_shares",
    "us-gaap_IncomeLossFromContinuingOperationsPerBasicShare": "income_continuing_operations_basic_share",
    "us-gaap_IncomeLossFromContinuingOperationsPerDilutedShare": "income_continuing_operations_diluted_share",
    "us-gaap_IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments": "income_continuing_operations_before_tax_equity_investments"
}


def clean_column_name(concept_name: str) -> str:
    """
    Cleans the concept name to be used as a column name in a DataFrame.
    It drops the 'us-gaap:' prefix and replaces the ':' with an underscore.

    Args:
        concept_name: The concept name to be cleaned.
    returns:
        The cleaned concept name.
    """
    cleaned_name = re.sub(r"us-gaap_|abnb_|crm_|v_|aapl_", "", concept_name) # simply remove the 'us-gaap:' prefix
    cleaned_name = re.sub(r"([A-Z])", r"_\1", cleaned_name).lower().strip("_")# replace ':' with '_'
    cleaned_name = cleaned_name.replace("and", "_").replace("netof", "_").replace("net", "_").replace("including", "_").replace("with", "_") #replace common words.
    return cleaned_name


def map_column_name(concept_names: list) -> list:
    """
    It first looks up for the mapping and if it exists, it returns the mapped concept name.
    otherwise it will call the function clean_column_name to clean the concept name.

    Args:
        concept_name: The concept name to be mapped.
    returns:
        The mapped concept name.
    """
    mapped_names = []
    for name in concept_names:
        if name in NAME_MAPPING:
            mapped_names.append(NAME_MAPPING[name])
        else:
            mapped_names.append(clean_column_name(name))
    return mapped_names


def UploadToBlob(container, filename, data):
    """
    Uploads the data to Azure Blob Storage.

    Args:
        fileName: The name of the file to be uploaded.
        data: The data to be uploaded.
    """ 
    uploadable_data = convert_to_csv(data)
    try:
        blob_service_client = BlobServiceClient.from_connection_string(config.connection_string)
        blob_client = blob_service_client.get_blob_client(container=container, blob=f'{filename}.csv')
        blob_client.upload_blob(uploadable_data)
        logger.info(f"Data uploaded to Azure Blob Storage: {filename}")
    except Exception as e:
        print(f"Error uploading data to Azure Blob Storage: {e}")

def convert_to_csv(data: dict):
    df = pd.DataFrame(data)
    return df.to_csv(index=False, header=True, encoding='utf-8')
