"""
Key: Person ID
    ToDo:
    1. Calculate Static Risk Features (For Person ID)
        //Country risk -------------- location
        Occupation risk ----------- occupation
        PEP risk ------------ alias, occupation
        Sanction risk ---------- alias, occupation
        High-risk geography ---------- location
        //Business type -------------- occupation
        Account age ------------- average(opendate)
        //KYC completeness -------- location, occupation, alias
        No of Aliases Used --------- unique(aliases)
        No of Phone Numbers used -------- unique(phone numbers)
        No of Accounts held -------- unique(account numbers)

    2. Behavioral Feature (for Person ID)
        Avg transaction amount
        Std deviation of amounts
        Tx frequency/day/week/month
        Tx volume/day/week/month
        Total Amount recieved
        Amount recieved vs sent ratio
        Cash vs non-cash ratio
        Cross-border ratio
        Night-time transaction ratio
        New/unique beneficiary ratio
        Dormant → active pattern


"""
"""------------------------------------------------------------------------------------------------------------------------------"""
import pandas as pd
import numpy as np
from main import Database_Connector

db_connector = Database_Connector()
engine = db_connector.get_engine()


input_table_name_1 = "identity_resolved_transactions"
input_table_name_2 = "person_entity_table"
input_table_name_3 = "account_entity_table"
output_table_name = "user_profiles_v2"

transactions_column_names = db_connector.get_table_columns(table_name=input_table_name_1)
person_column_names = db_connector.get_table_columns(table_name=input_table_name_2)
account_column_names = db_connector.get_table_columns(table_name=input_table_name_3)

df_transactions = db_connector.data_to_pandas_df(
    db_connector.get_table_data_selected(table_name=input_table_name_1, selected_columns=transactions_column_names),
    transactions_column_names)
df_person = db_connector.data_to_pandas_df(
    db_connector.get_table_data_selected(table_name=input_table_name_2, selected_columns=person_column_names),
    person_column_names)
df_account = db_connector.data_to_pandas_df(
    db_connector.get_table_data_selected(table_name=input_table_name_3, selected_columns=account_column_names),
    account_column_names)


"""
print(df_transactions.head())
print(df_person.head())
print(df_account.head())

print("--------------")

print(len(df_transactions))
print(len(df_person))
print(len(df_account))

print(df_transactions.dtypes)
print(df_person.dtypes)
print(df_account.dtypes)
"""


df_account['openeddate'] = pd.to_datetime(df_account['openeddate'], errors='coerce')

df_user_risk = pd.DataFrame(index=df_person['personid'].unique())



# 1. Calculate Static Risk Features (For Person ID)
#     Geography risk -------------- location



#     Occupation risk ----------- occupation




#     PEP risk ------------ alias, occupation



#     Sanction risk ---------- alias, occupation




#     Unique Aliases Used --------- unique(aliases)

df_user_risk["unique_aliases_used"] = df_person.groupby('personid')['alias'].nunique()


#     Unique Phone Numbers used -------- unique(phone numbers)

df_user_risk["unique_phone_numbers_used"] = df_person.groupby('personid')['phonenumber'].nunique()


#     Unique Accounts held -------- unique(account numbers)

df_user_risk["unique_accounts_held"] = df_account.groupby('ownerentity')['accountno'].nunique()


#     Account age ------------- min(opendate)

today = pd.Timestamp.now()

bins = list(range(0,21, 2)) + [np.inf]
labels = [f"{x} < x < {bins[i+1]}" for i,x in enumerate(bins[:-2])] + [f"> {bins[-2]}"]

df_user_risk[['account_age_days', 'account_age_years', 'account_age_bucket']] = df_account\
                            .groupby(['ownerentity'])\
                            .agg(openeddate=('openeddate', 'min'))\
                            .assign(account_age = lambda x: today - x['openeddate'])\
                            .assign(account_age_days = lambda x: x['account_age'].dt.days)\
                            .assign(account_age_years = lambda x: x['account_age_days'] / 365)\
                            .assign(account_age_bucket = lambda x: pd.cut(x['account_age_years'], bins=bins, labels=labels))\
                            [['account_age_days', 'account_age_years', 'account_age_bucket']]




# 2. Behavioral Feature (for Person ID)


##     No of transactions sent/received

df_user_risk['no_of_transactions_sent'] = df_transactions.groupby('fromentity').size()
df_user_risk['no_of_transactions_received'] = df_transactions.groupby('toentity').size()

##     Avg transaction amount

df_user_risk['avg_transaction_amount_sent'] = df_transactions.groupby('fromentity')['amountinbirr'].mean()
df_user_risk['avg_transaction_amount_received'] = df_transactions.groupby('toentity')['amountinbirr'].mean()

##     Std deviation of amounts

df_user_risk['std_transaction_amount_sent'] = df_transactions.groupby('fromentity')['amountinbirr'].std()
df_user_risk['std_transaction_amount_received'] = df_transactions.groupby('toentity')['amountinbirr'].std()
df_user_risk['std_transaction_amount_sent_and_received'] = pd.concat([
    df_transactions[['fromentity', 'amountinbirr']].rename(columns={'fromentity': 'entity'}),
    df_transactions[['toentity', 'amountinbirr']].rename(columns={'toentity': 'entity'})
]).groupby('entity')['amountinbirr'].std()


##     Tx frequency/hour/day/week/month

df_user_risk['max_freq_1hr'] = df_transactions.set_index('transactiondatetime')\
    .sort_index().groupby('fromentity')['amountinbirr'].rolling('1h').count().groupby('fromentity').max()

df_user_risk['max_freq_24hr'] = df_transactions.set_index('transactiondatetime')\
    .sort_index().groupby('fromentity')['amountinbirr'].rolling('24h').count().groupby('fromentity').max()

df_user_risk['max_freq_7d'] = df_transactions.set_index('transactiondatetime')\
    .sort_index().groupby('fromentity')['amountinbirr'].rolling('7D').count().groupby('fromentity').max()

df_user_risk['max_freq_1m'] = df_transactions.set_index('transactiondatetime')\
    .sort_index().groupby('fromentity')['amountinbirr'].rolling('30D').count().groupby('fromentity').max()

##     Tx volume/hour/day/week/month

df_user_risk['max_volume_1hr'] = df_transactions.set_index('transactiondatetime')\
    .sort_index().groupby('fromentity')['amountinbirr'].rolling('1h').sum().groupby('fromentity').max()

df_user_risk['max_volume_24hr'] = df_transactions.set_index('transactiondatetime')\
    .sort_index().groupby('fromentity')['amountinbirr'].rolling('24h').sum().groupby('fromentity').max()

df_user_risk['max_volume_7d'] = df_transactions.set_index('transactiondatetime')\
    .sort_index().groupby('fromentity')['amountinbirr'].rolling('7D').sum().groupby('fromentity').max()

df_user_risk['max_volume_1m'] = df_transactions.set_index('transactiondatetime')\
    .sort_index().groupby('fromentity')['amountinbirr'].rolling('30D').sum().groupby('fromentity').max()


##     Total Amount recieved
df_user_risk['total_amount_received'] = df_transactions.groupby('toentity')['amountinbirr'].sum()


##     Amount recieved vs sent ratio

df_user_risk['total_amount_sent'] = df_transactions.groupby('fromentity')['amountinbirr'].sum()
df_user_risk['amount_received_vs_sent_ratio'] = df_user_risk['total_amount_received'] / (df_user_risk['total_amount_sent'] + 1)  # Adding 1 to avoid division by zero

##     Cash vs non-cash ratio

df_user_risk['cash_transactions'] = df_transactions[df_transactions['transactiontype'] == 'cash'].groupby('fromentity')['amountinbirr'].sum()
df_user_risk['non_cash_transactions'] = df_transactions[df_transactions['transactiontype'] != 'cash'].groupby('fromentity')['amountinbirr'].sum()
df_user_risk['cash_vs_non_cash_ratio'] = df_user_risk['cash_transactions'] / (df_user_risk['non_cash_transactions'] + 1)

#     Cross-border ratio
local_addresses = ['aa', 'tg', 'or', 'snnp', 'af', 'sid', 'am', 'gamb', 'som', 'bgum', 'har', 'ddw']

df_user_risk["cross_border_risk"] = df_transactions[['fromentity','beneficiaryaddress']]\
                                        .assign(cross_risk = np.where(df_transactions['beneficiaryaddress'].isna() | df_transactions['beneficiaryaddress'].isin(local_addresses), 0, 1))\
                                        .groupby('fromentity')\
                                        .agg(cross_border_risk=('cross_risk', 'sum'))\
                                        ['cross_border_risk']


#     Night-time transaction ratio
df_transactions['transaction_hour'] = df_transactions['transactiondatetime'].dt.hour
df_user_risk['night_time_transaction_ratio'] = df_transactions.assign(is_night=lambda x: x['transaction_hour'].apply(lambda h: 1 if (h >= 20 or h < 6) else 0))\
                                        .groupby('fromentity')\
                                        .agg(total_transactions=('is_night', 'count'),
                                             night_transactions=('is_night', 'sum'))\
                                        .assign(night_time_transaction_ratio=lambda x: x['night_transactions'] / x['total_transactions'])\
                                        ['night_time_transaction_ratio']




#     New/unique beneficiary ratio

def get_single_ratio(group):
    total_volume = group[0].count()
    single_volume = group.loc[group[0] == 1, 0].count()
    return (single_volume / total_volume) * group[0].sum()

df_user_risk['new_beneficiary_ratio'] = df_transactions\
        .groupby(['fromentity', 'toentity'])\
        .size()\
        .reset_index()\
        .sort_values(by=['fromentity', 0], ascending=[True, False])\
        .groupby(['fromentity'])\
        .apply(get_single_ratio)\
        .rename("unknown_beneficiary_risk_score")


#     Dormant → active pattern

df_user_risk["all_transaction_times"] = pd.concat([
            df_transactions[['fromentity', 'transactiondatetime']].rename(columns={'fromentity': 'entity'}).assign(role=lambda x: "sender"),
            df_transactions[['toentity', 'transactiondatetime']].rename(columns={'toentity': 'entity'}).assign(role=lambda x: "beneficiary")
        ])\
        .sort_values(by=['entity', 'transactiondatetime'])\
        .assign(time_lapse = lambda x: x.groupby('entity')['transactiondatetime'].diff())\
        .reset_index(drop=True)\
        .assign(transactiondatetime = lambda x: x['transactiondatetime'].astype(str))\
        .assign(time_lapse = lambda x: x['time_lapse'].astype(str))\
        [['entity', 'transactiondatetime', 'time_lapse', 'role']]\
        .assign(
            time_lapse = lambda x: x['time_lapse'].fillna(pd.Timedelta(seconds=0)).astype(str),
            transactiondatetime = lambda x: x['transactiondatetime'].astype(str)
        )\
        .assign(transaction_timedelta_all=lambda x: '"' + x['transactiondatetime'].astype(str) + '": ' + "[\"" + x['time_lapse'].astype(str) + "\",\"" + x['role'].astype(str) + "\"]")\
        .groupby('entity')['transaction_timedelta_all']\
        .agg(lambda x: '{' + ', '.join(x) + '}')



df_user_risk[["min_time_lapse_minutes", "max_time_lapse_minutes", "avg_time_lapse_minutes"]] = pd.concat([
            df_transactions[['fromentity', 'transactiondatetime']].rename(columns={'fromentity': 'entity'}).assign(role=lambda x: "sender"),
            df_transactions[['toentity', 'transactiondatetime']].rename(columns={'toentity': 'entity'}).assign(role=lambda x: "beneficiary")
        ])\
        .sort_values(by=['entity', 'transactiondatetime'])\
        .assign(time_lapse = lambda x: x.groupby('entity')['transactiondatetime'].diff().dt.total_seconds() / 60)\
        .reset_index(drop=True)\
        .groupby("entity")\
        .agg(min_time_lapse_minutes=('time_lapse', 'min'), max_time_lapse_minutes=('time_lapse', 'max'), avg_time_lapse_minutes=('time_lapse', 'mean'))\
        .fillna(-1)\
        .apply(lambda x: round(x, 2))

df_user_risk["last_transaction_time"] = pd.concat([
            df_transactions[['fromentity', 'transactiondatetime']].rename(columns={'fromentity': 'entity'}).assign(role=lambda x: "sender"),
            df_transactions[['toentity', 'transactiondatetime']].rename(columns={'toentity': 'entity'}).assign(role=lambda x: "beneficiary")
        ])\
        .sort_values(by=['entity', 'transactiondatetime'])\
        .groupby('entity')\
        .last()['transactiondatetime']\
        .rename("last_transaction_time")



#     Prefered Branches

df_user_risk['prefered_branches'] = df_transactions[df_transactions['fromentity'].isin(df_user_risk.index)]\
                                        .groupby(['fromentity', 'branchid'])\
                                        .size()\
                                        .reset_index()\
                                        .rename(columns={0: "count"})\
                                        .assign(prefered_branches=lambda x: '"' + x['branchid'].astype(str) + '": ' + x['count'].astype(int).astype(str))\
                                        .groupby('fromentity')['prefered_branches']\
                                        .agg(lambda x: '{' + ', '.join(x) + '}')


#      Typical Transaction Types

df_user_risk['used_transaction_types'] = df_transactions[df_transactions['fromentity'].isin(df_user_risk.index)]\
                                        .groupby(['fromentity', 'transactiontype'])\
                                        .size()\
                                        .reset_index()\
                                        .rename(columns={0: "count"})\
                                        .assign(used_transaction_types=lambda x: '"' + x['transactiontype'].astype(str) + '": ' + x['count'].astype(int).astype(str))\
                                        .groupby('fromentity')['used_transaction_types']\
                                        .agg(lambda x: '{' + ', '.join(x) + '}')


#       Frequent Destinations

df_user_risk['frequent_destinations'] = df_transactions[df_transactions['fromentity'].isin(df_user_risk.index)]\
                                        .groupby(['fromentity', 'beneficiaryaddress'])\
                                        .size()\
                                        .reset_index()\
                                        .rename(columns={0: "count"})\
                                        .assign(frequent_destinations=lambda x: '"' + x['beneficiaryaddress'].astype(str) + '": ' + x['count'].astype(int).astype(str))\
                                        .groupby('fromentity')['frequent_destinations']\
                                        .agg(lambda x: '{' + ', '.join(x) + '}')

#       Top Beneficiaries

df_user_risk['top_beneficiaries'] = df_transactions[df_transactions['fromentity'].isin(df_user_risk.index)]\
                                        .groupby(['fromentity', 'toentity'])\
                                        .size()\
                                        .reset_index()\
                                        .rename(columns={0: "count"})\
                                        .sort_values(by=['fromentity', 'count'], ascending=[True, False])\
                                        .assign(top_beneficiaries=lambda x: '"' + x['toentity'].astype(str) + '": ' + x['count'].astype(int).astype(str))\
                                        .groupby('fromentity')['top_beneficiaries']\
                                        .agg(lambda x: '{' + ', '.join(x) + '}')


df_user_risk.reset_index().to_sql(output_table_name, con=engine, if_exists='replace', index=False)

print(df_user_risk.head())
print(len(df_user_risk))

"""------------------------------------------------------------------------------------------------------------------------------"""
"""

from pathlib import Path

file_path = Path.home() / "output/user_profiles_test_file.txt"

def main():
    try:
        file_path.write_text("Hello Abeni! This is a user profiles test file.")
        print(f"Success! File created at: {file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()

"""