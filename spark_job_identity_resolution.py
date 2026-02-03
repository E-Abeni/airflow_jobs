"""
    ToDo:
    1. Select necessary data (both for sender and beneficiary)
    2. Create Identity table (Person Profile Table)
        PERSON_ID
        NAME
        DOB
        GENDER
        NATIONALITY
        OCCUPATION
        RESIDENCE_COUNTRY
        //RISK_LEVEL
        //KYC_COMPLETENESS_SCORE
        //PEP_FLAG
        //SANCTION_FLAG

    3. Create Account table (Account Table)
        ACCOUNT_ID
        PERSON_ID
        BANK_ID
        BRANCH_ID
        ACCOUNT_TYPE
        OPEN_DATE
        BALANCE
        STATUS

    4. Try to get accounts of the same person
        Deterministic Way:
        - Name Match
        - Name + DOB match
        - ID card Match
        - Passport Number Match
        - Phone Number Match
        - [National ID when available]
        Probablistic Way:
        - Name similarity + DOB similarity + Address similarity + Phone similarity → confidence score

    5. Create entity table for identified entities
        - Person
        - Accounts
        - Phone
        - Address
        - Transaction

    Final Output: (UNIFIED DATA MODEL)
    - Clean Transactions Table with identity resolved (TRANSACTION TABLE)
    - All unique account entities at different banks and branches (ACCOUNT TABLE)
    - All unique person entities (PERSON TABLE)
        (who used to have different name variants, different accounts, different/same phone number, different addresses)
    
Note:
- From this step on every analysis will be done for either the person or the transaction
- A person have multiple accounts
- Our goal is to catch criminal person not criminal account

"""
"""------------------------------------------------------------------------------------------------------------------------------"""
import pandas as pd
import numpy as np
import uuid
import recordlinkage as rl
import networkx as nx
from main import Database_Connector

db_connector = Database_Connector()

input_table_name = "cleaned_transactions"
output_table_name = "identity_resolved_transactions"
person_entity_table_name = "person_entity_table"
account_entity_table_name = "account_entity_table"

column_names = db_connector.get_table_columns(table_name=input_table_name)

df = db_connector.data_to_pandas_df(
    db_connector.get_table_data_selected(table_name=input_table_name, selected_columns=column_names),
    column_names)


transaction_columns = ['accountno', 'fromentity', 'benaccountno', 'toentity', 'transactionid', 'transactiondatetime', 'transactiontype', 'conductingmanner', 
                       'currencytype', 'amountinbirr', 'amountincurrency', 'branchid', 'branchname', 'beneficiaryaddress']
account_columns = ['ownerentity', 'accountno', 'ownername', 'accounttype', 'openeddate', 'closeddate']
person_columns = ['personid', 'aliases', 'sex', 'birthdate', 'occupation', 'location', 'phonenumbers']


ben_info_columns = ['benfullname', 'beneficiaryaddress', 'beneficiaryphone']
sender_info_columns = ['accownername', 'senderaddress', 'senderphone', 'sex', 'birthdate', 'occupation']

ben_account_columns = ['benaccountno', 'benfullname']
sender_account_columns = ['accountno', 'accownername', 'accounttype', 'openeddate', 'closeddate']


# 1. Create Person Entity Table To get all unique persons (both sender and beneficiary)
df_senders = df[[x for x in sender_info_columns if x in df.columns]].drop_duplicates().reset_index(drop=True)
df_senders.rename(columns={'accownername': 'alias', 'senderaddress': 'location', 'senderphone': 'phonenumber'}, inplace=True)

df_beneficiaries = df[[x for x in ben_info_columns if x in df.columns]].drop_duplicates().reset_index(drop=True)
df_beneficiaries.rename(columns={'benfullname': 'alias', 'beneficiaryaddress': 'location', 'beneficiaryphone': 'phonenumber'}, inplace=True)

df_person = pd.concat([df_senders, df_beneficiaries], ignore_index=True).drop_duplicates().reset_index(drop=True)
"""df_person['personid'] = df_person.apply(lambda row: str(uuid.uuid4()) + f"_{row.name}", axis=1)"""



# 2. Identity Resolution Logic. Our Aim is to identify same person with different name variants using the given information to identify which unique entities are similar and most probably the same person.
# Methods to use:
# - Deterministic Way:
#     - Name Match
#     - Name + DOB match
#     - Name + Phone match
# - Probablistic Way:
#     - Name similarity + DOB similarity + Address similarity + Phone similarity → confidence score

"""
indexer = rl.Index()
indexer.block(['sex', 'location']) 
candidate_links = indexer.index(df_person)

compare = rl.Compare()

compare.exact('birthdate', 'birthdate', label='birthdate_match', missing_value=np.nan)
compare.exact('phonenumber', 'phonenumber', label='phone_match', missing_value=np.nan)

compare.string('phonenumber', 'phonenumber', method='jarowinkler', threshold=0.95, label='phone_similarity', missing_value=np.nan)
compare.string('alias', 'alias', method='jarowinkler', threshold=0.9, label='alias_score', missing_value=np.nan)
compare.string('location', 'location', method='jarowinkler', threshold=0.7, label='location_score', missing_value=np.nan)
compare.string('occupation', 'occupation', method='jarowinkler', threshold=0.8, label='job_score', missing_value=np.nan)

features = compare.compute(candidate_links, df_person)

features['total_points'] = features.sum(axis=1, skipna=True)
features['possible_points'] = features.notna().sum(axis=1)
features['normalized_score'] = features['total_points'] / features['possible_points']
features['normalized_score'] = features['normalized_score'].fillna(0)

#features.sort_values(by='normalized_score', ascending=False).to_csv("identity_resolution_scores.csv", index=True)

weights = {
    'birthdate_match': 0.05,
    'phone_match': 0.05,
    'phone_similarity': 0.35,
    'alias_score': 0.45,
    'location_score': 0.05,
    'job_score': 0.05,
}

features_weighted = features
features_weighted = features_weighted\
.assign(weighted_birthday_match = lambda x: x['birthdate_match'] * weights['birthdate_match'])\
.assign(weighted_phone_match = lambda x: x['phone_match'] * weights['phone_match'])\
.assign(weighted_phone_similarity = lambda x: x['phone_similarity'] * weights['phone_similarity'])\
.assign(weighted_alias_score = lambda x: x['alias_score'] * weights['alias_score'])\
.assign(weighted_location_score = lambda x: x['location_score'] * weights['location_score'])\
.assign(weighted_job_score = lambda x: x['job_score'] * weights['job_score'])


features_weighted['total_possible_score'] = features.notna()\
    .assign(possible_birthdate_match = lambda x: x["birthdate_match"] * weights['birthdate_match'])\
    .assign(possible_phone_match = lambda x: x["phone_match"] * weights['phone_match'])\
    .assign(possible_phone_similarity = lambda x: x["phone_similarity"] * weights['phone_similarity'])\
    .assign(possible_alias_score = lambda x: x["alias_score"] * weights['alias_score'])\
    .assign(possible_location_score = lambda x: x["location_score"] * weights['location_score'])\
    .assign(possible_job_score = lambda x: x["job_score"] * weights['job_score'])\
    .assign(total_score = lambda x: x[[
        "possible_birthdate_match", 
        "possible_phone_match", 
        "possible_phone_similarity", 
        "possible_alias_score", 
        "possible_location_score", 
        "possible_job_score"
    ]].sum(axis=1))['total_score']


features_weighted['total_score'] = features_weighted[[
    "weighted_birthday_match", 
    "weighted_phone_match", 
    "weighted_phone_similarity",
    "weighted_alias_score", 
    "weighted_location_score", 
    "weighted_job_score"
]].sum(axis=1)

features_weighted["similarity_index"] = features_weighted["total_score"] / features_weighted["total_possible_score"]

features_weighted.sort_values(by='similarity_index', ascending=False).to_csv("identity_resolution_scores.csv", index=True)


matches_df = features_weighted.loc[features_weighted['similarity_index'] > 0.4, ['similarity_index']].sort_values(by='similarity_index', ascending=False).index
matches = list(matches_df)
"""
#
matches = []

def indexes_to_tuple(group_series):
    return tuple(group_series.tolist())

matches += (
    df_person
    .reset_index()
    .groupby("alias")
    .agg(
        sum_count=('index', 'count'),
        matches=('index', indexes_to_tuple)
    )
    .query("sum_count > 1")
    ['matches'].tolist()
)

def clean_matches(matches):
    cleaned = []

    for match in matches:
        if len(match) <= 1:
            continue
        elif len(match) == 2:
            cleaned.append(match)
            continue
        for i in range(len(match)):
            for j in range(i + 1, len(match)):
                pair = (match[i], match[j])
                if pair not in cleaned and (pair[1], pair[0]) not in cleaned:
                    cleaned.append(pair)

    return cleaned

matches = clean_matches(matches)


G = nx.Graph()
G.add_edges_from(matches)

clusters = list(nx.connected_components(G))

entity_map = {}
for cluster_id, nodes in enumerate(clusters):
    p_id = uuid.uuid4()
    for node in nodes:
        entity_map[node] = f"ENTITY_{cluster_id}_{p_id}"

personid_list = [str(uuid.uuid4()) + f"_{i}" if i not in entity_map.keys() else entity_map[i] for i in range(len(df_person))]
df_person['personid'] = personid_list


# 3. Create Account Entity Table
df_senders_account = df[[x for x in sender_account_columns if x in df.columns]].drop_duplicates().reset_index(drop=True)
df_senders_account.rename(columns={'accownername': 'ownername'}, inplace=True)

df_beneficiaries_account = df[[x for x in ben_account_columns if x in df.columns]].drop_duplicates().reset_index(drop=True)
df_beneficiaries_account.rename(columns={'benfullname': 'ownername', 'benaccountno': 'accountno'}, inplace=True)

df_accounts = pd.concat([df_senders_account, df_beneficiaries_account], ignore_index=True).drop_duplicates().reset_index(drop=True)
df_accounts['accountid'] = df_accounts.apply(lambda row: str(uuid.uuid4()) + f"_{row.name}", axis=1)

#print(df_person)

df_accounts['ownerentity'] = df_accounts['ownername'].map(df_person.drop_duplicates(subset=['personid'], keep='first').set_index('alias')['personid'])


# 4. Create Final Identity Resolved Transaction Table
df_transaction = df[[x for x in transaction_columns if x in df.columns]].copy()
df_transaction['fromentity'] = df_transaction['accountno'].map(df_accounts.drop_duplicates(subset=['accountno'], keep='first').set_index('accountno')['ownerentity'])
df_transaction['toentity'] = df_transaction['benaccountno'].map(df_accounts.drop_duplicates(subset=['accountno'], keep='first').set_index('accountno')['ownerentity'])

df_transaction = df_transaction[transaction_columns]


# 5. Save the final tables to the database
df_person.to_sql(person_entity_table_name, db_connector.get_engine(), if_exists='replace', index=False)
df_accounts.to_sql(account_entity_table_name, db_connector.get_engine(), if_exists='replace', index=False)
df_transaction.to_sql(output_table_name, db_connector.get_engine(), if_exists='replace', index=False)

print("Done! Identity resolved data saved to the database. Table names:", person_entity_table_name, ",", account_entity_table_name, ",", output_table_name)

"""
print(df_person)
print(df_accounts)
print(df_transaction)
print(df_transaction.dtypes)
"""




















"""------------------------------------------------------------------------------------------------------------------------------"""
"""

from pathlib import Path

file_path = Path.home() / "output/identity_resolution_test_file.txt"

def main():
    try:
        file_path.write_text("Hello Abeni! This is an identity resolution test file.")
        print(f"Success! File created at: {file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
    
"""