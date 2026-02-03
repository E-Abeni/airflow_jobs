"""
Todo:
1. Select all the required fields only
2. Convert to smallcase everything
3. Parse and convert data/time data to appropirate format
4. Remove extreme dates (but extreme dates can be an indication for abnromal accounts)
5. Try to remove duplications
6. Try to make pattern matching (actually done on identity resolution step)
7. Try to handle missing values


Final Output:
    - Cleaned data with only necessary fields
"""
"""------------------------------------------------------------------------------------------------------------------------------"""
import pandas as pd
from main import Database_Connector
import string

db_connector = Database_Connector()

input_table_name = "test_transactions"
output_table_name = "cleaned_transactions"

selected_columns = ['transactionid', 'branchid', 'branchname', 'transactiondate', 'transactiontime', 'transactiontype', 
                    'conductingmanner', 'currencytype', 'amountinbirr', 'amountincurrency', 
                    'sex', 'birthdate', 'occupation', 'bussinesstelno', 'houseno',
                    'accountno', 'accownername', 'accounttype', 'openeddate', 'closeddate', 
                    'benfullname', 'benaccountno', 'bentelno', 'benisentity', 'benworeda']

df = db_connector.data_to_pandas_df(
    db_connector.get_table_data_selected(table_name=input_table_name, selected_columns=selected_columns),
    selected_columns)

# 1. Convert all values in every column to lower case

string_cols = df.select_dtypes(include=['object', 'string']).columns

df[string_cols] = df[string_cols].apply(lambda x: x.str.lower().str.strip() if isinstance(x, pd.Series) else x)

# 2. Convert data and time columns to appropirate format and also join transaction date and time columns (transactiondate, transactiontime, birthdate, openeddate, closeddate)

df['transactiondate'] = pd.to_datetime(df['transactiondate'].fillna(pd.NaT), errors='coerce')
df['transactiontime'] = pd.to_datetime(df['transactiontime'].fillna(pd.NaT), format='%H:%M:%S', errors='coerce')
df['transactiondatetime'] = df['transactiondate'] + pd.to_timedelta(df['transactiontime'].dt.hour, unit='h') + pd.to_timedelta(df['transactiontime'].dt.minute, unit='m')
df['transactiondatetime'] = df['transactiondatetime'].dt.tz_localize(None)
df['birthdate'] = pd.to_datetime(df['birthdate'].fillna(pd.NaT), errors='coerce').dt.date
df['openeddate'] = pd.to_datetime(df['openeddate'].fillna(pd.NaT), errors='coerce').dt.date
df['closeddate'] = pd.to_datetime(df['closeddate'].fillna(pd.NaT), errors='coerce').dt.date

# 3. Remove extreme dates

#df = df[df['transactiondatetime'].notna() & (df['transactiondatetime'] >= '2000-01-01') & (df['transactiondatetime'] <= '2025-12-31')]
#df = df[df['birthdate'].notna() & (df['birthdate'] >= '1900-01-01') & (df['birthdate'] <= '2025-12-31')]
#df = df[df['openeddate'].notna() & (df['openeddate'] >= '2000-01-01') & (df['openeddate'] <= '2025-12-31')]
#df = df[df['closeddate'].notna() & (df['closeddate'] >= '2000-01-01') & (df['closeddate'] <= '2025-12-31')]

# 4. Hash the transactionid column
import hashlib
df['transactionid'] = df['transactionid'].apply(lambda x: hashlib.sha256(x.encode()).hexdigest() if isinstance(x, str) else x)


# 5. Create a unified address field for the sender and beneficiary using multiple columns identified in the dataset
def transform_address(address):
    address = str(address).strip().lower()
    remove_chars = string.punctuation + string.digits + " "
    table = str.maketrans('', '', remove_chars)
    address = address.translate(table)

    if address in ['nan', 'none', '', 'na']:
        return pd.NA
    elif "not" in address.split():
        return pd.NA
    elif len(address) > 3:
        if ('aa' in address or "ad" in address or "add" in address or "aba" in address):
            return "aa"
        elif ('ti' in address or 'mk' in address or 'tig' in address or 'shire' in address or 'adw' in address or 'ax' in address or 'adi' in address):
            return "tg"
        elif ('or' in address):
            return 'or'
        elif ('snn' in address):
            return 'snnp'
        elif ('af' in address):
            return 'af'
        elif ('sid' in address):
            return 'sid'
        elif ('am' in address or 'amh' in address):
            return 'am'
        elif ('gam' in address):
            return 'gamb'
        elif ('so' in address):
            return 'som'
        elif ('ben' in address):
            return 'bgum'
        elif ('har' in address):
            return 'har'
        elif ('dd' in address or 'dire' in address or 'dre' in address):
            return 'ddw'
        else:
            return pd.NA
    else:
        return address
    

df['senderaddress'] = df['houseno'].apply(lambda x: transform_address(x))


# 6. Create a unified address field for the beneficiary using multiple columns identified in the dataset
df['beneficiaryaddress'] = df['benworeda'].apply(lambda x: transform_address(x))


# 7. Create Unified phone number field and clean the phone number to appropirate format 251******* for every transaction (Phone numbers are found on different columns in our dataset)
def transform_phone_number(phone):
    if "." in str(phone):
        phone = str(phone).split('.')[0]

    delete_chars = string.ascii_letters + string.punctuation + " "
    table = str.maketrans('', '', delete_chars)
    phone = str(phone).translate(table)

    if isinstance(phone, str):
        phone = phone.strip()
        if len(phone) < 7:
            return pd.NA
        elif phone.startswith("0"):
            return f"251{phone[1:]}"
        elif phone.startswith("9"):
            return f"251{phone}"
        elif phone.startswith("+251"):
            return phone[1:]
        elif phone.startswith("251"):
            return phone
        elif phone.startswith("+9"):
            return f"251{phone[2:]}"
        elif len(phone) == 8:
            return f"2519{phone}"
        elif len(phone) == 10 and not phone.startswith("09"):
            return f"251{phone[2:]}"
    else:
        try:
            phone = str(int(phone))
            transform_phone_number(phone)
        except:
            return pd.NA
    return phone

df['senderphone'] = df['bussinesstelno'].apply(lambda x: transform_phone_number(x))



# 8. Create Unified phone number field and clean the phone number to appropirate format 251******* for every transaction (Phone numbers are found on different columns in our dataset)
df['beneficiaryphone'] = df['bentelno'].fillna(df['benisentity']).apply(lambda x: transform_phone_number(x))


# 9. Remove duplicates if any
df = df.drop_duplicates(subset=['transactionid'], keep='first')

# 10. Handle missing values if any
df = df.dropna(subset=['transactionid', 'amountinbirr', 'transactiondatetime'])


# 11. Remove any decimal palces in account number columns if any and convert to string
df['accountno'] = df['accountno'].apply(lambda x: str(x).split('.')[0] if isinstance(x, float) else str(x).split('.')[0])
df['benaccountno'] = df['benaccountno'].apply(lambda x: str(x).split('.')[0] if isinstance(x, float) else str(x).split('.')[0])


# 12. Final cleaned data output (23 columns)

df = df[['transactionid', 'branchid', 'branchname', 'transactiondatetime', 'transactiontype', 'conductingmanner', 
         'currencytype', 'amountinbirr', 'amountincurrency', 
         'sex', 'birthdate', 'occupation', 'senderaddress', 'senderphone',
         'accountno', 'accownername', 'accounttype', 'openeddate', 'closeddate', 
         'benfullname', 'benaccountno', 'beneficiaryaddress', 'beneficiaryphone']]

# 13. Save the cleaned data to the database with the output table name
df.to_sql(output_table_name, db_connector.get_engine(), if_exists='replace', index=False)


print("Done! Cleaned data saved to the database. Table name:", output_table_name)




















"""------------------------------------------------------------------------------------------------------------------------------"""
"""
from pathlib import Path

file_path = Path.home() / "output/data_cleaning_test_file.txt"

def main():
    try:
        file_path.write_text("Hello Abeni! This is a data cleaning test file.")
        print(f"Success! File created at: {file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()

"""