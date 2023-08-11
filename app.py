import pandas as pd
from flask import Flask, render_template, request
from pymongo import MongoClient

app = Flask(__name__)

BATCH_SIZE = 1000

monogoDB_string = 'mongodb+srv://mukul:YWb0vrGQI@sapstore.kz6z8ks.mongodb.net'


def connect_to_mongodb():
    global client, db, collection
    client = MongoClient(monogoDB_string)
    db = client['test']
    collection = db['mukulsheets']


def disconnect_from_mongodb():
    global client
    client.close()


def clean_data_dataframe(df):
    # Remove the rows matching the string condition
    df = df.loc[~df.eq('Material no:').any(axis=1)]
    df = df.loc[~df.eq('Total for Binno.').any(axis=1)]
    df = df.loc[~df.eq('Grand Total :').any(axis=1)]
    df = df.dropna(how='all')
    df = df.fillna('N/A')

    # Drop columns by index
    columns_to_drop = [0, 5]
    df = df.drop(df.columns[columns_to_drop], axis=1)

    # Assign column names
    column_names = ['material no.', 'description', 'bin no.',
                    'val.stock', 'max.price', 'value', 'uom', 'storage loc']
    df.columns = column_names

    return df


def insert_documents_in_batches(documents):
    total_documents = len(documents)
    num_batches = total_documents // BATCH_SIZE
    remainder = total_documents % BATCH_SIZE

    for i in range(num_batches):
        start_idx = i * BATCH_SIZE
        end_idx = (i + 1) * BATCH_SIZE
        batch_documents = documents[start_idx:end_idx]
        collection.insert_many(batch_documents)

    if remainder > 0:
        batch_documents = documents[-remainder:]
        collection.insert_many(batch_documents)


@app.route('/')
def main():
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def clean_and_insert_data():

    try:
        # Get the uploaded file from the request
        file = request.files['fileToUpload']

        # Read the uploaded file
        if file.filename.endswith('.csv'):
            df = pd.read_csv(file)
        elif file.filename.endswith('.xlsx'):
            df = pd.read_excel(file)
        else:
            return "Unsupported file format. Only CSV and XLSX files are supported."

        df = pd.DataFrame(df)

        cleaned_df = clean_data_dataframe(df)

        # Convert the cleaned DataFrame to a list of dictionaries
        documents = cleaned_df.to_dict(orient='records')

        # Connect to MongoDB
        connect_to_mongodb()

        # Insert the documents into MongoDB in batches
        insert_documents_in_batches(documents)

        # Disconnect from MongoDB
        disconnect_from_mongodb()

        return "Data uploaded and inserted into MongoDB!"

    except:
        return "Something went wrong!"


if __name__ == '__main__':
    app.run(debug=False)
