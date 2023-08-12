import pandas as pd
from flask import Flask, render_template, request
from pymongo import MongoClient
import threading

app = Flask(__name__)

BATCH_SIZE = 1000

mongoDB_string = 'mongodb+srv://mukul:YWb0vrGQI@sapstore.kz6z8ks.mongodb.net/test'


def connect_to_mongodb():
    global client, db, collection
    try:
        client = MongoClient(mongoDB_string)
        db = client['test']
        collection = db['mukulsheets']
        return client, collection
    except Exception as e:
        print(f"Error connecting to MongoDB: {str(e)}")
        return None, None


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


def disconnect_from_mongodb():
    global client
    client.close()


def upload_data_in_background(documents):
    # Connect to MongoDB
    connect_to_mongodb()

    # Insert the documents into MongoDB in batches
    insert_documents_in_batches(documents)

    # Disconnect from MongoDB
    disconnect_from_mongodb()


@app.route('/')
def main():
    return render_template('index.html')


@app.route('/testConnection')
def test_connection():
    try:
        a, b = connect_to_mongodb()
        if a is not None and b is not None:
            return "Connected to MongoDB successfully!"
        else:
            return "Failed to connect to MongoDB."
    except Exception as e:
        return f"Some error occurred while connecting to MongoDB: {str(e)}"


@app.route('/upload', methods=['POST'])
def clean_and_insert_data():
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

    # Start a new thread to upload data in the background
    upload_thread = threading.Thread(
        target=upload_data_in_background, args=(documents,))
    upload_thread.start()

    return "Data upload process started in the background!"


if __name__ == '__main__':
    app.run(debug=True)
