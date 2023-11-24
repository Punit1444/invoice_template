import generateInvoice
import os
import subprocess
import time


class MongoDBManager:
    def __init__(self, url):
        self.client = None
        self.url = url

    def __enter__(self):
        self.client = MongoClient(self.url, tls=True, tlsCAFile=certifi.where())
        self.client.admin.command('ismaster')
        print("MongoDB connection is successful.")
        return self.client

    def __exit__(self, exc_type, exc_val, exc_tb): 
        if self.client:
            self.client.close()
            print("MongoDB connection is closed.")


def main():
    url = "mongodb://doadmin:8eqby75zk2Ng1634@db-mongodb-blr1-91426-e07cde9a.mongo.ondigitalocean.com:27017/?authSource=admin&tls=true"
    with MongoDBManager(url) as client:
        db = client['gst_portal']
        collection = db['einvoice_irn_raw']
        documents = collection.find({})

        for doc in documents:
            if 'url' not in doc:
                # Execute your .py and .html files
                subprocess.run(["python", "generateInvoice.py"])  # Replace with your script name

schedule.every().day.at("17:20").do(main)  # Adjust the time as needed

while True:
    schedule.run_pending()
    time.sleep(1)

if __name__ == "__main__":
    main()