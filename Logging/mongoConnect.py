from pymongo.mongo_client import MongoClient
uri = "mongodb+srv://se33:se33@se3.mrwhfdo.mongodb.net/?retryWrites=true&w=majority&appName=Se3"
# Create a new client and connect to the server
client = MongoClient(uri)
# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
    print(client.list_database_names())
    print(client.LoggerDB.list_collection_names())
except Exception as e:
    print(e)