from pymongo.mongo_client import MongoClient
uri = "mongodb+srv://Sreejan:IAS1234IAS@cluster0.q1cmmsz.mongodb.net/?retryWrites=true&w=majority"
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