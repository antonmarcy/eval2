import pandas as pd
from pymongo import MongoClient

csv_file = 'Medical_Equipment_Suppliers.csv'
df = pd.read_csv(csv_file)

print("Initial Data from CSV:")
print(df.head())

client = MongoClient("mongodb://localhost:27017/")
db = client['medical_equipment']
collection = db['suppliers']

data_dict = df.to_dict(orient='records')
collection.insert_many(data_dict)

print("Data inserted into MongoDB successfully.")

providers_in_CA = collection.find({"state": "CA"}, {"_id": 0, "provider_name": 1})
print("\nProviders in CA:")
for provider in providers_in_CA:
    print(provider['provider_name'])

ny_providers = collection.find({
    "state": "NY",
    "specialty": "Medical Supply Company Other",
    "supplies_list": {"$regex": "OXYGEN & EQUIPMENT"}
}, {"_id": 0, "provider_name": 1, "supplies_list": 1})

print("\nProviders in NY offering 'OXYGEN & EQUIPMENT':")
for provider in ny_providers:
    print(provider)

collection.update_one({"provider_ID": 20506619}, {"$set": {"phone": "123123123"}})
print("\nPhone number updated for provider_ID 20506619.")

tx_supplies = collection.distinct("supplies_list", {"state": "TX"})
print("\nUnique supplies offered by providers in TX:")
print(tx_supplies)

client.close()
