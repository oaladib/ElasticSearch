from elasticsearch import Elasticsearch

# Connect to Elasticsearch instance
es = Elasticsearch("http://localhost:9200")

# Check connection
if es.ping():
    print("Connected to Elasticsearch")
else:
    print("Could not connect to Elasticsearch")

# 1. Create an index
index_name = "library"
index_body = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 1
    },
    "mappings": {
        "properties": {
            "title": {"type": "text"},
            "author": {"type": "text"},
            "published_year": {"type": "integer"},
            "genre": {"type": "keyword"}
        }
    }
}

# Delete index if it exists
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)

# Create the new index
es.indices.create(index=index_name, body=index_body)
print(f"Index '{index_name}' created.")

# 2. Add documents to the index
books = [
    {"title": "The Great Gatsby", "author": "F. Scott Fitzgerald", "published_year": 1925, "genre": "fiction"},
    {"title": "To Kill a Mockingbird", "author": "Harper Lee", "published_year": 1960, "genre": "fiction"},
    {"title": "1984", "author": "George Orwell", "published_year": 1949, "genre": "dystopian"}
]

for i, book in enumerate(books):
    es.index(index=index_name, id=i+1, document=book)

print("Documents indexed.")

# 3. Search for a document
query = {
    "query": {
        "match": {
            "genre": "fiction"
        }
    }
}

response = es.search(index=index_name, body=query)
print("Search Results:")
for hit in response['hits']['hits']:
    print(hit["_source"])

# 4. Cleanup - delete the index
es.indices.delete(index=index_name)
print(f"Index '{index_name}' deleted.")