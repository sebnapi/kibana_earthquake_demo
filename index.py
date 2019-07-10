import json
import csv
import elasticsearch.helpers

#OrderedDict([('DateTime', '2016/01/01 00:30:04.91'),
# ('Latitude', '18.0772'), ('Longitude', '-67.1027'),
# ('Depth', '19.91'), ('Magnitude', '2.80'),
# ('MagType', 'Md'), ('NbStations', ''), ('Gap', '125'),
# ('Distance', '0'), ('RMS', '0.44'), ('Source', 'pr'),
# ('EventID', '201601012001')])


def indexableEQEvents():
    with open('earthquakes.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            lat, lon = row["Latitude"], row["Longitude"]
            del row["Latitude"]
            del row["Longitude"]
            row["location"] = {
                "lat": lat,
                "lon": lon
            }
            yield row


def bulkDocs(index):
    for event in indexableEQEvents():
        addCmd = {"_index": index,
                  "_source": event}
        print("Eq %s added to %s" % (event['DateTime'], index))
        yield addCmd


def index(es, schema, index='earthquakes'):
    es.indices.delete(index, ignore=[400, 404])
    es.indices.create(index, body=schema)

    elasticsearch.helpers.bulk(es, bulkDocs(index), chunk_size=1000)


if __name__ == "__main__":
    from elasticsearch import Elasticsearch
    from sys import argv

    with open("credentials.json") as crdFile:
        credentials = json.load(crdFile)

    esUrl='https://%s:%s@%s' % (credentials["username"], credentials["password"], credentials["host"])
    schema_filename = 'index_schema.json'

    if len(argv) > 1:
        schema_filename = argv[1]
    es = Elasticsearch(esUrl, timeout=30)

    with open(schema_filename) as schemaFile:
        schema = json.load(schemaFile)

    index(es, schema, index='earthquakes')

