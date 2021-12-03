import os

from LV_db_connection import GremlinClient
from dotenv import load_dotenv

def get_gerald_property_ids(client):
    """
    Returns a list of property IDs of properties under management
    Gerald.
    """
    query = "g.V().hasLabel('property').has('status','under management').values('id')"
    return client.submit(query)

def get_all_property_info_gerald(client, prop_id):
    """
    Returns a matching object to get_all_property_info(), containing all property
    information. Essentially used to compare the version just obtained from RPS to
    that which was already present in gerald.
    """
    query = f"""g.V().hasLabel('property').has('id', eq('{prop_id}'))
                .in('Owns','renting').hasLabel('landlord','tenant')
                .in('is a').dedup().tree()
                """
    res = client.submit(query)

    property = format_property_info(client, res)
    return property

def format_property_info(client, result):
    """
    Formats gerald output of all property info. The gremlin query
    returns the output as a tree with root as property, first level
    children as tenancy/landlord, second level children as contacts.
    This function parses this tree and returns more readable 
    nested representation similar to that returned by the API.
    """
    result = result[0].popitem()[1]
    property = client.parse_graph_json(result['key'])

    tenant = {}
    landlord = {}
    for item in result['value'].values():
        if item['key']['label'] == 'tenant':
            tenant = client.parse_graph_json(item['key'])

            tenant['contacts'] = []
            for contact in item['value'].values():
                con = client.parse_graph_json(contact['key'])
                tenant['contacts'].append(con)
            
        if item['key']['label'] == 'landlord':
            landlord = client.parse_graph_json(item['key'])

            landlord['contacts'] = []
            for contact in item['value'].values():
                con = client.parse_graph_json(contact['key'])
                landlord['contacts'].append(con)
    
    property["landlord"] = landlord
    property["tenancy"] = tenant

    return property

if __name__ == "__main__":
    load_dotenv()
    client = GremlinClient(os.environ['GERALD_USERNAME'], os.environ['GERALD_PWD'])
    print(get_gerald_properties(client))

