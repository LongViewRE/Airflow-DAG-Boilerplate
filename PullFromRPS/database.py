###############################################################################
# database.py                                                                 #
# Functions for retrieving data from Gerald                                   #
###############################################################################
import os

from LV_db_connection import GremlinClient
from dotenv import load_dotenv

def get_property_ids_gerald(gerald):
    """
    Returns a list of property IDs of properties under management
    Gerald.
    """
    query = "g.V().hasLabel('property').has('status','under management').values('id')"
    return gerald.submit(query)

def get_extended_property_info_gerald(gerald, prop_id):
    """
    Returns a matching object to get_extended_property_info(), containing
    extended property information. Essentially used to compare the version
    just obtained from RPS to that which was already present in gerald.

    ASSUMPTIONS: 
    properties have at most 1 landlord and at most 1 tenancy object.
    """
    query = f"""g.V().hasLabel('property').has('id', eq('{prop_id}'))
                .in('Owns','renting').hasLabel('landlord','tenant')
                .coalesce(__.in('is a'), constant('null')).dedup().tree()
                """
    res = gerald.submit(query)

    property = format_property_info(gerald, res)
    return property

def format_property_info(gerald, result):
    """
    Formats gerald output of all property info. The gremlin query
    returns the output as a tree with root as property, first level
    children as tenancy/landlord, second level children as contacts.
    This function parses this tree and returns more readable 
    nested representation similar to that returned by the API.
    """
    result = result[0].popitem()[1]
    property = gerald.parse_graph_json(result['key'])

    tenant = {}
    landlord = {}
    for item in result['value'].values():
        if item['key'] != 'null':
            if item['key']['label'] == 'tenant':
                tenant = gerald.parse_graph_json(item['key'])

                tenant['contacts'] = []
                for contact in item['value'].values():
                    if contact['key'] != 'null':
                        con = gerald.parse_graph_json(contact['key'])
                        tenant['contacts'].append(con)
                
            if item['key']['label'] == 'landlord':
                landlord = gerald.parse_graph_json(item['key'])

                landlord['contacts'] = []
                for contact in item['value'].values():
                    if contact['key'] != 'null':
                        con = gerald.parse_graph_json(contact['key'])
                        landlord['contacts'].append(con)
    
    property["landlord"] = landlord
    property["tenancy"] = tenant

    return property

def get_all_properties_gerald(gerald):
    """
    Returns current information in Gerald to sync.
    Result is a list of extended property objects, which include the property's
    landlord + contacts, and tenancy + contacts. 
    """
    prop_ids = get_property_ids_gerald(gerald)
    extended_properties = []

    for id in prop_ids:
        extended_property = get_extended_property_info_gerald(gerald, id)
        extended_properties.append(extended_property)
    
    return extended_properties


if __name__ == "__main__":
    load_dotenv()
    gerald = GremlinClient(os.environ['GERALD_USERNAME'], os.environ['GERALD_PWD'])

