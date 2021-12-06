###############################################################################
# sync.py                                                                     #
# Functions for comparing RPS and Gerald data and pushing/syncing any changes #
# to Gerald                                                                   #
###############################################################################
import json
import time
import client
import database

from dotenv import load_dotenv
from functools import partial, reduce
from operator import itemgetter, iconcat


###############################################################################
# SYNC LOGIC                                                                  #
###############################################################################

def sync(rps, gerald):
    """
    Main function that syncs RPS and Gerald.
    """
    queries = []
    rps_props, gerald_props = retrieve_properties(rps, gerald)
    to_add, to_archive, to_compare = sync_partition(rps_props, gerald_props)

    queries += list(map(add_item, to_add))
    queries += list(map(remove_item, to_archive))
    queries += list(map(compare_item, to_compare))

    submit_all(gerald, queries)


def sync_partition(rps_props, gerald_props):
    """
    Calculates the updates needed to keep RPS and gerald in sync
    Returns a triple of to_add, to_archive, to_compare
    to_add: list of things to add to gerald
    to_archive: list of things to archive in gerald
    to_compare: list of 2-tuples (rps, gerald) which
                must be compared and synced if necessary
    """
    get_id = lambda p: p['id']
    rps_ids = list(map(get_id, rps_props))
    gerald_ids = list(map(get_id, gerald_props))

    # partitions the RPS items - if the ID is in the gerald list,
    # we must compare them further to check for changes. If not, it is a new
    # item to add to gerald
    to_compare_rps, to_add = partition(lambda p: p['id'] in gerald_ids,
                                        rps_props)

    # partitions the gerald items - if the ID is in the rps list,
    # we must compare them. If not, it must be archived
    to_compare_gerald, to_archive = partition(lambda p: p['id'] in rps_ids,
                                                gerald_props)

    # sort the items to compare such that they align
    to_compare_gerald.sort(key=get_id)
    to_compare_rps.sort(key=get_id)

    # zip them to obtain a list of tuples
    to_compare = list(zip(to_compare_rps, to_compare_gerald))

    return to_add, to_archive, to_compare

def add_item(rps_property):
    """
    Returns a list of queries to add the property + landlord +
    tenancy + contacts to gerald.
    """
    queries = []
    rps_landlord = rps_property.pop('landlord', None)
    rps_tenancy = rps_property.pop('tenancy', None)
    prop_id = rps_property['id']

    queries += add_property(rps_property)
    queries += add_landlord(rps_landlord, prop_id)
    queries += add_tenancy(rps_tenancy, prop_id)

    return queries


def compare_item(tuple):
    """
    Compares all RPS and gerald info for a property and 
    outputs a set of gremlin queries to sync the two.
    """
    queries = []

    rps_property = tuple[0]
    rps_landlord = rps_property.pop('landlord', None)
    rps_tenancy = rps_property.pop('tenancy', None)

    gerald_property = tuple[1]
    gerald_landlord = gerald_property.pop('landlord', None)
    gerald_tenancy = gerald_property.pop('tenancy', None)

    prop_id = rps_property['id']

    queries += compare_property(rps_property, gerald_property)
    queries += compare_landlord(rps_landlord, gerald_landlord, prop_id)
    queries += compare_tenancy(rps_tenancy, gerald_tenancy, prop_id)

    return queries
    
def compare_property(rps_property, gerald_property):
    """
    Compares the RPS and gerald attributes for the property node
    and outputs a list of queries to sync the two (if both are identical
    outputs an empty list).
    """
    attributes = []

    # Type is an attribute that may or may not come from RPS (alternatively 
    # comes from domain.com.au), and hence is only synced if non-empty. 
    if rps_property['type'] != '':
        if gerald_property['type'] != rps_property['type']:
            attributes.append(f".property('type', '{rps_property['type']}')")
    
    rps_property.pop('type', None)
    gerald_property.pop('type', None)
    
    for key,value in rps_property.items():
        if gerald_property[key] != value:
            attributes.append(f".property('{key}', '{value}')")
    
    if attributes != []:
        query = f"g.V('{rps_property['id']}')" + "".join(attributes)
        return [query]
    else:
        return []

def compare_landlord(rps_landlord, gerald_landlord, prop_id):
    """
    Compares the RPS and gerald info for landlords and outputs
    a list of queries to sync the two (if both identical, returns
    empty list).
    """
    queries = []

    if gerald_landlord['id'] != rps_landlord['id']:
        queries.append(add_landlord(rps_landlord, prop_id))
        queries.append(remove_landlord(gerald_landlord, prop_id))
    else:
        # TODO: contact processing
        to_add, to_archive, to_compare = sync()

    return queries

def compare_tenancy(rps_tenancy, gerald_tenancy, prop_id):
    """
    Compares the RPS and gerald info for tenancies and outputs a 
    list of queries to sync the two (if both identical, returns
    empty list).
    """ 
    attributes = []
    queries = []

    if gerald_tenancy['id'] != rps_tenancy['id']:
        queries.append(add_tenancy(rps_tenancy, prop_id))
        queries.append(remove_tenancy(gerald_tenancy, prop_id))
    else: 
        # TODO: contact processing
        rps_contacts = rps_tenancy.pop('contacts', None)
        gerald_contacts = gerald_tenancy.pop('contacts', None)

        for key, value in rps_tenancy.items():
            if gerald_tenancy[key] != value:
                attributes.append(f".property('{key}', '{value}')")
    
        if attributes != []:
            query = f"g.V('{gerald_tenancy['id']}')" + "".join(attributes)
            queries.append(query)

    return queries


def add_property(gerald, property):
    """
    Returns gremlin queries to add a property. Checks if the property exists
    as 'not under management'
    """
    search_query = f"g.V().has('id', within('{property['id']}'))"
    res = gerald.submit(search_query)

    if len(res) > 0:
        query = f"g.V('{property['id']}')"
    else: 
        query = f"g.addV('property').property('id','{property['id']}')"

    property.pop('id', None)
    for key,value in property.items():
        query += f".property('{key}','{value}')"
    
    query += f".property('Last Updated', {time.time()})"
    
    return [query]

def add_landlord(gerald, landlord, prop_id):
    """
    Returns gremlin queries to add a landlord + contacts
    (connected to given property). Checks if the landlord already exists.
    """
    vertices = []
    edges = []

    search_query = f"g.V().has('id', within('{landlord['id']}'))"
    res = gerald.submit(search_query)

    if len(res) > 0:
        vquery = f"g.V('{landlord['id']}')"
    else:
        vquery = f"g.addV('landlord').property('id','{landlord['id']}')"
    
    contacts = landlord.pop('contacts', None)
    ll_id = landlord.pop('id', None)
    for key,value in landlord.items():
        vquery += f".property('{key}','{value}')"
    
    vquery += f".property('Last Updated', {time.time()})"
    
    equery = f"""g.V('{ll_id}')
            .coalesce(
                __.outE('Owns').inV().has('id', eq('{prop_id}')), 
                __.addE('Owns').to(g.V('{prop_id}')))"""
    
    vertices.append(vquery)
    edges.append(equery)

    # Get all the vertices/edges for the contacts
    queries = map(partial(add_contact, gerald, 'landlord', ll_id), contacts)
    vertices += reduce(iconcat, map(itemgetter(0), queries), [])
    edges += reduce(iconcat, map(itemgetter(1), queries), [])

    return vertices, edges

def add_tenancy(gerald, tenancy, prop_id):
    """
    Returns gremlin queries to add a tenancy + contacts (connected to
    given property). Checks if the tenancy already exists.
    """
    vertices = []
    edges = []
    
    search_query = f"g.V().has('id', within('{tenancy['id']}'))"
    res = gerald.submit(search_query)

    if len(res) > 0:
        vquery = f"g.V('{tenancy['id']}')"
    else: 
        vquery = f"g.addV('tenant').property('id','{tenancy['id']}')"
    
    contacts = tenancy.pop('contacts', None)
    tt_id = tenancy.pop('id', None)
    for key,value in tenancy.items():
        vquery += f".property('{key}', '{value}')"
    
    vquery += f".property('Last Updated', {time.time()})"

    equery = f"""g.V('{tt_id}')
            .coalesce(
                __.outE('renting'),
                __.addE('renting').to(g.V('{prop_id}')))"""

    vertices.append(vquery)
    edges.append(equery)

    # Get all the vertices/edges for the contacts
    queries = map(partial(add_contact, gerald, 'tenant', tt_id), contacts)
    vertices += reduce(iconcat, map(itemgetter(0), queries), [])
    edges += reduce(iconcat, map(itemgetter(1), queries), [])

    return vertices, edges

def add_contact(gerald, type: str, parent_id, contact):
    """
    Returns gremlin queries to add a contact (connected to given parent
    node). Checks if contact already exists.
    """


def check_contact(gerald, contact):
    """
    Checks whether a contact exists in gerald.
    Returns the output of the query.
    """
    
    
            
###############################################################################
# HELPER FUNCTIONS                                                            #
###############################################################################
def retrieve_properties(rps, gerald):
    """
    Retrieves a list of all property info from RPS and Gerald
    """
    rps_props = client.get_all_properties_rps(rps)
    rps_props = list(map(align_format_rps, rps_props))
    gerald_props = database.get_all_properties_gerald(gerald)
    gerald_props = list(map(align_format_gerald, gerald_props))

    return rps_props, gerald_props

def partition(predicate, iterable):
    """
    Partitions a list into two based on a predicate
    """
    true = []
    false = []

    for item in iterable:
        if predicate(item):
            true.append(item)
        else:
            false.append(item)
    
    return true, false

###############################################################################
# FORMATTING FUNCTIONS                                                        #
###############################################################################
def align_format_gerald(gerald_property):
    """
    Removes some unnecessary fields from gerald property output 
    (e.g ACC fields, last updated, etc.)
    """
    gerald_property.pop('Last Updated', None)
    gerald_property.pop('acID', None)
    gerald_property.pop('acStatus', None)
    gerald_property.pop('last appraisal date', None)
    
    if len(gerald_property['landlord']) > 0:
        gerald_property['landlord']['contacts'] = list(map(align_contact_gerald,
            gerald_property['landlord']['contacts']))
    
    if len(gerald_property['tenancy']) > 0:
        gerald_property['tenancy'].pop('Last Updated', None)
        gerald_property['tenancy']['contacts'] = list(map(align_contact_gerald,
            gerald_property['tenancy']['contacts']))

    return gerald_property

def align_contact_gerald(gerald_contact):
    """
    Removes some unnecessary fields from gerald contact output
    (e.g last updated, acID)
    """
    gerald_contact.pop('Last Updated', None)
    gerald_contact.pop('acID', None)

    return gerald_contact

def align_format_rps(rps_property):
    """
    Makes the rps property format/schema consistent with that of gerald.
    """
    rps_property['id'] = "pp-"+rps_property.pop('ID')
    rps_property['housenumber'] = str(rps_property.pop('HouseNumber', ''))
    rps_property['address1'] = str(rps_property.pop('Address1'))
    rps_property['address3'] = str(rps_property.pop('Address3'))
    rps_property['address4'] = str(rps_property.pop('Address4', ''))
    rps_property['postcode'] = str(rps_property.pop('Postcode', ''))
    rps_property['pm'] = str(rps_property['pm']).replace("'","")
    rps_property['system'] = "RPS"
    rps_property['status'] = "under management"
    rps_property['amf'] = str(rps_property['amf'])
    rps_property['latitude'] = str(rps_property.pop('Latitude', ''))
    rps_property['longitude'] = str(rps_property.pop('Longitude', ''))
    rps_property['type'] = str(rps_property.pop('Type', ''))
    
    rps_property.pop('Status', None)
    rps_property.pop('InternalLettingStatus', None)

    if len(rps_property['landlord']) == 0:
        rps_property['landlord'] = {}
    else:
        rps_property['landlord']['id'] = "ll-"+ \
            rps_property['landlord'].pop('reapitID')
        rps_property['landlord']['system'] = "RPS"
        
        rps_property['landlord']['contacts'] = list(map(align_contact_rps, 
            rps_property['landlord']['contacts']))

    if len(rps_property['tenancy']) == 0:
        rps_property['tenancy'] = {}
    else:
        rps_property['tenancy']['id'] = "tt-"+ rps_property['tenancy'].pop('reapitID')
        rps_property['tenancy']['periodic'] \
            = "True" if rps_property['tenancy']['periodic'] else "False"
        rps_property['tenancy']['endConfirmed'] \
            = "True" if rps_property['tenancy']['endConfirmed'] else "False"

        rps_property['tenancy']['contacts'] = list(map(align_contact_rps,
            rps_property['tenancy']['contacts']))
    
    return rps_property


def align_contact_rps(rps_contact):
    """
    Makes the rps contact format/schema consistent with that of gerald.
    """
    rps_contact['system'] = 'RPS'
    if rps_contact['type'] == 'contact':
        rps_contact['id'] = "cc-"+rps_contact.pop('contactID')
    elif rps_contact['type'] == 'Company':
        rps_contact['id'] = "cmp-"+rps_contact.pop('contactID')
    rps_contact.pop('type', None)

    return rps_contact











if __name__ == "__main__":
    load_dotenv()
    rps = RPSClient(os.environ['rpskey'])
    gerald = GremlinClient(os.environ['GERALD_USERNAME'], os.environ['GERALD_PWD'])
    #rps_props, gerald_props = sync(rps, gerald)
    gerald_props = get_all_properties_gerald(gerald)
    #with open('rps.json', 'w') as f:
    #    json.dump(rps_props, f, indent=4)
    
    with open('gerald.json', 'w') as f:
        json.dump(gerald_props, f, indent=4)

