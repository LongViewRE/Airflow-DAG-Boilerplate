###############################################################################
# sync.py                                                                     #
# Functions for comparing RPS and Gerald data and pushing/syncing any changes #
# to Gerald                                                                   #
###############################################################################
import os
import json
import time
import client
import database
import random

from copy import deepcopy
from dotenv import load_dotenv
from functools import partial, reduce

from LV_db_connection import GremlinClient
from LV_external_services import RPSClient

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
    queries += list(map(archive_item, to_archive))
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


###############################################################################
# VERTEX/EDGE COMPARISON FUNCTIONS
###############################################################################
def compare_item(tuple):
    """
    Compares all RPS and gerald info for a property and 
    outputs a set of gremlin queries to sync the two.
    """
    t = deepcopy(tuple)
    queries = []

    rps_property = t[0]
    rps_landlord = rps_property.pop('landlord', None)
    rps_tenancy = rps_property.pop('tenancy', None)

    gerald_property = t[1]
    gerald_landlord = gerald_property.pop('landlord', None)
    gerald_tenancy = gerald_property.pop('tenancy', None)

    prop_id = rps_property['id']

    queries.append(compare_property(rps_property, gerald_property))
    queries.append(compare_landlord(rps_landlord, gerald_landlord, prop_id))
    queries.append(compare_tenancy(rps_tenancy, gerald_tenancy, prop_id))

    queries = reduce(flatten, queries, {"vertices": [], "edges": []})
    return queries
    
def compare_property(rps_prop, gerald_prop):
    """
    Compares the RPS and gerald attributes for the property node
    and outputs a list of queries to sync the two (if both are identical
    outputs an empty list).
    """
    rps_property = deepcopy(rps_prop)
    gerald_property = deepcopy(gerald_prop)

    queries = []

    # Type is an attribute that may or may not come from RPS (alternatively 
    # comes from domain.com.au), and hence is only synced if non-empty. 
    if rps_property['type'] != '':
        if gerald_property['type'] != rps_property['type']:
            q = (   f"g.V('{rps_property['id']}')"
                    f".property('type', '{rps_property['type']}')")
            queries.append({"vertices": [q], "edges": []})
    rps_property.pop('type', None)
    gerald_property.pop('type', None)
    
    # Check if any differences exist
    update_required = False
    for key,value in rps_property.items():
        if gerald_property[key] != value:
            update_required = True
    
    # If they do, just call the add_property function, as it checks
    # for existing properties regardless
    if update_required:
        queries.append(add_property(gerald, rps_prop))
    
    queries = reduce(flatten, queries, {"vertices": [], "edges": []})
    return queries

def compare_landlord(gerald, rps_ll, gerald_ll, prop_id):
    """
    Compares the RPS and gerald info for landlords and outputs
    a list of queries to sync the two (if both identical, returns
    empty list).
    """
    rps_landlord = deepcopy(rps_ll)
    gerald_landlord = deepcopy(gerald_ll)
    
    queries = []

    if gerald_landlord['id'] != rps_landlord['id']:
        # We have a new landlord
        queries.append(add_landlord(rps_landlord, prop_id))
        queries.append(replace_edge(prop_id, "Owns", "Owned", gerald_landlord))
    else:
        # LANDLORD CONTACT PROCESSING
        ll_id = gerald_landlord['id']
        rps_cons = rps_landlord.pop('contacts', None)
        gerald_cons = gerald_landlord.pop('contacts', None)

        to_add, to_archive, to_compare \
            = sync_partition(rps_cons, gerald_cons)
        # Contacts to add to landlord
        queries += list(map(partial(add_contact, gerald, ll_id), to_add))
        # Contacts to remove to landlord
        queries += list(map(partial(replace_edge(ll_id, "is a", "was a"), to_archive)))
        # Contacts to compare
        queries += list(map(partial(compare_contact, gerald, ll_id), to_compare))

    queries = reduce(flatten, queries, {"vertices": [], "edges": []})
    return queries

def compare_tenancy(rps_ten, gerald_ten, prop_id):
    """
    Compares the RPS and gerald info for tenancies and outputs a 
    list of queries to sync the two (if both identical, returns
    empty lists).
    """ 
    rps_tenancy = deepcopy(rps_ten)
    gerald_tenancy = deepcopy(gerald_ten)

    queries = []

    if gerald_tenancy['id'] != rps_tenancy['id']:
        queries.append(add_tenancy(rps_tenancy, prop_id))
        queries.append(replace_edge(prop_id, "renting", "vacated", gerald_tenancy))
    else:
        # TENANCY CONTACT PROCESSING
        tt_id = gerald_tenancy['id']
        rps_cons = rps_tenancy.pop('contacts', None)
        gerald_cons = gerald_tenancy.pop('contacts', None)
        
        # TODO: this cannot work just based on ID! must use contact comparison
        #to_add, to_archive, to_compare = sync_partition(rps_cons, gerald_cons)


        # Contacts to add to landlord
        queries += list(map(partial(add_contact, gerald, tt_id), to_add))
        # Contacts to remove to landlord
        queries += list(map(partial(replace_edge(tt_id, "is a", "was a"), to_archive)))
        # Contacts to compare
        queries += list(map(partial(compare_contact, gerald, tt_id), to_compare))

        # TENANCY ATTRIBUTES PROCESSING
        # Check if any differences exist
        update_required = False
        for key,value in rps_tenancy.items():
            if gerald_tenancy[key] != value:
                update_required = True
        
        # If they do, just call the add_property function, as it checks
        # for existing properties regardless
        if update_required:
            queries.append(add_property(gerald, rps_tenancy))
    
    queries = reduce(flatten, queries, {"vertices": [], "edges": []})
    return queries

def compare_contact(rps_con, gerald_con, parent_id):
    """
    Compares the RPS and gerald info for contacts and outputs a list of 
    queries to sync the two (if both identical, returns empty lists).
    """
    rps_contact = deepcopy(rps_con)
    gerald_contact = deepcopy(gerald_con)

    queries = []
    type = "Company" if rps_contact["id"][:3] == "cmp" else "contact"


###############################################################################
# VERTEX/EDGE CREATION FUNCTIONS                                              #
###############################################################################
def add_item(gerald, rps_prop):
    """
    Returns a list of queries to add the property + landlord +
    tenancy + contacts to gerald.
    """
    rps_property = deepcopy(rps_prop)
    queries = []

    rps_landlord = rps_property.pop('landlord', None)
    rps_tenancy = rps_property.pop('tenancy', None)
    prop_id = rps_property['id']

    queries.append(add_property(gerald, rps_property))

    if len(rps_landlord) > 0:
        queries.append(add_landlord(gerald, rps_landlord, prop_id))
    if len(rps_tenancy) > 0:
        queries.append(add_tenancy(gerald, rps_tenancy, prop_id))
    
    queries = reduce(flatten, queries, {"vertices": [], "edges": []})
    return queries

def add_property(gerald, prop):
    """
    Returns gremlin queries to add a property. Checks if the property exists
    as 'not under management'
    """
    property = deepcopy(prop)
    queries = []

    search_query = f"g.V('{property['id']}')"
    res = gerald.submit(search_query)

    if len(res) > 0:
        vquery = f"g.V('{property['id']}')"
    else: 
        vquery = f"g.addV('property').property('id','{property['id']}')"

    property.pop('id', None)
    for key,value in property.items():
        vquery += f".property('{key}','{value}')"
    
    vquery += f".property('Last Updated', {int(time.time())})"
    queries.append({"vertices": [vquery], "edges": []})

    queries = reduce(flatten, queries, {"vertices": [], "edges": []})
    return queries

def add_landlord(gerald, ll, prop_id):
    """
    Returns gremlin queries to add a landlord + contacts
    (connected to given property). Checks if the landlord already exists.
    """
    landlord = deepcopy(ll)
    queries = []

    search_query = f"g.V('{landlord['id']}')"
    res = gerald.submit(search_query)

    if len(res) > 0:
        vquery = f"g.V('{landlord['id']}')"
    else:
        vquery = f"g.addV('landlord').property('id','{landlord['id']}')"
    
    contacts = landlord.pop('contacts', None)
    ll_id = landlord.pop('id', None)
    for key,value in landlord.items():
        vquery += f".property('{key}','{value}')"
    
    vquery += f".property('Last Updated', {int(time.time())})"
    
    equery = (  f"g.V('{ll_id}')"
                f".coalesce("
                f"__.outE('Owns').inV().has('id', eq('{prop_id}')),"
                f"__.addE('Owns').to(g.V('{prop_id}')))")
    
    queries.append({"vertices": [vquery], "edges": [equery]})

    # Get all the vertices/edges for the contacts
    queries += list(map(partial(add_contact, gerald, ll_id), contacts))

    queries = reduce(flatten, queries, {"vertices": [], "edges": []})
    return queries

def add_tenancy(gerald, ten, prop_id):
    """
    Returns gremlin queries to add a tenancy + contacts (connected to
    given property). Checks if the tenancy already exists.
    """
    tenancy = deepcopy(ten)
    queries = []
    
    search_query = f"g.V('{tenancy['id']}')"
    res = gerald.submit(search_query)

    if len(res) > 0:
        vquery = f"g.V('{tenancy['id']}')"
    else: 
        vquery = f"g.addV('tenant').property('id','{tenancy['id']}')"
    
    contacts = tenancy.pop('contacts', None)
    tt_id = tenancy.pop('id', None)
    for key,value in tenancy.items():
        vquery += f".property('{key}', '{value}')"
    
    vquery += f".property('Last Updated', {int(time.time())})"

    equery = (  f"g.V('{tt_id}')"
                f".coalesce("
                f"__.outE('renting'),"
                f"__.addE('renting').to(g.V('{prop_id}')))")

    queries.append({"vertices": [vquery], "edges": [equery]})

    # Get all the vertices/edges for the contacts
    queries += list(map(partial(add_contact, gerald, tt_id), contacts))
    
    queries = reduce(flatten, queries, {"vertices": [], "edges": []})
    return queries


def add_contact(gerald, parent_id, con):
    """
    Returns gremlin queries to add a contact (connected to given parent
    node). Checks if contact already exists.
    """
    contact = deepcopy(con)
    queries = []

    type = "Company" if contact["id"][:3] == "cmp" else "contact"

    # Check if contact already exists
    matches = check_contact(gerald, contact)
    if len(matches) > 0:
        # we have an existing contact

        # get the first match
        existingCon = gerald.parse_graph_json(matches[0])
        cc_id = existingCon['id']

        # TODO: what if there are multiple matches?

        # process email: if existing contact has a different email,
        # add this as an alternate. If existing contact has a placeholder
        # email, add this as primary email
        email = contact.pop("email", None)

        if "noemail" not in email:
            if email != existingCon["email"]:
                if "noemail" in existingCon["email"]:
                    vquery = f"g.V('{cc_id}').property('email','{email}')"
                else: 
                    vquery = (  f"g.V('{cc_id}').coalesce("
                                f"__.has('altEmail','{email}')"
                                f",__.property(list,'altEmail','{email}')")
                queries.append({"vertices": [vquery], "edges": []})
        
        vquery = f"g.V('{cc_id}')"
    else:
        # A completely new contact
        cc_id = contact.pop('id', None)
        vquery = f"g.addV('{type}').property('id', '{cc_id}')"

    
    for key, value in contact.items():
        vquery += f".property('{key}', '{value}')"

    vquery += f".property('Last Updated', {int(time.time())})"

    equery = (  f"g.V('{cc_id}')"
                f".coalesce("
                f"__.outE('is a').to(g.V('{parent_id}')),"
                f"__.addE('is a').to(g.V('{parent_id}')))")
    
    queries.append({"vertices": [vquery], "edges": [equery]})

    queries = reduce(flatten, queries, {"vertices": [], "edges": []})
    return queries

def check_contact(gerald, contact):
    """
    Checks whether a contact exists in gerald by trying to fetch
    any potential matches. Returns these matches (query output).
    """

    type = "Company" if contact["id"][:3] == "cmp" else "contact"
    email = contact["email"]
    mobile = contact["mobile"]
    query = "g.V()"

    # Contacts are considered identical if they match on first name,
    # last name, and at least one of (email, mobile, address)
    if type == "contact":
        if len(contact['first name']) > 0:
            query += f".has('first name', '{contact['first name']}')"
        if len(contact['last name']) > 0:
            query += f".has('last name', '{contact['first name']}')"
    else:
        # Companies are considered identical if they match on company
        # name and at least one of (email, mobile, address)
        query += f".has('Company Name', '{contact['Company Name']}')"
    
    query += ".or(" 
    if "noemail" not in email:
        query += f"__.has('email', '{email}')"
        query += f",__.has('altEmail', '{email}')"
    if len(mobile) > 0:
        query += f",__.has('mobile', '{mobile}')"
    if len(contact["address"]) > 0:
        query += f",__.has('address', '{contact['address']}')"
    
    return gerald.submit(query)
    
###############################################################################
# VERTEX/EDGE ARCHIVAL/REMOVAL FUNCTIONS                                      #
###############################################################################
def archive_item(rps_prop):
    """
    Sets the status of a property to 'not under management', effectively
    archiving it. Returns the query to do so.
    """
    query = (   f"g.V('{rps_prop['id']}')"
                f".property('status','not under management')"
                f".property('Last Updated', {int(time.time())})")
    return {"vertices": [query], "edges": []}

def replace_edge(parent_id, old_label, new_label, node):
    """
    Removes an edge from node to parent given the old label, and replacing
    it with a new edge. Returns the corresponding query.
    """
    equery = (  f"g.V('{node['id']}').property('Last Updated', {int(time.time())})"
                f".outE('{old_label}').as('e1')"
                f".where(inV().has('id','{parent_id}'))"
                f".V('{node['id']}')"
                f".addE('{new_label}').to(g.V('{parent_id}'))"
                f".property('Last Updated', {int(time.time())})"
                f".select('e1').drop()")
    return {"vertices": [], "edges": [equery]}

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

def flatten(d1, d2):
    """
    Merges dictionaries d2 into d1 by concatenating the values,
    assuming the keys are the same.
    """
    for key in d1.keys():
        d1[key] += d2[key]
    
    return d1

def contact_comparison(c1, c2):
    """
    Returns true if the contacts c1 and c2 represent the same person/entity,
    otherwise returns false. If c1, c2 both contacts, check if their names
    match. If c1, c2 both companies, check if company name matches. Further
    one of (email, mobile, address) must be the same. 
    """
    identical = True
    c1_type = "Company" if c1["id"][:3] == "cmp" else "contact"
    c2_type = "Company" if c2["id"][:3] == "cmp" else "contact"
    if (c1_type != c2_type):
        return False
    if c1_type == "Company" and c1['Company Name'] != c2['Company Name']:
        return False
    if c1_type == "contact" and (c1['first name'] != c2['first name'] or
        c1['last name'] != c2['last name']):
        return False
    if 'address' in c1.keys() and 'address' in c2.keys():
        pass
        #TODO: finish this function
        
    
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
    rps_property['address1'] = str(rps_property.pop('Address1')).replace("'", "")
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
    
    if 'first name' in rps_contact:
        if "'" in rps_contact['first name']:
            rps_contact['first name'] = rps_contact['first name'].replace("'", "")
    else:
        rps_contact['first name'] = ''
    if 'last name' in rps_contact:
        if "'" in rps_contact['last name']:
            rps_contact['last name'] = rps_contact['last name'].replace("'", "")
    else:
        rps_contact['last name'] = ''
    if 'mobile' not in rps_contact:
        rps_contact['mobile'] = ''
    if 'address' not in rps_contact:
        rps_contact['address'] = ''
    else:
        if "'" in rps_contact['address']:
            rps_contact['address'] = rps_contact['address'].replace("'", "")
    if 'email' not in rps_contact:
        rps_contact['email'] = "noemail." + str(random.randint(0, 10000000)) + "@nocontact.com.au"
    if 'Company Name' in rps_contact:
        if "'" in rps_contact['Company Name']:
            rps_contact['Company Name'] = rps_contact['Company Name'].replace("'", "")
    
    return rps_contact

if __name__ == "__main__":
    load_dotenv()
    rps = RPSClient(os.environ['rpskey'])
    gerald = GremlinClient(os.environ['GERALD_USERNAME'], os.environ['GERALD_PWD'])
    #rps_props, gerald_props = sync(rps, gerald)
    #gerald_props = get_all_properties_gerald(gerald)
    #with open('rps.json', 'w') as f:
    #    json.dump(rps_props, f, indent=4)
    
    #with open('gerald.json', 'w') as f:
    #    json.dump(gerald_props, f, indent=4)
    with open('gerald.json') as f:
        gerald_props = json.load(f)
        gerald_props = list(map(align_format_gerald, gerald_props))

    with open('rps.json') as f:
        rps_props = json.load(f)
        rps_props = list(rps_props.values())
        rps_props = list(map(align_format_rps, rps_props))

    to_add, to_archive, to_compare = sync_partition(rps_props, gerald_props)
