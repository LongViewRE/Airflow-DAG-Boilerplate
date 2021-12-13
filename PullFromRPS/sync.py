###############################################################################
# sync.py                                                                     #
# Functions for comparing RPS and Gerald data and syncing any changes         #
# to Gerald                                                                   #
###############################################################################
import os
import json


from copy import deepcopy
from dotenv import load_dotenv
from functools import partial, reduce
from itertools import product
from operator import itemgetter

from LV_db_connection import GremlinClient
from LV_external_services import RPSClient

from utils import partition, retrieve_properties, flatten, contact_comparison, \
                    align_format_gerald, align_format_rps, diff
from database import submit_all

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

    queries += list(map(partial(add_item, gerald), to_add))
    queries += list(map(archive_item, to_archive))
    queries += list(map(partial(compare_item, gerald), to_compare))

    queries = reduce(flatten, queries, {"vertices": [], "edges": []})
    return queries
    #submit_all(gerald, queries)


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
def compare_item(gerald, tuple):
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

    queries.append(compare_property(gerald, rps_property, gerald_property))
    queries.append(compare_landlord(gerald, rps_landlord, gerald_landlord, prop_id))
    queries.append(compare_tenancy(gerald, rps_tenancy, gerald_tenancy, prop_id))

    queries = reduce(flatten, queries, {"vertices": [], "edges": []})
    return queries
    
def compare_property(gerald, rps_prop, gerald_prop):
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
        if key not in gerald_property.keys() or gerald_property[key] != value:
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
    
    if gerald_landlord is None or len(gerald_landlord) == 0:
        if rps_landlord is None or len(rps_landlord) == 0:
            return {"vertices": [], "edges": []}
        else:
            # We have a new landlord
            queries.append(add_landlord(gerald, rps_landlord, prop_id))
    elif rps_landlord is None or len(rps_landlord) == 0:
        # We no longer have a landlord
        queries.append(replace_edge(prop_id, "Owns", "Owned", gerald_landlord))
    elif gerald_landlord['id'] != rps_landlord['id']:
        # We have replaced the landlord
        queries.append(add_landlord(gerald, rps_landlord, prop_id))
        queries.append(replace_edge(prop_id, "Owns", "Owned", gerald_landlord))
    else:
        # We have the same landlord
        # LANDLORD CONTACT PROCESSING
        ll_id = gerald_landlord['id']
        rps_cons = rps_landlord.pop('contacts', None)
        gerald_cons = gerald_landlord.pop('contacts', None)

        # partition the rps and gerald contacts
        pairs = product(rps_cons, gerald_cons)
        to_compare = list(filter(contact_comparison, pairs))
        to_add = diff(rps_cons, list(map(itemgetter(0), to_compare)))
        to_archive = diff(gerald_cons, list(map(itemgetter(1), to_compare)))

        # Contacts to add to landlord
        queries += list(map(partial(add_contact, gerald, ll_id), to_add))
        # Contacts to remove to landlord
        queries += list(map(partial(replace_edge, ll_id, "is a", "was a"), to_archive))
        # Contacts to compare
        queries += list(map(partial(compare_contact, gerald, ll_id), to_compare))

    queries = reduce(flatten, queries, {"vertices": [], "edges": []})
    return queries

def compare_tenancy(gerald, rps_ten, gerald_ten, prop_id):
    """
    Compares the RPS and gerald info for tenancies and outputs a 
    list of queries to sync the two (if both identical, returns
    empty lists).
    """ 
    rps_tenancy = deepcopy(rps_ten)
    gerald_tenancy = deepcopy(gerald_ten)

    queries = []

    if gerald_tenancy is None or len(gerald_tenancy) == 0:
        if rps_tenancy is None or len(rps_tenancy) == 0:
            return {"vertices": [], "edges": []}
        else:
            # We have a new tenant
            queries.append(add_tenancy(gerald, rps_tenancy, prop_id))
    elif rps_tenancy is None or len(rps_tenancy) == 0:
        # We no longer have a tenant
        queries.append(replace_edge(prop_id, "renting", "vacated", gerald_tenancy))
    elif gerald_tenancy['id'] != rps_tenancy['id']:
        # We have replaced the tenant
        queries.append(add_tenancy(gerald, rps_tenancy, prop_id))
        queries.append(replace_edge(prop_id, "renting", "vacated", gerald_tenancy))
    else:
        # We have the same tenant
        # TENANCY CONTACT PROCESSING
        tt_id = gerald_tenancy['id']
        rps_cons = rps_tenancy.pop('contacts', None)
        gerald_cons = gerald_tenancy.pop('contacts', None)

        # partition the rps and gerald contacts
        pairs = product(rps_cons, gerald_cons)
        to_compare = list(filter(contact_comparison, pairs))
        to_add = diff(rps_cons, list(map(itemgetter(0), to_compare)))
        to_archive = diff(gerald_cons, list(map(itemgetter(1), to_compare)))

        # Contacts to add to tenancy
        queries += list(map(partial(add_contact, gerald, tt_id), to_add))
        # Contacts to remove from tenancy
        queries += list(map(partial(replace_edge, tt_id, "is a", "was a"), to_archive))
        # Contacts to compare
        queries += list(map(partial(compare_contact, gerald, tt_id), to_compare))

        # TENANCY ATTRIBUTES PROCESSING
        # Check if any differences exist
        update_required = False
        for key,value in rps_tenancy.items():
            if key not in gerald_tenancy.keys() or gerald_tenancy[key] != value:
                update_required = True
        
        # If they do, just call the add_property function, as it checks
        # for existing properties regardless
        if update_required:
            queries.append(add_property(gerald, rps_tenancy))
    
    queries = reduce(flatten, queries, {"vertices": [], "edges": []})
    return queries

def compare_contact(gerald, parent_id, tuple):
    """
    Compares the RPS and gerald info for contacts and outputs a list of 
    queries to sync the two (if both identical, returns empty lists).
    """
    rps_con = tuple[0]
    gerald_con = tuple[1]
    rps_contact = deepcopy(rps_con)
    gerald_contact = deepcopy(gerald_con)

    queries = []

    # Check if any differences exist
    update_required = False
    for key,value in rps_contact.items():
        if key not in gerald_contact.keys() or gerald_contact[key] != value:
            update_required = True
    
    # If they do, just call the add_property function, as it checks
    # for existing properties regardless
    if update_required:
        queries.append(add_contact(gerald, parent_id, rps_contact))
    
    queries = reduce(flatten, queries, {"vertices": [], "edges": []})
    return queries


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
