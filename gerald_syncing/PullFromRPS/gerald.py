###############################################################################
# gerald.py                                                                   #
# Functions for pushing data to Gerald                                        #
###############################################################################
import time
import logging

from functools import reduce
from copy import deepcopy

from gerald_syncing.PullFromRPS.utils import flatten

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
    rps_pm = rps_property.pop('property_manager', None)
    prop_id = rps_property['id']

    queries.append(add_property(gerald, rps_property))
    queries.append(add_pm(gerald, rps_pm, prop_id))

    if len(rps_landlord) > 0:
        queries.append(add_landlord(gerald, rps_landlord, prop_id))
    if len(rps_tenancy) > 0:
        queries.append(add_tenancy(gerald, rps_tenancy, prop_id))
    
    queries = reduce(flatten, queries, {"vertices": [], "edges": []})
    queries["id"] = prop_id
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
        property.pop('system', None) # system is a partition key so is immutable
    else: 
        vquery = f"g.addV('property').property('id','{property['id']}')"

    property.pop('id', None)
    for key,value in property.items():
        vquery += f".property('{key}','{value}')"
    
    vquery += f".property('Last Updated', {int(time.time())})"
    queries.append({"vertices": [vquery], "edges": []})

    queries = reduce(flatten, queries, {"vertices": [], "edges": []})
    return queries

def add_pm(gerald, pm, prop_id):
    """
    Returns gremlin queries to add a connection to a property manager employee
    node.
    """
    property_manager = deepcopy(pm)
    email = property_manager['email'].lower()
    queries = []

    search_query = f"g.V().hasLabel('employee').has('email','{email}')"
    res = gerald.submit(search_query)

    if len(res) == 0:
        logging.error(f"Property manager not found: {email}")
        return {"vertices": [], "edges": []}
    else:
        # assume the first is the correct and only result
        pm_node = gerald.parse_graph_json(res[0])
        pm_id = pm_node['id']

    equery = (  f"g.V('{pm_id}')"
                f".coalesce("
                f"__.outE('manages').inV().has('id', eq('{prop_id}')),"
                f"__.addE('manages').to(g.V('{prop_id}')))")

    queries.append({"vertices": [], "edges": [equery]})

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
        landlord.pop('system', None)
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
    queries += [add_contact(gerald, ll_id, c) for c in contacts]

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
        tenancy.pop('system', None)
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
    queries += [add_contact(gerald, tt_id, c) for c in contacts]
    
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
                                f",__.property(list,'altEmail','{email}'))")
                queries.append({"vertices": [vquery], "edges": []})
        
        vquery = f"g.V('{cc_id}')"
        contact['rpsID'] = contact.pop('id')
        contact.pop('system', None)
    else:
        # A completely new contact
        cc_id = contact.pop('id', None)
        vquery = f"g.addV('{type}').property('id', '{cc_id}')"
        contact['rpsID'] = cc_id

    
    for key, value in contact.items():
        vquery += f".property('{key}', '{value}')"

    vquery += f".property('Last Updated', {int(time.time())})"

    equery = (  f"g.V('{cc_id}')"
                f".coalesce("
                f"__.outE('is a').inV().has('id', eq('{parent_id}')),"
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
    query = f"g.V('{contact['id']}').fold().coalesce(unfold(), "
    query += "g.V()"
    # Contacts are considered identical if they match on first name,
    # last name, and at least one of (email, mobile, address)
    if type == "contact":
        if len(contact['first name']) > 0:
            query += f".has('first name', '{contact['first name']}')"
        if len(contact['last name']) > 0:
            query += f".has('last name', '{contact['last name']}')"
    else:
        # Companies are considered identical if they match on company
        # name and at least one of (email, mobile, address)
        query += f".has('Company Name', '{contact['Company Name']}')"
    
    query += ".or(" 
    if "noemail" not in email:
        query += f"__.has('email', '{email}'),"
        query += f"__.has('altEmail', '{email}'),"
    if len(mobile) > 0:
        query += f"__.has('mobile', '{mobile}'),"
    if len(contact["address"]) > 0:
        query += f"__.has('address', '{contact['address']}'),"
    query += ")).dedup()"

    return gerald.submit(query)

def add_company_contacts(gerald, company):
    """
    Finds contacts in gerald with the same email as the company, and returns
    queries to add connections between them.
    """
    cmp_id = company["id"][:3]
    email = company['email']
    
    if cmp_id != "cmp":
        raise Exception("Expected a company, got a contact")
    
    search_query = "g.V().or("
    search_query += f"__.has('email', '{email}'),"
    search_query += f"__.has('altEmail', '{email}')).dedup()"

    res = gerald.submit(search_query)
    equeries = []
    for item in res:
        c = gerald.parse_graph_json(item)
        equery = (  f"g.V('{c['id']}')"
                f".coalesce("
                f"__.outE('is part of').inV().has('id', eq('{cmp_id}')),"
                f"__.addE('is part of').to(g.V('{cmp_id}')))")
        equeries.append(equery)
    
    return {"vertices": [], "edges": equeries}

###############################################################################
# VERTEX/EDGE ARCHIVAL/REMOVAL FUNCTIONS                                      #
###############################################################################
def archive_item(gerald_prop):
    """
    Sets the status of a property to 'not under management', effectively
    archiving it. Returns the query to do so.
    """
    vquery = (   f"g.V('{gerald_prop['id']}')"
                f".property('status','not under management')"
                f".property('Last Updated', {int(time.time())})")

    pm = gerald_prop['property_manager']
    if pm is not None:
        equery = replace_edge(gerald_prop['id'], 'manages', 'stopped managing', pm)
        equery = equery["edges"][0]
    
    return {"id": gerald_prop['id'], "vertices": [vquery], "edges": [equery]}

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
# MISC FUNCTIONS                                                              #
###############################################################################
def submit_all(gerald, queries):
    """
    Submits all queries to gerald.
    """
    for query in queries['vertices']:
        try:
            gerald.submit(query)
        except Exception as e:
            logging.error(f"Error submitting query: {query}", exc_info=True)
    
    for query in queries['edges']:
        try:
            gerald.submit(query)
        except Exception as e:
            logging.error(f"Error submitting query: {query}", exc_info=True)