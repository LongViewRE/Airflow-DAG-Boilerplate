###############################################################################
# utils.py                                                                    #
# Utility functions, including helpers and formatting                         #
###############################################################################
import random
import logging

from thefuzz import fuzz

from gerald_syncing.PullFromRPS.client import get_all_properties_rps

###############################################################################
# HELPER FUNCTIONS                                                            #
###############################################################################
def retrieve_properties(rps, property_client):
    """
    Retrieves a list of all property info from RPS and Gerald
    """
    rps_props = get_all_properties_rps(rps)
    rps_props = [align_format_rps(p) for p in rps_props]
    logging.info("Pulled properties from RPS")
    gerald_props = property_client.get_all_managed_properties()
    gerald_props = [align_format_gerald(p) for p in gerald_props]
    logging.info("Pulled properties from Gerald")

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

def diff(d1, d2):
    """
    List difference of d1, d2, i.e returns items in d1 that are
    not in d2. Cannot use set operations as dicts are unhashable.
    """
    return [e for e in d1 if e not in d2]

def contact_comparison(tuple):
    """
    Returns true if the contacts c1 and c2 represent the same person/entity,
    otherwise returns false. Compares contacts based on name and
    one piece of contact information (email, mobile, address). Names are
    matched fuzzily to account for typos/minor changes. Only first names are
    compared, as family members may exist with same contact info - these
    should be represented as separate contacts.
    """
    c1 = tuple[0]
    c2 = tuple[1]

    if c1["id"] == c2["id"]:
        return True

    c1_type = "Company" if c1["id"][:3] == "cmp" else "contact"
    c2_type = "Company" if c2["id"][:3] == "cmp" else "contact"
    if (c1_type != c2_type):
        return False
    
    # Fuzzy matches on company name, if the similarity is below 75 it is 
    # unlikely to be the same company
    if c1_type == "Company" \
        and fuzz.ratio(c1['Company Name'].lower(), c2['Company Name'].lower()) < 75:
        return False
    
    if c1_type == "contact":
        # Fuzzy matches on first name - if similarity is below 75, it is
        # unlikely to be the same
        fname_ratio = fuzz.ratio(c1['first name'].lower(), c2['first name'].lower())
        if fname_ratio < 75:
            return False

    
    if 'address' in c1.keys() and 'address' in c2.keys():
        if c1['address'] == c2['address']:
            return True
    
    if fuzz.ratio(c1['email'].lower(), c2['email'].lower()) > 85:
        return True

    if c1['mobile'] == c2['mobile']:
        return True
    
    return False 

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
        gerald_property['landlord']['contacts'] = \
            [align_contact_gerald(c) for c in gerald_property['landlord']['contacts']]
    
    if len(gerald_property['tenancy']) > 0:
        gerald_property['tenancy'].pop('Last Updated', None)
        gerald_property['tenancy']['contacts'] = \
            [align_contact_gerald(c) for c in gerald_property['tenancy']['contacts']]

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
        
        rps_property['landlord']['contacts'] = \
            [align_contact_rps(c) for c in rps_property['landlord']['contacts']]

    if len(rps_property['tenancy']) == 0:
        rps_property['tenancy'] = {}
    else:
        rps_property['tenancy']['id'] = "tt-"+ rps_property['tenancy'].pop('reapitID')
        rps_property['tenancy']['periodic'] \
            = "True" if rps_property['tenancy']['periodic'] else "False"
        rps_property['tenancy']['endConfirmed'] \
            = "True" if rps_property['tenancy']['endConfirmed'] else "False"

        rps_property['tenancy']['contacts'] = \
            [align_contact_rps(c) for c in rps_property['tenancy']['contacts']]
    
    return rps_property


def align_contact_rps(rps_contact):
    """
    Makes the rps contact format/schema consistent with that of gerald.
    """
    rps_contact['system'] = 'RPS'
    type = rps_contact.pop('type', None)
    if type == 'contact':
        rps_contact['id'] = "cc-"+rps_contact.pop('contactID')
        rps_contact.pop('Company Name', None)
        if 'first name' in rps_contact:
            if "'" in rps_contact['first name']:
                rps_contact['first name'] = \
                    rps_contact['first name'].replace("'", "")
        else:
            rps_contact['first name'] = ""
        
        if 'last name' in rps_contact:
            if "'" in rps_contact['last name']:
                rps_contact['last name'] = \
                    rps_contact['last name'].replace("'", "")
        else:
            rps_contact['last name'] = ""

    elif type == 'Company':
        rps_contact['id'] = "cmp-"+rps_contact.pop('contactID')
        rps_contact.pop('first name', None)
        rps_contact.pop('last name', None)
        
        if 'Company Name' in rps_contact:
            if "'" in rps_contact['Company Name']:
                rps_contact['Company Name'] = rps_contact['Company Name'].replace("'", "")
        else:
            rps_contact['Company Name'] = ""
    
    
    if 'mobile' not in rps_contact:
        rps_contact['mobile'] = ''
    if 'address' not in rps_contact:
        rps_contact['address'] = ''
    else:
        if "'" in rps_contact['address']:
            rps_contact['address'] = rps_contact['address'].replace("'", "")
    if 'email' not in rps_contact:
        rps_contact['email'] = "noemail." + str(random.randint(0, 10000000)) + "@nocontact.com.au"
    
    
    return rps_contact