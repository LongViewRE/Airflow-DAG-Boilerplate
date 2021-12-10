###############################################################################
# client.py                                                                   #
# Functions for retrieving data from Reapit                                   #
###############################################################################
import os
import logging

from dotenv import load_dotenv
from LV_external_services import RPSClient

def get_properties_rps(rps):
    """
    Returns a list of all RPS properties.
    """
    path = 'properties/general?SearchType=lettings&Unavailable=true&IncludeUnadvertised=true&PropertyStatus=for%20sale,to%20let,under%20offer,sold,exchanged,completed,recently%20sold,sold,sold%20externally,let%20privately,tenancy%20finished,tenancy%20cancelled,reserved,pre-appraisal,paid%20valuation,market%20appraisal,let&PropertyField=ID,HouseName,HouseNumber,Address1,Address2,Address3,Address4,Postcode,Landlord,InternalLettingStatus,Latitude,Longitude,Negotiator,Status,Type'
    props = rps.call(path, 'get')
    for prop in props:
        if 'Negotiator' in prop:
            prop['pm'] = prop['Negotiator']['Name']
            prop.pop('Negotiator', None)
        
        if 'Landlord' in prop:
            # remove redundant field for clarity - we will find the landlord
            # and their contacts in a later step
            prop.pop('Landlord', None)
        if 'Type' in prop:
            prop['type'] = prop['Type'][0]
            prop.pop('Type', None)

    return props

def get_amf(rps, prop_id):
    """
    Returns the annual management fee for a property.
    """
    amf = 0

    try:
        path = f'landlord/properties/{prop_id}/transactions'
        transactions = rps.call(path, 'get')

        for i in range(len(transactions)-1, -1, -1):
            act = transactions[i]
            if 'Category' in act:
                if (act['Category'] == "Management Fee") and act['Net'] > 30:
                    amf = act['Net'] * 12
                    break
                else:
                    continue
            else:
                continue
    except:
        logging.error("Error processing AMF for property: " + prop_id,
            exc_info=True)

    return amf

def get_landlord(rps, prop_id):
    """
    Returns the landlord and contact info for a property (ID is already in 
    property object).

    Return format (assuming response is not empty):
        {
            "reapitID": landlord's reapit ID,
            "contacts": list of contact objects linked to landlord object
        }
    """
    path = f'properties/{prop_id}/landlord'
    response = rps.call(path, 'get')
    landlord = {}

    if len(response) > 0:
        landlord['reapitID'] = response['ID']
        landlord['contacts'] = []

        for item in response['Ownership']:
            contact_id = item['Contact']['ID']
            contact_object = get_contact(rps, contact_id)
            landlord['contacts'].append(contact_object)
    
    return landlord
    
def get_tenancy(rps, prop_id):
    """
    Returns the tenancy and contact info for a property. 
    """
    path = f'properties/{prop_id}/tenancy/current'
    response = rps.call(path, 'get')
    tenancy = {}

    if len(response) > 0:
        tenancy['reapitID'] = response['ID']
        tenancy['status'] = response['Status']
        tenancy['periodic'] = response['Periodic']
        tenancy['endConfirmed'] = response['EndConfirmed']
        tenancy['weeklyRent'] = response['WeeklyRent']
        if 'EndDate' in response:
            tenancy['endDate'] = response['EndDate']
        else:
            tenancy['endDate'] = ''
        if 'StartDate' in response:
            tenancy['startDate'] = response['StartDate']
        else:
            tenancy['startDate'] = ''
        
        tenancy['contacts'] = []
        if 'ID' in response['Tenant']:
            contact_id = response['Tenant']['ID']
            contact_object = get_contact(rps, contact_id)
            tenancy['contacts'].append(contact_object)

            if 'JointTenant' in response:
                for item in response['JointTenant']:
                    contact_id = item['ID']
                    contact_object = get_contact(rps, contact_id)
                    tenancy['contacts'].append(contact_object)

    return tenancy

def get_contact(rps, contact_id):
    """
    Returns a contact object for the given ID.
    Return format (assuming response is not empty):
        {
            "contactID": reapit ID for contact,
            "first name": first name,
            "last name": last name,
            "email": email,
            "mobile": mobile,
            "address": full combined address,
            "type": company or contact,
            "Company name": company name (only if type = company)
        }
    Note: some fields may be missing
    """
    path = f'contacts/{contact_id}?IncludeArchive=True'
    response = rps.call(path,'get')
    contact = {}

    if len(response) > 0:
        contact['contactID'] = contact_id
        if 'Initials' in response:
            contact['first name'] = response['Initials']
        if 'Surname' in response:
            contact['last name'] = response['Surname']
        if 'Email' in response:
            contact['email'] = response['Email']
        if 'Mobile' in response:
            contact['mobile'] = response['Mobile']
        contact['address'] = ''
        contact['address'] += response['HouseName'] + " " if 'HouseName' in response else ""
        contact['address'] += response['HouseNumber'] + " " if 'HouseNumber' in response else ""
        contact['address'] += response['Address1'] + " " if 'Address1' in response else ""
        contact['address'] += response['Address3'] + " " if 'Address3' in response else ""
        contact['address'] += response['Address4'] + ", " if 'Address4' in response else ""
        contact['address'] += response['Postcode'] + " " if 'Postcode' in response else ""
        contact['address'] += response['Country'] if 'Country' in response else ""
        if 'CompanyName' in response:
            if len(response['CompanyName']) > 0:
                contact['Company Name'] = response['CompanyName']
                contact['type'] = 'Company'
            else:
                contact['type'] = 'contact'
    
    return contact

def get_extended_property_info_rps(rps, prop):
    """
    Returns extended property information for the given property, including
    landlords, tenants and management fee.
    """
    try:
        prop['amf'] = get_amf(rps, prop['ID'])
        prop['landlord'] = get_landlord(rps, prop['ID'])
        prop['tenancy'] = {}

        if (prop['InternalLettingStatus'] != 'Tenancy Current - Available') \
            and (prop['InternalLettingStatus'] != 'Arranging Tenancy - Available') \
            and (prop['InternalLettingStatus'] != 'To Let - Available') \
            and (prop['InternalLettingStatus'] != 'Tenancy Finished') \
            and (prop['InternalLettingStatus'] != 'Arranging Tenancy - Unavailable'):
            
            prop['tenancy'] = get_tenancy(rps, prop['ID'])

    except Exception as e:
        logging.error("Error processing property: " + str(prop['ID']), exc_info=True)
    
    return prop

def get_all_properties_rps(rps):
    """
    Returns all information from RPS to sync.
    Result is a list of extended property objects, which include the property's
    landlord + contacts, and tenancy + contacts. 
    """
    props = get_properties_rps(rps)
    extended_properties = []

    for prop in props:
        extended_property = get_extended_property_info_rps(rps, prop)
        extended_properties.append(extended_property)
    
    return extended_properties

if __name__ == "__main__":
    load_dotenv()
    rps = RPSClient(os.environ['rpskey'])
    print(get_landlord(rps, 'rps_lng-HDO120385'))
