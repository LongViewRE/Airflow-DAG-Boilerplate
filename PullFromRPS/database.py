import os

from LV_db_connection import GremlinClient
from dotenv import load_dotenv

def get_gerald_properties(client):
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
                .union(identity(),
                __.in('Owns').as('landlord').in('is a').as('contact').select('landlord','contact'),
                __.in('renting').as('tenancy').in('is a').as('contact').select('tenancy','contact'))
                .dedup()
                """
                ### TODO - find better query to return an easier format

def format_property_info(result):
    """
    Formats gerald output """


def get_field(vertex, key):
    """
    Gets a value from the given graph output vertex that may or may not exist.
    If it does not exist, returns None.
    """
    value = None
    with contextlib.suppress(KeyError):
        value = vertex["properties"][key][0]["value"]
    return value

def read_landlord(lan):
    """
    Parses landlord graph json representation returned by query and itself returns a dict
    corresponding to the representation defined in the API. 
    """
    landlord = {}
    landlord["id"] = lan["id"]
    landlord["contacts"] = 

def read_tenancy(ten):
    """
    Parses tenancy graph json representation returned by query and itself returns a dict
    corresponding to the representation defined in the API. 
    """  
    tenancy = {}
    tenancy["id"] = ten["id"]
    tenancy["startDate"] = ten["properties"]["startDate"][0]["value"]
    tenancy["endDate"] = ten["properties"]["endDate"][0]["value"]
    tenancy["weeklyRent"] = ten["properties"]["weeklyRent"][0]["value"]
    tenancy["status"] = ten["properties"]["status"][0]["value"]
    tenancy["periodic"] = (get_field(ten, "periodic") == True)
    tenancy["endConfirmed"] = (get_field(ten, "endConfirmed") == True)

    return tenancy

def read_contact(con):
    """
    Parses contact graph json representation returned by query and itself returns a dict
    corresponding to the representation defined in the API. 
    """
    contact = {}
    contact["id"] = con["id"]

    contact["first name"] = get_field(con, "first name")
    contact["last name"] = get_field(con, "last name")
    contact["Company Name"] = get_field(con, "Company Name")
    contact["address"] = get_field(con, "address")
    contact["email"] = get_field(con, "email")
    contact["mobile"] = get_field(con, "mobile")

    return contact


def read_property(prop):
    """
    Parses property graph json representation returned by query and itself returns a dict
    corresponding to the representation defined in the API. 
    """
    property = {}
    property["ID"] = prop["id"]
    property["housenumber"] =  prop["properties"]["housenumber"][0]["value"]
    property["address1"] = prop["properties"]["address1"][0]["value"]
    property["address3"] = prop["properties"]["address3"][0]["value"]
    property["postcode"] = prop["properties"]["postcode"][0]["value"]
    property["address4"] = prop["properties"]["address4"][0]["value"]

    property["latitude"] = get_field(prop, "latitude")
    property["longitude"] = get_field(prop, "longitude")
    property["pm"] = get_field(prop, "pm")
    property["amf"] = get_field(prop, "amf")
    property["type"] = get_field(prop, "type")

    return property

if __name__ == "__main__":
    load_dotenv()
    client = GremlinClient(os.environ['GERALD_USERNAME'], os.environ['GERALD_PWD'])
    print(get_gerald_properties(client))

