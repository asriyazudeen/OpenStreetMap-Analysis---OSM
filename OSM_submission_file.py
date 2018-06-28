# -*- coding: utf-8 -*-
"""
Created on Sat Jan  6 20:22:52 2018

@author: Riyazudeen
"""

# importing all possible library's 
import os 
import xml.etree.cElementTree as ET  
import pprint
import re
import schema
import csv
import codecs
from collections import defaultdict
import sqlite3
import pandas as pd 

#setting up my working directory
PATH= "/Study/Udacity/Projects/OSM/" 

#defining regular expressions to find the data anomolies, 
lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

test=[]

def audit_element(element,attrib):    
    if element.tag in["node","way"]:
        for tag in element.iter("tag"):
            if attrib == "phone":
                if tag.attrib['k'] == attrib:
                    x1=tag.get('v')        
                    return x1
            elif attrib=="addr:street":
                 if tag.attrib['k'] == attrib:
                    x1=tag.get('v')
                    x2=x1.split(" ")
                    x2=x2[-1]
                    return x2
            elif attrib=="addr:state":
                 if tag.attrib['k'] == attrib:
                    x3=tag.get('v')  
                    return x3
            
                

def process_map(filename,attrib):
    #Creating empyt set to store the information
    phone_numbers = set()
    street= set()
    state=set()
  
    if attrib=="phone": 
        
        for _, element in ET.iterparse(filename):   
            phone_numbers.add(audit_element(element,attrib))            
        return phone_numbers
    
    elif attrib=="addr:street":
        for _, element in ET.iterparse(filename):
            street.add(audit_element(element,attrib))
        return street
    
    elif attrib=="addr:state":
        for _, element in ET.iterparse(filename):
            state.add(audit_element(element,attrib))
        return state
            
################33
OSM_PATH = "OSM.xml" 

#List of CSV files that we intented to create for out DB load
NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"

#regular expression to find the lower, colon and the problem char.
LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

SCHEMA = schema.schema

# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp'] 
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type'] 
WAY_NODES_FIELDS = ['id', 'node_id', 'position'] 

#correct street names
mapping = { "St": "Street",
            "St.": "Street",
            "st":"Street",
            "Rd": "Road",
            "Rd.":"Road",
            "Ave":"Avenue",
            "Ste":"Suite",
            "Bldg":"Building",
            "Ste":"Suite"
            }

#Result from our analysis
audit_state={None, 'CA', 'None', 'California'} 

#This function used to correct the street names
def update_street_name(name, mapping):   
    x=name.split(" ")
    x=x[-1]    
    if x in mapping.keys():
        rp=mapping[x]        
        name=name.replace(x,rp)
    return name

#This dic value used to correct the phone numnbers
replace_txt={'(':'' , ')':'' , ' ':'','-':'','.':'','–':'','*':'','+':'','or':'','/':''}

#function to correct the phone numbers
def fix_phone(value,replace_txt):    
    for i,l in replace_txt.items():
        value=value.replace(i,l)
    if value.startswith('1'):
        value='+' + value 
    elif not value.startswith('1'):
        value='+1' + value         
 
    return value

#function to fix the values
def fix_data (val,tags):
    for i in range(len(tags)):
        if tags[i][val] in audit_state:
            tags[i][val]='California'
        elif tags[i][val] == 'addr:street':
            name=tags[i]['value']
            tags[i]['value']   = update_street_name(name,mapping)
        elif tags[i][val]=='phone':
            name=tags[i]['value']
            tags[i]['value'] = fix_phone(name,replace_txt)                    
    return tags
    
def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""

    #Empty dictonary & list declared to store the nodes & ways values
    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []  
 

    #Iterate through nodes and store the final value in node_attribs   
    if element.tag == 'node':
        node_attribs=element.attrib
        node_attribs.pop('visible',None)
        
        #Iterate through tag's & store the final values in tags
        for tag in element.iter('tag'):
            node_tg=tag.attrib    
            node_tg['id']=element.attrib['id']
            node_tg['value']=node_tg.pop('v') #This will create a new note called value and delete the old 'v'
            node_tg['key']=node_tg.pop('k')
            node_tg['type']='regular'   
            #This line of code to used to fix the 'contact:phone' to 'phone'.
            if node_tg['key']=="contact:phone":
                node_tg['key']="phone"
            tags.append(node_tg)   
           
            tags=fix_data('value',tags)   
            tags=fix_data('key',tags)  
            
                 
        
        #pprint.pprint({'node': node_attribs, 'node_tags': tags})
        return {'node': node_attribs, 'node_tags': tags} 
    elif element.tag == 'way':
        way_attribs=element.attrib
        way_attribs.pop('visible',None)      
        position=-1        
        for tag in element.iter("nd"):
            position += 1
            way_nod=tag.attrib
            way_nod['id']=element.attrib['id']
            way_nod['position']=position
            way_nod['node_id']=way_nod.pop('ref') #renaming & deleting the old key
            way_nodes.append(way_nod)
                        
        for tag in element.iter("tag"):
            tag=tag.attrib
            key=tag.get('k')
            typ=tag.get('k')
            value=tag.get('v')
            tag['id']=element.attrib['id']            
            tag['value']=value           
            
            #Code to split the k value & store the actual 
            if typ.find(":") >=1:
                z1=typ.split(":")
                z1=z1[0]
            elif typ.find(":") < 0:
                z1="regular"
            
            if key.find(":") >=1:
                k1=key[key.find(":")+1:]
            elif key.find(":") < 0:
                k1=key
                
            tag['key']=k1
            tag['type']=z1    
            tag.pop('v',None)
            tag.pop('k',None)
            
            tags.append(tag)
            
            tags=fix_data('value',tags)   
            tags=fix_data('key',tags) 

#         pprint.pprint({'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags})
        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}


def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)
        
        raise Exception(message_string.format(field, error_string)) 


class UnicodeDictWriter(csv.DictWriter, object):
    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# ================================================== #
#               Main Function                        #
# ================================================== #
def proc_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'w',encoding='utf-8') as nodes_file, \
            codecs.open(NODE_TAGS_PATH, 'w',encoding='utf-8') as nodes_tags_file, \
            codecs.open(WAYS_PATH, 'w',encoding='utf-8') as ways_file, \
            codecs.open(WAY_NODES_PATH, 'w',encoding='utf-8') as way_nodes_file, \
            codecs.open(WAY_TAGS_PATH, 'w',encoding='utf-8') as way_tags_file:
    
        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader() 
        node_tags_writer.writeheader() 
        ways_writer.writeheader() 
        way_nodes_writer.writeheader() 
        way_tags_writer.writeheader() 

#         validator = cerberus.Validator() 
        
        
#         #added this
#        count = 0
        for element in get_element(file_in, tags=('node','way')):
#             print(element)
            
            el = shape_element(element)
            if el:

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags']) 

 
            

if __name__ == "__main__":    
    audit_telephone=process_map("OSM_sample1.xml","phone")
    print("AUDITING SAMPLE FILES:\n")
    print("Audit Telephone:\n")
    print(audit_telephone)
    print("\n")
    
    audit_street=process_map("OSM_sample1.xml","addr:street")
    print("Audit Street:\n")
    print(audit_street)
    print("\n")
    
    audit_state=process_map("OSM_sample1.xml","addr:state")
    print("Audit State:\n")
    print(audit_state)      
    
    print("\nCleaning the XML file & Generating CSV files.....")
    proc_map(OSM_PATH, validate=True) 
    print("\nCSV files are created Sucessfully!")
