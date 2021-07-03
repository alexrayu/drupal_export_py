# Scrapper from the ere db for companies.

from json import dump
from mysql.connector import (connection)


# Fetch a single text row value (will fetch first if many found).
def fetch_text(nid, field_name):
    table = 'node__' + field_name
    row = field_name + '_value'
    query = 'SELECT ' + row + ' FROM ' + table + ' WHERE entity_id=' + str(nid)
    cursor.execute(query, nid)
    value = cursor.fetchone()
    if isinstance(value, (tuple, list)) and len(value) < 2:
        value = value[0]
    if isinstance(value, str):
        value = str(value).strip()
    return value


cnx = connection.MySQLConnection(user='drupal', password='drupal',
                                 host='127.0.0.1',
                                 database='ere-src')
cursor = cnx.cursor()
query = 'SELECT nid, title, created, changed, langcode FROM node_field_data WHERE type="svg_kontaktperson" AND status=1'
cursor.execute(query)
data = {}

# Main node data.
for (nid, title, created, changed, langcode) in cursor:
    data[nid] = {
        'nid': nid,
        'title': title,
        'created': created,
        'changed': changed,
        'langcode': langcode
    }

for key in data:
    node = data[key]
    nid = node['nid']

    # body
    node['body'] = fetch_text(nid, 'body')

    # field_kontaktperson_vorname
    node['first_name'] = fetch_text(nid, 'field_kontaktperson_vorname')

    # field_kontaktperson_nachname
    node['second_name'] = fetch_text(nid, 'field_kontaktperson_nachname')

    # field_kontaktperson_titel
    node['formal_address'] = fetch_text(nid, 'field_kontaktperson_titel')

    # field_kontaktperson_telefon
    node['phone'] = fetch_text(nid, 'field_kontaktperson_telefon')

    # field_kontaktperson_email
    node['phone'] = fetch_text(nid, 'field_kontaktperson_email')

    data[key] = node


# Get the email.


cursor.close()
cnx.close()

with open('contacts.json', 'w') as outfile:
    dump(data, outfile, indent=2, separators=(',', ': '))

print('Saved {0} records to "contacts.json".' . format(len(data)))