# Scrapper from the ere db for companies.

from json import dump
from mysql.connector import (connection)
import export_drupal as drupal


cnx = connection.MySQLConnection(user='drupal', password='drupal',
                                 host='127.0.0.1',
                                 database='ere-src')
cursor = cnx.cursor()
query = 'SELECT nid, title, status, created, changed, langcode FROM node_field_data WHERE type="svg_kontaktperson"'
cursor.execute(query)
data = {}

# Main node data.
for (nid, title, status, created, changed, langcode) in cursor:
    data[nid] = {
        'nid': nid,
        'title': title,
        'status': status,
        'created': created,
        'changed': changed,
        'langcode': langcode
    }

for key in data:
    node = data[key]
    nid = node['nid']

    # body
    value = drupal.field_values(cursor, nid, 'body')
    if value is not None: node['body'] = value

    # field_kontaktperson_vorname
    value = drupal.field_values(cursor, nid, 'field_kontaktperson_vorname')
    if value is not None: node['first_name'] = value

    # field_kontaktperson_nachname
    value = drupal.field_values(cursor, nid, 'field_kontaktperson_nachname')
    if value is not None: node['second_name'] = value

    # field_kontaktperson_titel
    value = drupal.field_values(cursor, nid, 'field_kontaktperson_titel')
    if value is not None: node['formal_address'] = value

    # field_kontaktperson_telefon
    value = drupal.field_values(cursor, nid, 'field_kontaktperson_telefon')
    if value is not None: node['phone'] = value

    # field_kontaktperson_email
    value = drupal.field_values(cursor, nid, 'field_kontaktperson_email')
    if value is not None: node['email'] = value

    data[key] = node


# Get the email.


cursor.close()
cnx.close()

with open('contacts.json', 'w') as outfile:
    dump(data, outfile, indent=2, separators=(',', ': '))

print('Saved {0} records to "contacts.json".' . format(len(data)))