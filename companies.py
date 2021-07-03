# Scrapper from the ere db for companies.

from json import dump
from mysql.connector import (connection)
import export_drupal as drupal


cnx = connection.MySQLConnection(user='drupal', password='drupal',
                                 host='127.0.0.1',
                                 database='ere-src')
cursor = cnx.cursor()
query = 'SELECT nid, title, created, changed, langcode FROM node_field_data WHERE type="svg_firma" AND status=1'
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
    value = drupal.field_values(cursor, nid, 'body')
    if value is not None: node['body'] = value

    # field_cka_aid
    value = drupal.field_values(cursor, nid, 'field_cka_aid')
    if value is not None: node['cka_aid'] = value

    # field_cka_oid
    value = drupal.field_values(cursor, nid, 'field_cka_oid')
    if value is not None: node['cka_oid'] = value

    # field_firma_bildergalerie
    value = drupal.image_field_values(cursor, nid, 'field_firma_bildergalerie')
    if value is not None: node['gallery'] = value

    # field_firma_branchen
    value = drupal.terms_field_values(cursor, nid, 'field_firma_branchen')
    if value is not None: node['sectors'] = value

    # field_firma_email
    value = drupal.field_values(cursor, nid, 'field_firma_email')
    if value is not None: node['email'] = value

    # field_firma_englischer_text
    value = drupal.field_values(cursor, nid, 'field_firma_englischer_text')
    if value is not None: node['english_text'] = value

    # field_firma_fax
    value = drupal.field_values(cursor, nid, 'field_firma_fax')
    if value is not None: node['fax'] = value

    # field_firma_hausnummer
    value = drupal.field_values(cursor, nid, 'field_firma_hausnummer')
    if value is not None: node['house_nr'] = value

    # field_firma_homepage
    value = drupal.field_values(cursor, nid, 'field_firma_homepage', 'uri')
    if value is not None: node['website'] = value

    # field_firma_kontaktpersonen
    value = drupal.field_values(cursor, nid, 'field_firma_kontaktpersonen', 'target_id')
    if value is not None: node['contacts'] = value


    data[key] = node


# Get the email.


cursor.close()
cnx.close()

with open('companies.json', 'w') as outfile:
    dump(data, outfile, indent=2, separators=(',', ': '))

print('Saved {0} records to "companies.json".' . format(len(data)))