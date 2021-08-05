# Scrapper from the ere db for companies.

from json import dump
from mysql.connector import (connection)
import export_drupal as drupal
import contacts



cnx = connection.MySQLConnection(user='drupal', password='drupal',
                                 host='127.0.0.1',
                                 database='ere-src')
cursor = cnx.cursor()
query = 'SELECT nid, title, status, created, changed, langcode FROM node_field_data WHERE type="svg_firma"'
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
    target_id = drupal.field_values(cursor, nid, 'field_firma_kontaktpersonen', 'target_id')
    value = contacts.contactData(target_id)
    if value is not None: node['contact'] = value

    # field_firma_land
    value = drupal.field_values(cursor, nid, 'field_firma_land')
    if value is not None: node['country'] = value

    # field_firma_logos
    value = drupal.image_field_values(cursor, nid, 'field_firma_logos')
    if value is not None: node['logos'] = value

    # field_firma_namenszusatzzeile
    value = drupal.field_values(cursor, nid, 'field_firma_namenszusatzzeile')
    if bool(value): node['slogan'] = value

    # field_firma_niederlassungen
    value = drupal.terms_field_values(cursor, nid, 'field_firma_niederlassungen')
    if value is not None: node['offices'] = value

    # field_firma_notizen
    value = drupal.field_values(cursor, nid, 'field_firma_notizen')
    if bool(value): node['notes'] = value

    # field_firma_ort
    value = drupal.field_values(cursor, nid, 'field_firma_ort')
    if bool(value): node['city'] = value

    # field_firma_postleitzahl
    value = drupal.field_values(cursor, nid, 'field_firma_postleitzahl')
    if bool(value): node['zip'] = value

    # field_firma_postleitzahl
    value = drupal.field_values(cursor, nid, 'field_firma_postleitzahl')
    if bool(value): node['zip'] = value

    # field_firma_rubriken
    value = drupal.terms_field_values(cursor, nid, 'field_firma_rubriken')
    if value is not None: node['categories'] = value

    # field_firma_shortlink
    value = drupal.field_values(cursor, nid, 'field_firma_shortlink')
    if bool(value): node['shortlink'] = value

    # field_firma_strasse
    value = drupal.field_values(cursor, nid, 'field_firma_strasse')
    if bool(value): node['street'] = value

    # field_firma_telefon
    value = drupal.field_values(cursor, nid, 'field_firma_telefon')
    if bool(value): node['phone'] = value

    # node__field_firma_veroeffentlichen_am
    value = drupal.field_values(cursor, nid, 'field_firma_veroeffentlichen_am')
    if bool(value): node['publish_from'] = value

    # field_firma_telefon
    value = drupal.field_values(cursor, nid, 'field_firma_veroeffentlichen_bis')
    if bool(value): node['publish_to'] = value

    data[key] = node


cursor.close()
cnx.close()

with open('companies.json', 'w') as outfile:
    dump(data, outfile, indent=2, separators=(',', ': '))

print('Saved {0} records to "companies.json".' . format(len(data)))