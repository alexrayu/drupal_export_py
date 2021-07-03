# Scrapper from the ere db for companies.

from json import dump
from mysql.connector import (connection)


# Fetch a row values by node id.
def field_values(nid, field_name):
    table = 'node__' + field_name
    row = field_name + '_value'
    query = 'SELECT ' + row + ' FROM ' + table + ' WHERE entity_id=' + str(nid)
    cursor.execute(query)
    values = cursor.fetchall()
    if (len(values) == 0):
        return None
    if isinstance(values, (tuple, list)):
        for key, value in enumerate(values):
            if isinstance(value, (tuple, list)):
                values[key] = ', '.join(value).strip()
    if len(values) < 2:
       values = values[0]
    if isinstance(values, str):
        values = str(values).strip()
    return values


# Fetch a single text row value (will fetch first if many found).
def image_field_values(nid, field_name):
    table = 'node__' + field_name
    row = 'delta'
    fields = ['target_id', 'alt', 'title', 'width', 'height']
    for field in fields:
        row += ', ' + field_name + '_' + field

    # Field values.
    query = 'SELECT ' + row + ' FROM ' + table + ' WHERE entity_id=' + str(nid)
    cursor.execute(query)
    values = cursor.fetchall()
    if (len(values) == 0):
        return None
    result = None
    if isinstance(values, (tuple, list)):
        result = {}
        for value in values:
            result[value[0]] = {
                'delta': value[0],
                'target_id': value[1]
            }
            if bool(value[2]): result[value[0]]['alt'] = value[2]
            if bool(value[3]): result[value[0]]['title'] = value[3]
            if bool(value[4]): result[value[0]]['width'] = value[4]
            if bool(value[5]): result[value[0]]['height'] = value[5]

            # Fetch image entity data by id.
            query = 'SELECT * FROM file_managed WHERE fid=' + str(value[1])
            cursor.execute(query, nid)
            file_values = cursor.fetchall()
            file = {}
            columns = [column[0] for column in cursor.description]
            for i, column in enumerate(columns):
                if isinstance(file_values[0], (tuple, list)):
                    file_value = file_values[0][i]
                    if not isinstance(file_value, (str, int, float)) and bool(file_value):
                        if isinstance(file_value, bytearray): file_value = file_value.decode()
                        file_value = str(file_value)
                    if bool(file_value): file[column] = file_value
            result[value[0]]['file'] = file

    return result


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
    value = field_values(nid, 'body')
    if value is not None: node['body'] = value

    # field_cka_aid
    value = field_values(nid, 'field_cka_aid')
    if value is not None: node['cka_aid'] = value

    # field_cka_oid
    value = field_values(nid, 'field_cka_oid')
    if value is not None: node['cka_oid'] = value

    # field_firma_bildergalerie
    value = image_field_values(nid, 'field_firma_bildergalerie')
    if value is not None: node['gallery'] = value

    data[key] = node


# Get the email.


cursor.close()
cnx.close()

with open('companies.json', 'w') as outfile:
    dump(data, outfile, indent=2, separators=(',', ': '))

print('Saved {0} records to "companies.json".' . format(len(data)))