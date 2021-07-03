# Common library for querying Drupal fields.
# Alexei Raiu, 2021

# Fetch a row values by node id.
def field_values(cursor, nid, field_name, suffix='value'):
    table = 'node__' + field_name
    row = field_name + '_' + suffix
    query = 'SELECT ' + row + ' FROM ' + table + ' WHERE entity_id=' + str(nid)
    cursor.execute(query)
    values = list(cursor.fetchall())
    if (len(values) == 0):
        return None
    if isinstance(values, (tuple, list)):
        for key, value in enumerate(values):
            if isinstance(value, (tuple, list)):
                if len(value) < 2:
                    values[key] = value[0]
                else:
                    values[key] = value
    if isinstance(values, (tuple, list)):
        if len(values) < 2:
            values = values[0]
    if isinstance(values, str):
        values = str(values).strip()
    return values


# Fetch a single text row value (will fetch first if many found).
def image_field_values(cursor, nid, field_name):
    table = 'node__' + field_name
    row = 'delta'
    fields = ['target_id', 'alt', 'title', 'width', 'height']
    for field in fields:
        row += ', ' + field_name + '_' + field
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
            used = ['fid', 'langcode', 'filename', 'uri', 'filemime', 'filesize', 'status', 'type']
            for i, column in enumerate(columns):
                if not column in used: continue
                if isinstance(file_values[0], (tuple, list)):
                    file_value = file_values[0][i]
                    if not isinstance(file_value, (str, int, float)) and bool(file_value):
                        if isinstance(file_value, bytearray): file_value = file_value.decode()
                    if bool(file_value):
                        result[value[0]][column] = file_value

    return result


# Fetch a single text row value (will fetch first if many found).
def terms_field_values(cursor, nid, field_name):
    table = 'node__' + field_name
    row = 'delta'
    fields = ['target_id']
    for field in fields:
        row += ', ' + field_name + '_' + field
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
                'tid': value[1]
            }

            # Fetch image entity data by id.
            query = 'SELECT vid, langcode, name FROM taxonomy_term_field_data WHERE tid=' + str(value[1])
            cursor.execute(query, nid)
            term_values = cursor.fetchone()
            result[value[0]]['vid'] = term_values[0]
            result[value[0]]['langcode'] = term_values[1]
            result[value[0]]['name'] = term_values[2]

    return result