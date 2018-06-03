import mysql.connector

mysql_config = {
    'user': 'jc',
    'password': 'aller',
    'host': '127.0.0.1',
    'port': 3306,
    'database': 'douban_imdb_rating',
    'raise_on_warnings': True
    }


def make_mysql_connection(**kwargs):
    """ return a mysql connecting point
        (mysql.connector.connect)
    """
    return mysql.connector.connect(**kwargs)


def mysql_query_cursor(
                        connect_point=None, table_name='',
                        columns=["*"], limit_num=500):
    """ return a cursor (mysql.connector.connect.cursor)
        for selected columns (list) (if not specific, select all the columns)
        if connect_point (mysql.connector.connect)
        and table_name (str) is given
    """
    if not connect_point:
        print('Missing connection')
    elif not table_name:
        print('Missing table')
    else:
        cursor = connect_point.cursor(buffered=True)
        columns_str = ','.join(columns)
        url_query = "SELECT " + columns_str + """ FROM {} LIMIT {};
                                """.format(table_name, str(limit_num))
        cursor.execute(url_query)
        return cursor


# def update_row(
#                 connect_point=None, table_name='',
#                 update_column='', identify_column='',
#                 update_entry='', identify_entry=''):
#     if not connect_point:
#         print('Missing connection')
#     elif not table_name:
#         print('Missing table')
#     elif not update_column:
#         print('Missing update column')
#     elif not identify_column:
#         print('Missing identify column')
#     elif not update_entry:
#         print('Missing update entry')
#     elif not identify_entry:
#         print('Missing identify entry')
#     else:
#         cursor = connect_point.cursor(buffered=True)
#         kwargs = {
#                 'table_name': table_name,
#                 'update_column': update_column,
#                 'identify_column': identify_column,
#                 'update_entry': update_entry,
#                 'identify_entry': identify_entry}
#         update_query = """
#         UPDATE {table_name}
#         SET {update_column}='{update_entry}'
#         WHERE {identify_column}='{identify_entry}'
#         """.format(**kwargs)
#         cursor.execute(update_query)
#         connect_point.commit()
#         print('Updated to', update_entry)


cnx = make_mysql_connection(**mysql_config)
cursor = mysql_query_cursor(
                connect_point=cnx,
                table_name='douban_to_imdb',
                columns=["douban_url", "imdb_url"],
                limit_num=5)
for (douban_url, imdb_url) in cursor:
    # print(douban_url, imdb_url)
    new_imdb_url = imdb_url+'/'
    # update_row(
    #     connect_point=cnx,
    #     table_name='douban_to_imdb',
    #     update_column='imdb_url',
    #     identify_column='douban_url',
    #     update_entry=new_imdb_url,
    #     identify_entry=douban_url
    # )
