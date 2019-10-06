#!/usr/bin/python3

import psycopg2
import re
import string
import sys

_PUNCTUATION = frozenset(string.punctuation)


def _remove_punc(token):
    """Removes punctuation from start/end of token."""
    i = 0
    j = len(token) - 1
    idone = False
    jdone = False
    while i <= j and not (idone and jdone):
        if token[i] in _PUNCTUATION and not idone:
            i += 1
        else:
            idone = True
        if token[j] in _PUNCTUATION and not jdone:
            j -= 1
        else:
            jdone = True
        return "" if i > j else token[i:(j + 1)]
    
    
def _get_tokens(query):
    rewritten_query = []
    tokens = re.split('[ \n\r]+', query)
    for token in tokens:
        cleaned_token = _remove_punc(token)
        if cleaned_token:
            if "'" in cleaned_token:
                cleaned_token = cleaned_token.replace("'", "''")
            rewritten_query.append(cleaned_token.lower())
    return rewritten_query


def search(query, query_type, offset, choice):
    rows = []
    number = (0,)
    rewritten_query = _get_tokens(query)
    aset = set(rewritten_query)
    rewritten_query = list(aset)
    try:
        connection = psycopg2.connect(database="cs143")
        cursor = connection.cursor()
        first_element = rewritten_query[:1]
        if (choice == 'hold'):
            print(str(offset))
            cursor.execute("""select * from current limit 20 offset """ + str(offset))
        elif (len(rewritten_query) == 1):
            cursor.execute("drop materialized view if exists current;")
            cursor.execute("""create materialized view current as
            select song_name, artist_name, page_link
            from tfidf t
            join song s on t.song_id = s.song_id
            join artist a on s.artist_id = a.artist_id
            where token = %s
            order by score desc, song_name, artist_name;""", first_element)
            cursor.execute("select count(*) from current")
            number = cursor.fetchone()
            cursor.execute("""select * from current limit 20 offset """ + str(offset))
        elif (query_type == "or"):
            middle = ""
            middle_elements = len(rewritten_query) - 1
            for iter in range(0, middle_elements):
                middle += "union all (select song_id, score from tfidf where token = %s)"
            query = """create materialized view current as
            select s.song_name, a.artist_name, s.page_link
            from(
            (select song_id, score
            from tfidf where token = %s)""" + middle + """) temp
            join song s on temp.song_id = s.song_id
            join artist a on s.artist_id = a.artist_id
            group by temp.song_id, s.song_name, a.artist_name, s.page_link
            order by sum(temp.score) desc, s.song_name, a.artist_name;"""
            cursor.execute("drop materialized view if exists current;")
            cursor.execute(query, rewritten_query)
            cursor.execute("select count(*) from current")
            number = cursor.fetchone()
            cursor.execute("""select * from current limit 20 offset """ + str(offset))
        elif (query_type == "and"):
            middle = ""
            middle_elements = len(rewritten_query) - 1
            for iter in range(0, middle_elements):
                middle += "intersect (select song_id from tfidf where token = %s)"
            middle2 = ""
            for iter in range(0, middle_elements):
                middle2 += "or token = %s"
            query = """create materialized view current as
            select s.song_name, a.artist_name, s.page_link
            from(
            (select song_id
            from tfidf where token = %s)""" + middle + """) temp
            join tfidf on temp.song_id = tfidf.song_id
            join song s on temp.song_id = s.song_id
            join artist a on s.artist_id = a.artist_id
            where token = %s""" + middle2 + """group by temp.song_id, s.song_name, a.artist_name, s.page_link
            order by sum(tfidf.score) desc, s.song_name, a.artist_name;"""
            cursor.execute("drop materialized view if exists current;")
            cursor.execute(query, rewritten_query + rewritten_query)
            cursor.execute("select count(*) from current")
            number = cursor.fetchone()
            cursor.execute("""select * from current limit 20 offset """ + str(offset))
        connection.commit()
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
    except:
        print("connection error")
        cursor.close()
        connection.close()
        
    """TODO
    Your code will go here. Refer to the specification for projects 1A and 1B.
    But your code should do the following:
    1. Connect to the Postgres database.
    2. Graciously handle any errors that may occur (look into try/except/finally).
    3. Close any database connections when you're done.
    4. Write queries so that they are not vulnerable to SQL injections.
    5. The parameters passed to the search function may need to be changed for 1B. 
    """
    return (rows, number[0], offset)


if __name__ == "__main__":
    if len(sys.argv) > 2:
        result = search(' '.join(sys.argv[3:]), sys.argv[1].lower(), sys.argv[2], 'recompute')
        print(result)
    else:
        print("USAGE: python3 search.py [or|and] term1 term2 ...")
