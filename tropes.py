from collections import defaultdict
from functools import lru_cache, partial
import time
from urllib.request import urlopen
from urllib.parse import urljoin, urlparse, urlunparse, urlencode
import json
import sqlite3
from contextlib import closing
import traceback
from typing import Optional, Mapping, Sequence, Any, Callable, Dict, Tuple, Type, TypeVar, List, Set, Generator, Iterable

# ('http', 'host', '/path', 'params', 'query', 'fragment')
# scheme = 'https'
# net_loc = 'allthetropes.org'
# path = 'w/api.php/'
# params = None
# query = None
# fragment = None

# {
#    "pages":{
#       "63175":{
#          "pageid":63175,
#          "ns":0,
#          "title":"100% Completion",
#          "categories":[
#             {
#                "ns":14,
#                "title":"Category:100% Completion"
#             },
#             {
#                "ns":14,
#                "title":"Category:Ending Tropes"
#             }
#          ]
#       },



# How about a table per category?
# > SQLite does not support joins containing more than 64 tables...
# > So one could not query a Trope belonging to more than 64 categories...
# How about one hot encoding?
# There is a hard limit of 32,767 columns in any sqlite table...
# The default setting for SQLITE_MAX_COLUMN is 2000
# You can change it at compile time to values as large as 32767
# SQLite does not have a separate Boolean storage class so we burn a signed int per value...
# > One hot encoding rather than references in other tables.
# How about storing as a bitfield?
# > This limits the number of categories drastically and obfuscates their meaning...
# How about a compound key on a categories table i.e. (id, category)?
# It seems like we have a winner!

# https://docs.python.org/3/howto/functional.html
# The functions map, filter are examples of lazy iterators AKA generators as they yield rather than return.
# The any, all, sorted, enumerate, or constructors such as list, will execute the iterator/generator.
# The function zip takes iterables and is iteself an iterable i.e. is lazy.
# The itertools module contains a number of commonly used iterators as well as functions for combining several iterators.
# The operator module contains a set of functions corresponding to Python’s operators.
# The functools module contains some higher-order functions e.g. the functools.partial(), .reduce() functions.

Page = Tuple[int, str]
Pages = Set[Page]
Members = Mapping[Page, Pages]

# Generator[YieldType, SendType, ReturnType]
# Iterable[YieldType]

@lru_cache(maxsize=None)
def site_maintenance_pages() -> Pages:
    pages = set()
    for members in category_members(category='Site Maintenance', gcmlimit=100, max_members=None):
        pages.update(members.keys())
    return pages

@lru_cache(maxsize=None)
def category_page(category: str,
            scheme: str = 'https', 
            net_loc: str = 'allthetropes.org', 
            path: str = 'w/api.php/') -> Optional[Page]:

    query_params = {
        'action': 'query',
        'format': 'json',
        'formatversion': 2, # returns a list
        'generator': 'allcategories',
        'gacfrom': category,
        'gacto': category,
        'maxlag': 1
    }
    
    url = urlunparse((scheme, net_loc, path, None, urlencode(query_params), None))
    with urlopen(url) as response:
        utf8 = response.read().decode('utf8')
        dict = json.loads(utf8)
        if ((query := dict.get('query', None)) != None and 
            (pages := query.get('pages', None)) != None and 
            (page := (pages[0] if len(pages) == 1 else None)) != None and
            (page.get('missing', None)) == None):
            #print(f"{category}, {scheme}, {net_loc}, {path} -> (page['pageid'], page['title'])")
            return (page['pageid'], page['title'])
        else:
            return None

# Nota bene: The larger the 'limit' (10 - 500), the more memory is used, and fewer requests are made.
# The size of each 'batch' depends on the association of category members to categories, category
# members belonging to many categories which trigger more continues before the batch is complete.

def category_members(category: str,
                     exclude_pages: Pages = set(),
                     max_members: int = None,
                     gcmlimit: int = 50,
                     cllimit: int = 20,
                     scheme: str = 'https', 
                     net_loc: str = 'allthetropes.org', 
                     path: str = 'w/api.php/') -> Generator[Members, None, None]:

    if max_members != None and max_members <= 0:
        max_members = None

    gcmlimit = max(10, min(gcmlimit, 500))
    cllimit = max(10, min(cllimit, 500))

    # When you make an API request using a generator together with properties, the API result may signal to 
    # continue because there are more properties to retrieve for the pages so far (in which case the same set
    # of pages is returned but with the next set of properties), or because there are more pages from the 
    # generator, or both. From version 1.25 onwards, the API returns a batchcomplete element to indicate that 
    # all data for the current "batch" of pages has been returned and the continue element doesn't contain 
    # continuation data for properties, but instead can contain continuation data for the generator. This can 
    # be useful to avoid building a combined result set for thousands of pages when using a generator together 
    # with prop modules that may themselves need continuation. 

    # 'linkshere' also includes pageid but requires lhnamespace to limit scope...
    #'lhnamespace': 14, # 'Category' from https://www.mediawiki.org/wiki/Help:Namespaces

    # One can limit the 
    # https://allthetropes.org/w/api.php?action=query&format=json&maxlag=1&prop=categories&continue=%7C%7C&generator=categorymembers&formatversion=2&clshow=!hidden&clcategories=Category%3AEnding%20Tropes%7CCategory%3ATropes%7CCategory%3A%20Twist%20Ending&gcmtitle=Category%3A%20Twist%20Ending&gcmprop=ids%7Ctitle%7Ctype&gcmtype=page%7Csubcat

    # We don't want any of these...
    # https://allthetropes.org/wiki/Category:Site_Maintenance

    query_params = {
        'action': 'query',
        'format': 'json',
        'formatversion': 2, # returns a list
        'generator': 'categorymembers',
        'gcmtitle': f'Category:{category}',
        'gcmlimit': gcmlimit, # 'max', # or 1 - 500
        'gcmtype': 'subcat|page',
        'prop': 'categories', # limit the result with clcategories=Category:Ending Tropes|Category:Tropes|Category:Twist Ending
        'cllimit': cllimit,
        'maxlag': 1
    }

    num_members = 0
    
    # urlunparse
    # <scheme>://<netloc>/<path>;<params>?<query>#<fragment>

    # we want to yield on "batchcomplete": true with a "continue"
    # a bare return statement, or simply returning from the function, indicates that it is done...
    
    category_page_ = partial(category_page, scheme=scheme, net_loc=net_loc, path=path)
    
    members = defaultdict(set)
    while max_members == None or num_members < max_members:        
        url = urlunparse((scheme, net_loc, path, None, urlencode(query_params), None))
        with urlopen(url) as response:
            utf8 = response.read().decode('utf8')
            dict = json.loads(utf8)
            if ((query := dict.get('query', None)) != None and 
                (pages := query.get('pages', None)) != None):
                for page in pages:
                    category_titles = map(lambda category: category['title'].removeprefix('Category:'), page.get('categories', []))
                    categories = set(map(category_page_, category_titles))
                    categories.discard(None)
                    members[(page['pageid'], page['title'])].update(categories.difference(exclude_pages))
                    #print(f"{(page['pageid'], page['title'])} {members[(page['pageid'], page['title'])]}")
            else:
                print(dict)
            if dict.get('batchcomplete', None) == True:
                num_members += len(members)
                print(f'batchcomplete...{len(members)} members of {num_members} total')
                yield members
                members.clear()

            if (continue_value := dict.get('continue', None)) == None:
                return

            print(f'continue...{len(members)} members {continue_value}')
            # Nota bene: Need to remove previous continue items as they can switch
            # from e.g. lhcontinue to gcmcontinue when combining props with generator...
            query_params = {k: v for k, v in query_params.items() if not k.endswith('continue')}
            query_params.update(continue_value)

# TODO: Can one fake a continue?
# TODO: Maybe use cmsort=sortkey together with gcmstarthexsortkey?
def upsert_category_members(members: Members, database: str = "tropes.db", category: str = 'Trope'):
    with closing(sqlite3.connect(database)) as connection:
        create_tropes = "CREATE TABLE IF NOT EXISTS tropes (id INTEGER PRIMARY KEY, title TEXT)"
        create_categories = "CREATE TABLE IF NOT EXISTS categories (id INTEGER PRIMARY KEY, title TEXT)"
        create_members = """CREATE TABLE IF NOT EXISTS members (
                            category_id INTEGER, 
                            member_id INTEGER, 
                            PRIMARY KEY (category_id, member_id),
                            FOREIGN KEY (category_id) REFERENCES categories(id),
                            FOREIGN KEY (member_id) REFERENCES tropes(id))"""
        upsert_category = """INSERT INTO categories(id, title) VALUES(?, ?) 
                            ON CONFLICT(id) DO UPDATE SET title=title;"""
        upsert_trope = """INSERT INTO tropes(id, title) VALUES(?, ?) 
                            ON CONFLICT(id) DO UPDATE SET title=title;"""
        upsert_membership = """INSERT INTO members(category_id, member_id) VALUES(?, ?) 
                                ON CONFLICT(category_id, member_id) DO NOTHING;"""

        cursor = connection.cursor()

        cursor.execute(create_tropes)
        cursor.execute(create_categories)
        cursor.execute(create_members)

        for ((member_id, member_title), categories) in members.items():
            # Nota bene: Bound parameters handle apostrophes and quotation marks in values...
            if member_title.startswith('Category:'):
                cursor.execute(upsert_category, (member_id, member_title.removeprefix('Category:')))
            else:
                cursor.execute(upsert_trope, (member_id, member_title))

            for (category_id, category_title) in categories:
                #print(f'{(category_id, category_title)} {(category_id, member_id)}')
                cursor.execute(upsert_category, (category_id, category_title.removeprefix('Category:')))
                cursor.execute(upsert_membership, (category_id, member_id))

        connection.commit()


def show_categories(database: str = "tropes.db"):
    with closing(sqlite3.connect(database)) as connection:
        select = """
        SELECT id, title, (SELECT COUNT(1) FROM members WHERE category_id = id)
        FROM categories
        ;"""
        cursor = connection.cursor()
        cursor.execute(select)
        rows = cursor.fetchall()
        print(list(map(lambda x: x[0], cursor.description)))
        for row in rows:
            print(row)
        print(f'Fetched {len(rows)} rows...')  

def show_tropes(database: str = "tropes.db"):
    with closing(sqlite3.connect(database)) as connection:
        select = """SELECT id, title FROM tropes;"""
        cursor = connection.cursor()
        cursor.execute(select)
        rows = cursor.fetchall()
        print(list(map(lambda x: x[0], cursor.description)))
        for index, row in enumerate(rows):
            print(row)          
        print(f'Fetched {len(rows)} rows...')  

def show_categories_grouped_by_trope(database: str = "tropes.db"):
    with closing(sqlite3.connect(database)) as connection:
        # Nota bene: A nice alternative to UNION ALL
        select = """SELECT 
                        member_id, 
                        CASE WHEN tropes.title IS NULL THEN m.title ELSE tropes.title END AS member_title,
                        GROUP_CONCAT(category_id || ':' || categories.title)
                    FROM members
                    INNER JOIN tropes ON tropes.id = member_id
                    LEFT JOIN categories ON categories.id = category_id
                    LEFT JOIN categories AS m ON m.id = member_id
                    GROUP BY member_id;"""

        cursor = connection.cursor()
        cursor.execute(select)
        rows = cursor.fetchall()
        print(list(map(lambda x: x[0], cursor.description)))
        for index, row in enumerate(rows):
            print(row)          
        print(f'Fetched {len(rows)} rows...')  

def show_members(database: str = "tropes.db"):
    with closing(sqlite3.connect(database)) as connection:
        select = """SELECT category_id, categories.title, member_id, tropes.title
                    FROM members
                    INNER JOIN tropes ON tropes.id = member_id
                    INNER JOIN categories ON categories.id = category_id
        
                    UNION ALL
                    
                    SELECT category_id, categories.title, member_id, m.title
                    FROM members
                    INNER JOIN categories ON categories.id = category_id
                    INNER JOIN categories AS m ON m.id = member_id
                    
                    ORDER BY category_id
                    ;"""

        cursor = connection.cursor()
        cursor.execute(select)
        rows = cursor.fetchall()
        print(list(map(lambda x: x[0], cursor.description)))
        for index, row in enumerate(rows):
            print(row)          
        print(f'Fetched {len(rows)} rows...')  

def show_categories_that_are_members(database: str = "tropes.db"):
    with closing(sqlite3.connect(database)) as connection:
        select = """SELECT DISTINCT member_id, m.title
                    FROM members 
                    INNER JOIN categories AS m ON m.id = member_id;"""

        cursor = connection.cursor()
        cursor.execute(select)
        rows = cursor.fetchall()
        print(list(map(lambda x: x[0], cursor.description)))
        for index, row in enumerate(rows):
            print(row)          
        print(f'Fetched {len(rows)} rows...')  

def show_categories_grouped_by_category(database: str = "tropes.db"):
    with closing(sqlite3.connect(database)) as connection:
        select = """SELECT 
                        member_id, m.title,
                        GROUP_CONCAT(category_id || ':' || categories.title)
                    FROM members
                    INNER JOIN categories ON categories.id = category_id
                    INNER JOIN categories AS m ON m.id = member_id
                    GROUP BY member_id;"""

        cursor = connection.cursor()
        cursor.execute(select)
        rows = cursor.fetchall()
        print(list(map(lambda x: x[0], cursor.description)))
        for index, row in enumerate(rows):
            print(row)          
        print(f'Fetched {len(rows)} rows...')  


def show_hierachy(database: str = "tropes.db", limit: int = 100):
    with closing(sqlite3.connect(database)) as connection:
        
        # we have a graph where any node can have 0,N children
        # the leaves will have 0 children
        # so we recurse up from the leaves AKA tropes in our case...
        # this will fail for cyclic graphs...
        # 1. purge the graph of cycles...
        # 2. https://docs.python.org/2/library/sqlite3.html#sqlite3.Connection.create_function
        #       since SQLite does not have a stored function/stored procedure language
        # 3. limit recursion the moment a value repeats in any given path...

        # the initial-select will be a row for each trope category i.e. the first edge
        # the recursive-select will take each of these in turn and produce more
        # each row is a unique path
        # then need to limit the path...

        recursive_select = f"""WITH RECURSIVE member_hierarchy AS (
                            --- Start with the tropes...
                            SELECT DISTINCT
                                member_id,
                                --- CASE WHEN tropes.title IS NULL THEN m.title ELSE tropes.title END AS member_title,
                                tropes.title,
                                category_id,
                                --- c.title,
                                c.title AS path
                            FROM members
                            INNER JOIN tropes ON tropes.id = member_id
                            LEFT JOIN categories AS c ON c.id = category_id
                            --- LEFT JOIN categories AS m ON m.id = member_id
                            
                            UNION --- ALL
                            
                            --- Move up ...
                            SELECT DISTINCT
                                members.member_id,
                                m.title,
                                members.category_id,
                                --- c.title,
                                c.title || '->' || member_hierarchy.path
                            FROM members, member_hierarchy
                            INNER JOIN categories AS c ON c.id = members.category_id
                            INNER JOIN categories AS m ON m.id = members.member_id
                            WHERE members.member_id = member_hierarchy.category_id
                            
                            LIMIT {limit}
                            )
                            SELECT *
                            FROM member_hierarchy
                            ORDER BY member_hierarchy.member_id;"""
        cursor = connection.cursor()
        cursor.execute(recursive_select)
        rows = cursor.fetchall()
        print(list(map(lambda x: x[0], cursor.description)))
        for index, row in enumerate(rows):
            print(row)          
        print(f'Fetched {len(rows)} rows...')  

def show_hierachy2(database: str = "tropes.db", limit: int = 100, min_members: int = 10):
    
    # Nota bene: This does what I want but, since many paths exist, and each row is a unique path,
    # the number of rows expodes... so we use min_members to restrict the number of results from initial_select,
    # which in turn will influence the results from recursive_select. The limit (LIMIT in recursive_select) is
    # used to prevent endless recursion...
    #
    # (167476, 'Category:Seekers', 234656, 'Category:Narrative Tropes->Category:Always Female->Category:Seekers')
    # (167476, 'Category:Seekers', 234656, 'Category:Narrative Tropes->Category:Character Flaw Index->Category:Seekers')
    # (167476, 'Category:Seekers', 234656, 'Category:Narrative Tropes->Category:Characterization Tropes->Category:Seekers')
    # ...
    # (167476, 'Category:Seekers', 234656, 'Category:Narrative Tropes->Category:Villains->Category:Seekers')

    # The result is still odd as Take My Hand (trop with pageid 164002) is a member of Trope,     
    # Take My Hand (category with pageid 247057), Ending Tropes, Just in Time Tropes, Drama Tropes, 
    # Climbing the Cliffs of Insanity, Hand Tropes...
    # and NOT Twist Ending or Happy Ending...
    # also, we have Take My Hand (category with pageid 247057) as a category, but initial_select 
    # does not include it...
    #
    # (164002, 'Take My Hand', 234232, 'Trope=>Take My Hand')
    # (164002, 'Take My Hand', 234488, 'Ending Tropes=>Take My Hand')
    # (164002, 'Take My Hand', 234232, 'Trope->Twist Ending=>Take My Hand')
    # (164002, 'Take My Hand', 234488, 'Ending Tropes->Twist Ending=>Take My Hand')
    # (164002, 'Take My Hand', 234488, 'Ending Tropes->Happy Ending=>Take My Hand')
    # (164002, 'Take My Hand', 234488, 'Ending Tropes->Twist Ending->Happy Ending=>Take My Hand')
    # (164002, 'Take My Hand', 234488, 'Ending Tropes->Happy Ending->Twist Ending=>Take My Hand')



    with closing(sqlite3.connect(database)) as connection:    
        recursive_select = f"""WITH RECURSIVE member_hierarchy AS (
                            --- Start with the categories that are not members...
                            --- and have more than min_members themselves...
                            SELECT
                                1 AS iter,
                                NULL AS category_id,
                                NULL AS category_title,
                                id AS member_id,
                                title AS path
                            FROM categories ---AS initial_select
                            --- Performance is fine if we have only a few top-level categories,
                            --- which we influence with min_members...
                            WHERE NOT EXISTS (SELECT 1 FROM members WHERE members.member_id = categories.id)
                            AND (
                                SELECT COUNT(1) 
                                FROM members
                                ---INNER JOIN tropes ON tropes.id = members.member_id
                                WHERE members.category_id = categories.id
                            ) > {min_members}
                                                        
                            --- UNION ALL is fine here and is quicker than UNION which filters...
                            UNION ALL
                            
                            --- Move down the categories ...
                            SELECT
                                member_hierarchy.iter + 1 AS iter,
                                members.category_id AS category_id,
                                c.title,
                                members.member_id AS member_id,
                                member_hierarchy.path || '->' || m.title AS path
                            FROM members, member_hierarchy
                            INNER JOIN categories AS c ON c.id = members.category_id
                            INNER JOIN categories AS m ON m.id = members.member_id
                            WHERE members.category_id = member_hierarchy.member_id 
                            
                            UNION ALL

                            --- Move down the tropes ...
                            SELECT
                                member_hierarchy.iter + 1 AS iter,
                                members.category_id AS category_id,
                                c.title,
                                members.member_id AS member_id,
                                member_hierarchy.path || '=>' || m.title AS path
                            FROM members, member_hierarchy
                            INNER JOIN tropes AS m ON m.id = members.member_id
                            INNER JOIN categories AS c ON c.id = members.category_id
                            WHERE members.category_id = member_hierarchy.member_id 

                            LIMIT {limit}
                            )

                            --- We had to build the entire member_hierarchy but only care about the tropes...
                            SELECT member_hierarchy.*
                            FROM member_hierarchy
                            ---INNER JOIN tropes ON tropes.id = member_hierarchy.member_id
                            ---ORDER BY member_id
                            ---ORDER BY member_hierarchy.member_id
                            ;"""
        cursor = connection.cursor()
        cursor.execute(recursive_select)
        rows = cursor.fetchall()
        print(list(map(lambda x: x[0], cursor.description)))
        for index, row in enumerate(rows):
            print(row)          
        print(f'Fetched {len(rows)} rows...')  

# Rather than build the members table, each batch could be flattened as it is obtained from 
# the server. We would like to move up the hierarchy matching on the last id associated with
# the leaf. An efficient way to know that it is a leaf in this case is categorymembers on 
# category 'Trope' (which oddly has subcat...), with prop=categories and gcmtype=page (to 
# avoid subcat and file).

# https://allthetropes.org/w/api.php?action=query&format=json&maxlag=1&prop=categories&generator=categorymembers&formatversion=2&gcmtitle=Category%3A%20Trope&gcmtype=page

# It begs the question why I used linkshere...
# It was as 'categories' does not provide its id...
# These can of course be used to obtain the id...

# The allpages has few ways to limit the response; there is by prefix but the tropes do not have
# a prefix. There are 325,140 pages. Many we can identify by prefix e.g. 'Category:' or 'File:'
# others would be subpages e.g. <>/Laconic... we can ask for the prop 'categories' to obtain
# the direct ancestors. Then the tropes would be (hopefully) what is left over...

# def get_category_members(category: str ='Trope', limit: int = 100, max_members: int = None):
#     try: 
#         t0 = time.time()
#         page = category_page(category=category)
#         for members in category_members(category=category, gcmlimit=limit, max_members=max_members):
#             print(f"category_members returned {len(members)} members.")    
#             for member in members.keys(): # should add Ending Tropes as parent of each member...
#                 members[member].append(page)
#             upsert_category_members(members)
#     except Exception as e:
#         print(traceback.format_exc())
#     else:
#         print('bazinga!')
#     finally:
#         t1 = time.time()
#         print(f"...took {t1-t0:.2f}s")

def get_category_members2(category: str ='Trope', gcmlimit: int = 100, cllimit: int = 20, max_members: int = None):
    try: 
        t0 = time.time()
        for members in category_members(category=category, exclude_pages=site_maintenance_pages(), gcmlimit=gcmlimit, cllimit=cllimit, max_members=max_members):
            print(f"category_members returned {len(members)} members.")
            upsert_category_members(members)
    except Exception as e:
        print(traceback.format_exc())
    else:
        print('bazinga!')
    finally:
        t1 = time.time()
        print(f"...took {t1-t0:.2f}s")

# This will get all the tropes, plus some categories that belong to 'Trope',
# it uses 'linkshere' to grab *only* categories that reference these pages,
# i.e. categories that reference other categories and categories that reference tropes...


#get_category_members(limit=100, max_members=None)
# 'Anime_Creators' contains 1 category and 1 page so is a great test...
#get_category_members(category='Anime Creators')



# get_category_members2(category='Literature')
# get_category_members2(category='Plots')

#get_category_members2(category='Twist Ending')
# get_category_members2(category='Happy Ending')
# get_category_members2(category='Ending Tropes')

# get_category_members2(category='Narrative Tropes')

# An example of a category that contains a page with the same title once prefix removed...
#get_category_members2(category='Romance Arc')


# We seem to be getting some subpages...
# TODO: grab the Laconic to enrich the description...
# (61644, 'Twist Ending/Laconic', 235647, 'Twist Ending')

# The 'Index' in the title does not seem to be indicative of anything unusual so
# it could just as easily be titled 'Character Flaw'...
# (81025, 'Character Flaw Index', 234656, 'Narrative Tropes->Character Flaw Index')
# Transcluded templates (3)	
#     Template:IndexTrope (view source) (protected)
#     Template:Reflist (view source) (protected)
#     Module:Subpages 6 (view source) (protected)

print("show_tropes")
show_tropes()
print("show_categories_grouped_by_trope")
# # # Nota bene: This will GROUP_CONCAT to show the parents rather than the entire hierarchy...
show_categories_grouped_by_trope()
print("show_categories")
show_categories()
# # # 234488 Ending Tropes -> 248675 Happy Ending
print("show_members")
show_members()
print("show_categories_that_are_members")
show_categories_that_are_members()
print("show_categories_grouped_by_category")
show_categories_grouped_by_category()
t0 = time.time()
print("show_hierachy")
# show_hierachy(limit=10)
# (2, 234604, 'Romance Arc', 26356, 'Romance Arc=>Romance Arc')
show_hierachy2(limit=1000, min_members=20)
t1 = time.time()
print(f"...took {t1-t0:.2f}s")