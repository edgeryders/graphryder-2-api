import psycopg2
import time
import json
import os
from sys import exit
from neo4j import GraphDatabase
from pprint import pprint


# For this script to work, neo4j must have APOC installed and the neo4j.conf file 
# must have the following properties set:
# apoc.import.file.enabled=true
# apoc.import.file.use_neo4j_config=false

# TODO: Move the database list to a config file
# TODO: Trigger reload from database through parameter when running the script

databases = [
    {   
        'name': 'edgeryders',
        'host': 'localhost',
        'port': '5432',
        'dbname': 'erbackup',
        'user': 'postgres',
        'password': ''
    },
    {
        'name': 'bbu',
        'host': 'localhost',
        'port': '5432',
        'dbname': 'bbubackup',
        'user': 'postgres',
        'password': ''
    },
    {
        'name': 'blivande',
        'host': 'localhost',
        'port': '5432',
        'dbname': 'blivandebackup',
        'user': 'postgres',
        'password': ''
    }]

reload_from_database = True

start_time = time.time()
print("--- %s seconds ---" % (round(time.time() - start_time,2)))

def get_data(db_cursor, db_name):
    # This function gets the data we need from the Discourse psql database.
    # It assumes that the database is built from backup dumps. 
    # If running on the live database, 'backup' in the database names should be changed.
    # TODO: make the database name into a variable to enable loading from backup or live db

    print(f'Loading new data from {db_name}')

    # Get site data

    site_query = """
    SELECT
    value
    FROM backup.site_settings
    WHERE name = 'vapid_base_url'
    LIMIT 1
    """
    db_cursor.execute(site_query)
    site_data = db_cursor.fetchall()
    site = {
        'name': db_name,
        'url': site_data[0][0]
    }
    print(f'    Loading data from {site["url"]} database...')

    # Get users, consent, group memberships

    users_query = """
    SELECT
    users.id, username_lower, email
    FROM backup.users AS users, backup.user_emails as emails
    WHERE users.id = emails.user_id;
    """

    consent_query = """
    SELECT 
    user_id, value, updated_at 
    FROM backup.user_custom_fields 
    WHERE name = 'edgeryders_consent';
    """

    group_members_query = """
    SELECT
    group_id, user_id
    FROM backup.group_users
    """

    users = {}
    db_cursor.execute(users_query)
    users_data = db_cursor.fetchall()
    for user in users_data:
        uid = user[0]
        users[uid] = {
            'id': uid,
            'username': user[1],
            'email': user[2],
            'groups': [],
            'consent': 0,
            'consent_updated': 0
        }
    
    # Adding another system user as a dummy user for private and deleted content
    users[-100] = {
        'id': -100,
        'username': "Unknown",
        'email': "Unknown",
        'groups': [],
        'consent': 0,
        'consent_updated': 0
    }

    db_cursor.execute(consent_query)
    consent_data = db_cursor.fetchall()
    for user in consent_data:
        uid = user[0]
        users[uid]['consent'] = user[1]
        users[uid]['consent_updated'] = user[2]

    db_cursor.execute(group_members_query)
    group_members_data = db_cursor.fetchall()
    for group_member in group_members_data:
        uid = group_member[1]
        users[uid]['groups'].append(group_member[0])

    print(f'    Got {len(users.keys())} users')

    # Get groups

    groups_query = """
    SELECT 
    id, name, visibility_level 
    FROM backup.groups
    """

    groups = {}
    db_cursor.execute(groups_query)
    group_data = db_cursor.fetchall()
    for group in group_data:
        gid = group[0]
        groups[gid] = {
            'id': gid,
            'name': group[1],
            'visibility_level': group[2]
        }

    print(f'    Got {len(groups.keys())} groups')

    # Get categories

    categories_query = """
    SELECT
    id, name, name_lower, created_at, updated_at, read_restricted, parent_category_id
    FROM backup.categories
    """

    categories_permissions = """
    SELECT
    id, category_id, group_id, permission_type
    FROM backup.category_groups
    """

    categories = {}
    db_cursor.execute(categories_query)
    category_data = db_cursor.fetchall()
    for category in category_data:
        cid = category[0]
        categories[cid] = {
            'id': cid,
            'name': category[1], 
            'name_lower': category[2],
            'created_at': category[3], 
            'updated_at': category[4], 
            'read_restricted': category[5], 
            'parent_category_id': category[6],
            'permissions': []
        }

    # Group 0 is 'everyone' and permission_type is an integer 1 = Full 2 = Reply and read 3 = Read Only
    db_cursor.execute(categories_permissions)
    category_permission_data = db_cursor.fetchall()
    for permission in category_permission_data:
        cid = permission[1]
        categories[cid]['permissions'].append(permission[2])

    print(f'    Got {len(categories.keys())} categories')

    # Get tags

    tags_query = """
    SELECT
    id, name, topic_count, created_at, updated_at
    FROM backup.tags
    """

    tags = {}
    db_cursor.execute(tags_query)
    tags_data = db_cursor.fetchall()
    for tag in tags_data:
        tid = tag [0]
        tags[tid] = {
            'id': tid,
            'name': tag[1],
            'topic_count': tag[2],
            'created_at': tag[3],
            'updated_at': tag[4]
        }

    print(f'    Got {len(tags.keys())} tags.')

    # Get topics, permissions, topic tags
    # Private messages are excluded

    topics_query = """
    SELECT
    id, title, created_at, updated_at, user_id, category_id
    FROM backup.topics
    """

    allowed_users_query = """
    SELECT 
    topic_id, user_id
    FROM backup.topic_allowed_users
    """

    topic_tags_query = """
    SELECT
    topic_id, tag_id
    FROM backup.topic_tags
    """

    pm_count = 0
    pm_topic_set = set()
    db_cursor.execute(allowed_users_query)
    allowed_users_data = db_cursor.fetchall()
    for permission in allowed_users_data:
        tid = permission[0]
        pm_topic_set.add(tid)
        pm_count += 1

    topics = {}
    db_cursor.execute(topics_query)
    topics_data = db_cursor.fetchall()
    lost_topics = set()
    for topic in topics_data:
        tid = topic[0]
        cid = topic[5] if topic[5] in categories.keys() else None
        read_restricted = True if tid in pm_topic_set or not cid else categories[cid]['read_restricted']
        topics[tid] = {
            'id': tid,
            'title': 'Private message' if tid in pm_topic_set else topic[1],
            'created_at': topic[2],
            'updated_at': topic[3],
            'user_id': -100 if tid in pm_topic_set or topic[4] not in users.keys() else topic[4],
            'is_message_thread': True if tid in pm_topic_set else False,
            'category_id': cid,
            'read_restricted': read_restricted,
            'allowed_users': [],
            'tags': []
        }
        if topic[4] not in users.keys():
            lost_topics.add(tid)

    db_cursor.execute(topic_tags_query)
    topic_tags_data = db_cursor.fetchall()
    for tag in topic_tags_data:
        tid = tag[0]
        topics[tid]['tags'].append(tag[1])

    print(f'    Got {len(topics.keys())} topics and applied {len(topic_tags_data)} tags.')

    # Get posts
    # Private messages are excluded

    posts_query = """
    SELECT
    id, user_id, topic_id, post_number, raw, created_at, updated_at, deleted_at, hidden, word_count, wiki, reads, score, like_count, reply_count, quote_count
    FROM backup.posts
    """

    replies_query = """
    SELECT
    post_id, reply_post_id
    FROM backup.post_replies
    """

    quotes_query = """
    SELECT
    post_id, quoted_post_id
    FROM backup.quoted_posts
    """

    likes_query = """
    SELECT
    post_id, user_id
    FROM backup.post_actions
    WHERE post_action_type_id = 2
    """

    posts = {}
    pm_post_set = set()
    private_count = 0
    db_cursor.execute(posts_query)
    posts_data = db_cursor.fetchall()
    for post in posts_data:
        pid = post[0]
        tid = post[2]
        private = True if post[2] in pm_topic_set else False
        read_restricted = True if tid not in topics.keys() else topics[tid]['read_restricted']
        if private:
            pm_post_set.add(pid)
            private_count += 1
        deleted = post[7]
        posts[pid] = {
            'id': pid,
            'user_id': -100 if private or deleted or post[1] not in users.keys() else post[1],
            'topic_id': tid,
            'post_number': post[3],
            'raw': 'Removed content' if private or deleted else post[4],
            'created_at': post[5],
            'updated_at': post[6],
            'deleted_at': post[7],
            'hidden': post[8],
            'read_restricted': read_restricted,
            'word_count': 0 if private or deleted else post[9],
            'wiki': post[10],
            'reads': 0 if private or deleted else post[11],
            'score': 0 if private or deleted else post[12],
            'like_count': 0 if private or deleted else post[13],
            'reply_count': post[14],
            'quote_count': post[15],
            'quotes_posts': [],
            'is_reply_to': [],
            'is_liked_by': [],
            'is_private': private
        }

    quotes = {}
    db_cursor.execute(quotes_query)
    quotes_data = db_cursor.fetchall()
    for num, quote in enumerate(quotes_data):
        posts[quote[0]]['quotes_posts'].append(quote[1])
        quotes[num] = {
            'id': num,
            'post_id': quote[0],
            'quoted_post_id': quote[1]
        }

    replies = {}
    db_cursor.execute(replies_query)
    replies_data = db_cursor.fetchall()
    for num, reply in enumerate(replies_data):
        posts[reply[1]]['is_reply_to'].append(reply[0])
        replies[num] = {
            'id': num,
            'post_id': reply[0],
            'reply_post_id': reply[1]
        }

    likes = {}
    db_cursor.execute(likes_query)
    likes_data = db_cursor.fetchall()
    for num, like in enumerate(likes_data):
        posts[like[0]]['is_liked_by'].append(like[1])
        likes[num] = {
            'id': num,
            'post_id': like[0],
            'user_id': like[1]
        }

    print(f'    Got {len(posts.keys())} posts.')
    print(f'    Got {len(replies.keys())} replies.')
    print(f'    Got {len(quotes.keys())} quotes.')
    print(f'    Got {len(likes.keys())} likes.')

    # Get annotator languages

    annotator_languages_query = """
    SELECT
    id, name, locale
    FROM backup.annotator_store_languages
    """

    annotator_languages = {}
    language_list = ''
    db_cursor.execute(annotator_languages_query)
    annotator_languages_data = db_cursor.fetchall()
    for language in annotator_languages_data:
        lid = language[0]
        annotator_languages[lid] = {
            'id': lid,
            'name': language[1],
            'locale': language[2]
        }
        language_list += f' {language[1]},'

    print(f'    Got annotation languages:{language_list[:-1]}.') 

    # Get annotator codes and code names

    annotator_codes_query = """
    SELECT
    id, description, creator_id, created_at, updated_at, ancestry, annotations_count
    FROM backup.annotator_store_tags
    """

    annotator_code_names_query = """
    SELECT
    id, name, tag_id, language_id, created_at
    FROM backup.annotator_store_tag_names
    """

    annotator_codes = {}
    db_cursor.execute(annotator_codes_query)
    annotator_codes_data = db_cursor.fetchall()
    for code in annotator_codes_data:
        cid = code[0]
        annotator_codes[cid] = {
            'id': cid,
            'description': code[1],
            'creator_id': code[2],
            'created_at': code[3],
            'updated_at': code[4],
            'ancestry': code[5],
            'annotations_count': code[6]
        }

    annotator_code_names = {}
    db_cursor.execute(annotator_code_names_query)
    annotator_code_names_data = db_cursor.fetchall()
    for name in annotator_code_names_data:
        nid = name[0]
        annotator_code_names[nid] = {
            'id': nid,
            'name': name[1],
            'tag_id': name[2],
            'language_id': name[3],
            'created_at':name[4]
        }

    print(f'    Got {len(list(annotator_codes.keys()))} codes with {len(list(annotator_code_names.keys()))} names.')

    # Get annotator annotations and ranges

    annotations_query = """
    SELECT
    id, text, quote, created_at, updated_at, tag_id, post_id, creator_id, type, topic_id
    FROM backup.annotator_store_annotations
    """

    annotations = {}
    db_cursor.execute(annotations_query)
    annotations_data = db_cursor.fetchall()
    for annotation in annotations_data:
        aid = annotation[0]
        annotations[aid] = {
            'id': aid,
            'text': annotation[1], 
            'quote': annotation[2], 
            'created_at': annotation[3],
            'updated_at': annotation[4], 
            'tag_id': annotation[5],
            'post_id': annotation[6], 
            'creator_id': annotation[7], 
            'type': annotation[8],
            'topic_id': annotation[9] 
        }

    print(f'    Got {len(list(annotations.keys()))} annotations.')

    # Omit data
    # We accept the redundancy and inefficency of looping through the data
    # and removing records as this makes the code less complicated
    # and makes it easier to add new rules later for how and when to omit data
    # TODO: Move these bools to a config file
    
    omit_private_messages = True
    # Omit private messages from the graph

    omit_protected_content = True
    # Omit protected content (posts, categories, groups) from the graph.
    # Content that is not readable by all logged in users is considered protected.
    # This also omits 'hidden' posts, also known as 'whispers'.
    # In the future, we may want to handle 'hidden' posts separately if we 
    # if we want to give access to the graph based on the permissions of a loggen in user.

    omit_system_users = True
    # This omits content created by system users and by deleted users.
    # It also omits those users completely from the graph.

    omit = True if omit_private_messages or omit_protected_content or omit_system_users else False

    if omit:

        new = dict(users)
        for u, d in users.items():
            if omit_system_users and d['id'] < 0:
                del(new[u])
                continue
        users = new

        new = dict(groups)
        # Group visibility levels, public=0, logged_on_users=1, members=2, staff=3, owners=4
        for g, d in groups.items():
            if omit_protected_content and d['visibility_level'] > 1:
                del(new[g])
                continue
        groups = new

        new = dict(categories)
        for c, d in categories.items():
            if omit_protected_content and d['read_restricted']:
                del(new[c])
                continue
            for group in d['permissions']:
                if group not in groups.keys():
                    new[c]['permissions'].remove(group)
        categories = new

        new = dict(topics)
        for t, d in topics.items():
            if omit_private_messages and t in pm_topic_set:
                del(new[t])
                continue
            if omit_protected_content and d['read_restricted']:
                del(new[t])
                continue
        topics = new

        new = dict(posts)
        for p, d in posts.items():
            if omit_private_messages and p in pm_post_set:
                del(new[p])
                continue
            if omit_protected_content and (d['read_restricted'] or d['hidden']):
                del(new[p])
                continue
        posts = new

        new = dict(quotes)
        for q, d in quotes.items():
            if omit_private_messages and (d['quoted_post_id'] in pm_post_set or d['post_id'] in pm_post_set):
                del(new[q])
                continue
            if omit_protected_content and (d['quoted_post_id'] in pm_post_set or d['post_id'] not in posts.keys()):
                del(new[q])
                continue
        quotes = new
        
        new = dict(likes)
        for l, d in likes.items():
            if omit_private_messages and d['post_id'] in pm_post_set:
                del(new[l])
                continue
            if omit_protected_content and d['post_id'] not in posts.keys():
                del(new[l])
                continue
        likes = new

        new = dict(annotations)
        for a, d in annotations.items():
            if omit_private_messages and d['post_id'] in pm_post_set:
                del(new[a])
                continue
            if omit_protected_content and d['post_id'] not in posts.keys():
                del(new[a])
                continue
        annotations = new

        if omit_private_messages:
            print('Omitted private messages.')
        if omit_protected_content:
            print('Omitted protected content.')
        if omit_system_users:
            print('Omitted system users and content.')

    if omit_protected_content:
        pass

    if omit_system_users:
        pass
    
    stats = {
        'omit_pm': omit_private_messages,
        'omit_protected': omit_protected_content,
        'omit_system_users': omit_system_users,
        'users': len(users.keys()),
        'groups': len(groups.keys()),
        'tags': len(tags.keys()),
        'categories': len(categories.keys()),
        'topics': len(topics.keys()),
        'pm_threads': pm_count,
        'topics_by_deleted_users': len(lost_topics),
        'tags_applied': len(topic_tags_data),
        'posts': len(posts.keys()),
        'messages': private_count,
        'annotator_languages': language_list[:-1],
        'annotator-codes': len(list(annotator_codes.keys())),
        'annotator-code-names': len(list(annotator_code_names.keys())),
        'annotator-annotations': len(list(annotations.keys()))
    }

    return {
        'stats': stats,
        'site': site,
        'users': users,
        'groups': groups,
        'tags': tags,
        'categories': categories,
        'topics': topics,
        'posts': posts,
        'replies': replies,
        'quotes': quotes,
        'likes': likes,
        'languages': annotator_languages,
        'codes': annotator_codes,
        'code_names': annotator_code_names,
        'annotations': annotations
    }

def reload_data(dbs):
    # This function triggers loading data from all databases it gets as input.
    # It outputs the data into chunked json files in the 'db' directory.
    # This is done as the APOC calls of Neo4j work best when loading from files in chunks.
    # Chunk size is 1000 records.
    # TODO: Set chunk size through parameter when loading script with reload flag.

    print('Loading new data from databases...')
    db_path = './db'
    try:
        os.mkdir(db_path)
    except OSError:
        print ("Database directory %s" % db_path)
    else:
        print ("Successfully created the directory %s " % db_path)

    data = {}
    for db in dbs:

        db_conn = psycopg2.connect(
            host=db['host'], 
            port=db['port'], 
            dbname=db['dbname'], 
            user=db['user'], 
            password=db['password']
        )

        # Open a cursor to perform database operations
        db_cursor = db_conn.cursor()

        # Get data
        d = get_data(db_cursor, db['name'])
        data[db['name']] = d
        stats = d['stats']
        stats['chunk_sizes'] = {}

        # Site data is always a single object
        with open(f'./db/{db["name"]}_site.json', 'w') as file:
            json.dump(d['site'], file, default=str)

        # Save data in chunks of size n
        def dumpSplit(data_topic, data_set, stats):
            path = './db/'
            n = 1000
            data_list = list(d[data_topic].values())
            data_chunks = [data_list[i * n:(i + 1) * n] for i in range((len(data_list) + n - 1) // n )]
            for num, item in enumerate(data_chunks):
                with open(f'{path}{data_set}_{data_topic}_{str(num+1)}.json', 'w') as file:
                    json.dump(item, file, default=str)
            stats['chunk_sizes'][data_topic] = len(data_chunks)
            return stats

        stats = dumpSplit('users', db['name'], stats)
        stats = dumpSplit('groups', db['name'], stats)
        stats = dumpSplit('tags', db['name'], stats)
        stats = dumpSplit('categories', db['name'], stats)
        stats = dumpSplit('topics', db['name'], stats)
        stats = dumpSplit('posts', db['name'], stats)
        stats = dumpSplit('replies', db['name'], stats)
        stats = dumpSplit('quotes', db['name'], stats)
        stats = dumpSplit('likes', db['name'], stats)
        stats = dumpSplit('languages', db['name'], stats)
        stats = dumpSplit('codes', db['name'], stats)
        stats = dumpSplit('code_names', db['name'], stats)
        stats = dumpSplit('annotations', db['name'], stats)

        # Add chunk sizes to stats last
        with open(f'./db/{db["name"]}_stats.json', 'w') as file:
            json.dump(stats, file, default=str)

def load_data(dbs):
    # This function is basically just a verification of that the data we need is in files in the db directory. 
    # TODO: Actually test data integrity before import?

    print('')
    print('Loading JSON data files to verify...')
    print('')
    data = {}
    for db in dbs:
        data[db['name']] = {}
        with open(f'./db/{db["name"]}_site.json') as file:
            data[db['name']]['site'] = json.load(file)
        with open(f'./db/{db["name"]}_stats.json') as file:
            data[db['name']]['stats'] = json.load(file)
            stats = data[db['name']]['stats']

        for topic, chunks in stats['chunk_sizes'].items():
            data[db['name']][topic] = []
            for chunk in range(1, chunks + 1):
                with open(f'./db/{db["name"]}_{topic}_{chunk}.json') as file:
                    data[db['name']][topic].extend(json.load(file))

    for k,d in data.items():
        print(f'-------| {k} |-------')
        print(f'{len(d["users"])} users')
        print(f'{len(d["groups"])} groups')
        print(f'{len(d["tags"])} tags.')
        print(f'{len(d["categories"])} tags.')
        print(f'{len(d["topics"])} topics and {d["stats"]["pm_threads"]} PM threads.')
        print(f'{d["stats"]["tags_applied"]} tag applications to topics.')
        print(f'{len(d["posts"])} posts and {d["stats"]["messages"]} private messages.')
        print(f'{len(d["replies"])} posts are replies to other posts.')
        print(f'{len(d["quotes"])} quotes.')
        print(f'{len(d["likes"])} likes.')
        
        if d['stats']['omit_pm']:
            print('Private messages have been omitted.')
        else:
            print('Private message content and message user identification has been omitted from the dataset.')

        if d['stats']['omit_protected']:
            print('Protected content has been omitted.')

        if d['stats']['omit_system_users']:
            print('System users and their content has been omitted.')

        print(f'Annotation languages:{d["stats"]["annotator_languages"]}.') 
        print(f'{len(d["codes"])} ethnographic codes with {len(d["code_names"])} names.')
        print(f'{len(d["annotations"])} ethnographic annotations.')
        print(' ')

    return data

# Load data from Discourse psql databases and dump to json files
# Data is loaded from JSON files because Neo4j APOC functions are optimized for this.
dbs = databases[:]
if reload_from_database:
    reload_data(dbs)
data = load_data(dbs)
print("--- %s seconds ---" % (round(time.time() - start_time,2)))

# Build Neo4j database

# TODO: Refactor 'for platform in data.values()' loop into function
# TODO: Refactor create index into function

print(' ')
print('Building Neo4j database...')
print(' ')
print("--- %s seconds ---" % (round(time.time() - start_time,2)))
uri = 'bolt://localhost:7687'
driver = GraphDatabase.driver(uri, auth=('neo4j', 'shen4yaya'))
data_path = os.path.abspath('./db/')

def graph_clear():
    # Clear database function

    def tx_clear_neo4j(tx):
        tx.run('MATCH (a) DETACH DELETE a')

    with driver.session() as session:
        try:
            session.write_transaction(tx_clear_neo4j)
            print('Cleared database')
        except Exception as e:
            print(e)

def graph_create_platform(data):
    # Add platforms function

    def tx_create_platform(tx, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_site.json")'
            f'YIELD value '
            f'CREATE (p:platform {{url: value.url, name: "{dataset}"}})'
        )

    def tx_create_platform_index(tx):
        tx.run(
            f'CREATE INDEX platform IF NOT EXISTS '
            f'FOR (p:platform) '
            f'ON (p.name) '
        )

    with driver.session() as session:
        session.write_transaction(tx_create_platform_index)

    for platform in data.values():
        with driver.session() as session:
            try:
                session.write_transaction(tx_create_platform, platform['site']['name'])
                print(f'Loaded platform data from {platform["site"]["name"]}')
            except Exception as e:
                print(f'Import failed for platform data on {platform["site"]["name"]}')
                print(e)

    print('Loaded all platforms.')

def graph_create_groups(data):
    # Add user groups function

    def tx_create_groups(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_groups_{chunk}.json") '
            f'YIELD value '
            f'MERGE (g:group {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET g.name = value.name '
            f'WITH g, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'WITH g, p '
            f'MERGE (p)<-[:ON_PLATFORM]-(g) '
        )

    def tx_create_group_index(tx):
        tx.run(
            f'CREATE INDEX group IF NOT EXISTS '
            f'FOR (g:group) '
            f'ON (g.discourse_id, g.platform) '
        )

    with driver.session() as session:
        session.write_transaction(tx_create_group_index)
        print('Created group index')

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'groups'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_groups, str(chunk), platform_name)
                    print(f'Loaded group data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    print(f'Import failed for groups on {platform_name}, chunk #{chunk}')
                    print(e)

    print('Added all groups')

def graph_create_users(data):
    # Add users function

    def tx_create_users(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_users_{chunk}.json") '
            f'YIELD value '
            f'MERGE (u:user {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET u.username = value.username '
            f'SET u.email = value.email '
            f'SET u.consent = value.consent '
            f'SET u.consent_updated = value.consent_updated '
            f'SET u.groups = value.groups '
            f'WITH u, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'WITH u, p, value '
            f'MERGE (p)<-[:ON_PLATFORM]-(u) '
            f'WITH u, value '
            f'UNWIND value.groups AS gids '
            f'MATCH (g:group {{discourse_id: gids, platform: "{dataset}"}}) '
            f'WITH u, g, value '
            f'CREATE (u)-[:IN_GROUP]->(g) '
            f'MERGE (global:globaluser {{email: value.email}}) '
            f'SET global.username = value.username '
            f'WITH global, u '
            f'MERGE (u)-[:IS_GLOBAL_USER]->(global)'
            f'WITH global '
            f'MATCH (p:platform {{name:"{dataset}" }}) '
            f'WITH p, global '
            f'MERGE (p)<-[:HAS_ACCOUNT_ON]-(global)'
        )

    def tx_create_user_index(tx):
        tx.run(
            f'CREATE INDEX user IF NOT EXISTS '
            f'FOR (u:user) '
            f'ON (u.discourse_id, u.platform) '
        )

    def tx_create_globaluser_index(tx):
        tx.run(
            f'CREATE INDEX global IF NOT EXISTS '
            f'FOR (g:globaluser) '
            f'ON (g.email) '
        )

    with driver.session() as session:
        session.write_transaction(tx_create_user_index)
        print('Created user index')

    with driver.session() as session:
        session.write_transaction(tx_create_globaluser_index)
        print('Created globaluser index')

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'users'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_users, chunk, platform_name)
                    print(f'Loaded user data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    print(f'Import failed for users on {platform_name}, chunk #{chunk}')
                    print(e)

    print('Added all users')

def graph_create_tags(data):
    # Add tags function

    def tx_create_tags(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_tags_{chunk}.json") '
            f'YIELD value '
            f'CREATE (tag:tag {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET tag.name = value.name '
            f'SET tag.topic_count = value.topic_count '
            f'SET tag.created_at = value.created_at '
            f'SET tag.updated_at = value.updated_at '
            f'WITH tag, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'WITH tag, p, value '
            f'CREATE (p)<-[:ON_PLATFORM]-(tag) '
        )

    def tx_create_tag_index(tx):
        tx.run(
            f'CREATE INDEX tag IF NOT EXISTS '
            f'FOR (t:tag) '
            f'ON (t.discourse_id, t.platform) '
        )

    with driver.session() as session:
        session.write_transaction(tx_create_tag_index)
        print('Created tag index')

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'tags'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_tags, chunk, platform_name)
                    print(f'Loaded tag data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    print(f'Import failed for tag on {platform_name}, chunk #{chunk}')
                    print(e)

    print('Added all tags')

def graph_create_categories(data):
    # Add categories

    def tx_create_categories(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_categories_{chunk}.json") '
            f'YIELD value '
            f'MERGE (c:category {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET c.name = value.name '
            f'SET c.name_lower = value.name_lower '
            f'SET c.created_at = value.created_at '
            f'SET c.updated_at = value.updated_at '
            f'SET c.read_restricted = value.read_restricted '
            f'SET c.parent_category_id = value.parent_category_id '
            f'SET c.permissions = value.permissions '
            f'WITH c, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'CREATE (p)<-[:ON_PLATFORM]-(c) '
            f'WITH c, value '
            f'UNWIND value.permissions AS permissions '
            f'MATCH (g:group {{discourse_id: permissions, platform: "{dataset}"}}) '
            f'MERGE (g)-[:HAS_ACCESS]->(c) '
            f'WITH c, value '
            f'CALL apoc.do.when(value.parent_category_id IS NOT NULL,'
            f'"MERGE (c)<-[:PARENT_CATEGORY_OF]-(ca:category {{discourse_id: value.parent_category_id, platform: dataset}})",'
            f'"",'
            f'{{c:c, value:value, dataset: c.platform}}) '
            f'YIELD value AS value2 '
            f'RETURN value2 '
        )

    def tx_create_category_index(tx):
        tx.run(
            f'CREATE INDEX categories IF NOT EXISTS '
            f'FOR (g:category) '
            f'ON (g.discourse_id, g.platform) '
        )

    with driver.session() as session:
        session.write_transaction(tx_create_category_index)
        print('Created category index')

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'categories'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_categories, chunk, platform_name)
                    print(f'Loaded category data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    print(f'Import failed for categories on {platform_name}, chunk #{chunk}')
                    print(e)

    print('Added all categories')

def graph_create_topics(data):
    # Add topics

    def tx_create_topics(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_topics_{chunk}.json") '
            f'YIELD value '
            f'CREATE (t:topic {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET t.title = value.title '
            f'SET t.created_at = value.created_at '
            f'SET t.updated_at = value.updated_at '
            f'SET t.user_id = value.user_id '
            f'SET t.is_message_thread = value.is_message_thread '
            f'SET t.tags = value.tags '
            f'SET t.category_id = value.category_id '
            f'WITH t, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'CREATE (p)<-[:ON_PLATFORM]-(t) '
            f'WITH t, value '
            f'MATCH (c:category {{discourse_id: value.category_id, platform: "{dataset}"}}) '
            f'CREATE (c)<-[:IN_CATEGORY]-(t) '
            f'WITH t, value '
            f'MATCH (u:user {{discourse_id: value.user_id, platform: "{dataset}"}}) '
            f'CREATE (t)<-[:CREATED]-(u) '
            f'WITH t, value '
            f'UNWIND value.tags AS tagids '
            f'MATCH (tag:tag {{discourse_id: tagids, platform: "{dataset}"}}) '
            f'CREATE (t)-[:TAGGED_WITH]->(tag) '
        )

    def tx_create_topic_index(tx):
        tx.run(
            f'CREATE INDEX topic IF NOT EXISTS '
            f'FOR (t:topic) '
            f'ON (t.discourse_id, t.platform) '
        )

    with driver.session() as session:
        session.write_transaction(tx_create_topic_index)
        print('Created topic index')

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'topics'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_topics, chunk, platform_name)
                    print(f'Loaded topic data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    print(f'Import failed for topic on {platform_name}, chunk #{chunk}')
                    print(e)

    print('Added all topics')

def graph_create_posts(data):
    # Add posts

    def tx_create_posts(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_posts_{chunk}.json") '
            f'YIELD value '
            f'CREATE (p:post {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET p.user_id = value.user_id '
            f'SET p.topic_id = value.topic_id '
            f'SET p.post_number = value.post_number '
            f'SET p.raw = value.raw '
            f'SET p.created_at = value.created_at '
            f'SET p.updated_at = value.updated_at '
            f'SET p.deleted_at = value.deleted_at '
            f'SET p.hidden = value.hidden '
            f'SET p.word_count = value.word_count '
            f'SET p.wiki = value.wiki '
            f'SET p.reads = value.reads '
            f'SET p.score = value.score '
            f'SET p.like_count = value.like_count '
            f'SET p.reply_count = value.reply_count '
            f'SET p.quote_count = value.quote_count '
            f'WITH p, value '
            f'MATCH (platform:platform {{name: "{dataset}"}}) '
            f'MERGE (platform)<-[:ON_PLATFORM]-(p) '
            f'WITH p, value '
            f'MATCH (u:user {{discourse_id: value.user_id, platform: "{dataset}"}}) '
            f'MERGE (p)<-[:CREATED]-(u) '
            f'WITH p, value '
            f'MATCH (t:topic {{platform: "{dataset}", discourse_id: value.topic_id}}) '
            f'WITH p, t '
            f'MERGE (t)<-[r:IN_TOPIC]-(p)'
        )

    def tx_create_post_index(tx):
        tx.run(
            f'CREATE INDEX post IF NOT EXISTS '
            f'FOR (g:post) '
            f'ON (g.discourse_id, g.platform) '
        )

    with driver.session() as session:
        try:
            session.write_transaction(tx_create_post_index)
            print('Created post index')
        except Exception as e:
            print(f'Creating post index failed on {platform_name}')
            print(e)

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'posts'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_posts, chunk, platform_name)
                    print(f'Loaded post data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    print(f'Import failed for posts on {platform_name}, chunk #{chunk}')
                    print(e)

    print('Added all posts')

def graph_create_replies(data):
    # Add replies
    
    def tx_create_replies(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_replies_{chunk}.json") '
            f'YIELD value '
            f'MATCH (p1:post {{discourse_id: value.reply_post_id, platform: "{dataset}"}}) '
            f'MATCH (p2:post {{discourse_id: value.post_id, platform: "{dataset}"}}) '
            f'CREATE (p2)<-[r:IS_REPLY_TO]-(p1) '
        )
    
    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'replies'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_replies, chunk, platform_name)
                    print(f'Loaded reply data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    print(f'Import failed for replies on {platform_name}, chunk #{chunk}')
                    print(e)

    print('Added all reply links')

def graph_create_quotes(data):
    # Add quotes
    
    def tx_create_quotes(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_quotes_{chunk}.json") '
            f'YIELD value '
            f'MATCH (p1:post {{discourse_id: value.quoted_post_id, platform: "{dataset}"}}) '
            f'MATCH (p2:post {{discourse_id: value.post_id, platform: "{dataset}"}}) '
            f'CREATE (p1)<-[r:CONTAINS_QUOTE_FROM]-(p2) '
        )
    
    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'quotes'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_quotes, chunk, platform_name)
                    print(f'Loaded quote data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    print(f'Import quote for reply on {platform_name}, chunk #{chunk}')
                    print(e)

    print('Added all quote links')

def graph_create_interactions():
    # Add interactions

    def tx_create_user_talks(tx):
        tx.run(
            f'MATCH (g1:globaluser)<-[:IS_GLOBAL_USER]-()-[:CREATED]->()-[r:IS_REPLY_TO]-()<-[:CREATED]-()-[:IS_GLOBAL_USER]->(g2:globaluser) '
            f'WITH g1, g2, count(r) AS c '
            f'MERGE (g1)-[gr:TALKED_TO]-(g2) '
            f'SET gr.count = c '
        )

    def tx_create_global_user_talks(tx):
        tx.run(
            f'MATCH (u1:user)-[:CREATED]->()-[r:IS_REPLY_TO]-()<-[:CREATED]-(u2:user) '
            f'WITH u1, u2, count(r) AS c '
            f'MERGE (u1)-[ur:TALKED_TO]-(u2) '
            f'SET ur.count = c '
        )

    def tx_create_user_quotes(tx):
        tx.run(
            f'MATCH (g1:globaluser)<-[:IS_GLOBAL_USER]-()-[:CREATED]->()-[r:CONTAINS_QUOTE_FROM]->()<-[:CREATED]-()-[:IS_GLOBAL_USER]->(g2:globaluser) '
            f'WITH g1, g2, count(r) AS c '
            f'MERGE (g1)-[gr:QUOTED]->(g2) '
            f'SET gr.count = c '
        )

    def tx_create_global_user_quotes(tx):
        tx.run(
            f'MATCH (u1:user)-[:CREATED]->()-[r:CONTAINS_QUOTE_FROM]->()<-[:CREATED]-(u2:user) '
            f'WITH u1, u2, count(r) AS c '
            f'MERGE (u1)-[ur:QUOTED]->(u2) '
            f'SET ur.count = c '
        )

    def tx_create_user_talks_and_quotes(tx):
        tx.run(
            f'MATCH (g1:globaluser)<-[:IS_GLOBAL_USER]-()-[:CREATED]->()-[r:IS_REPLY_TO|CONTAINS_QUOTE_FROM]-()<-[:CREATED]-()-[:IS_GLOBAL_USER]->(g2:globaluser) '
            f'WITH g1, g2, count(r) AS c '
            f'MERGE (g1)-[gr:TALKED_OR_QUOTED]-(g2) '
            f'SET gr.count = c '
        )

    def tx_create_global_user_talks_and_quotes(tx):
        tx.run(
            f'MATCH (u1:user)-[:CREATED]->()-[r:IS_REPLY_TO|CONTAINS_QUOTE_FROM]-()<-[:CREATED]-(u2:user) '
            f'WITH u1, u2, count(r) AS c '
            f'MERGE (u1)-[ur:TALKED_OR_QUOTED]-(u2) '
            f'SET ur.count = c '
        )

    with driver.session() as session:
        try:
            session.write_transaction(tx_create_user_talks)
            print('Created user talk graph')
        except Exception as e:
            print('Creating user talk graph failed.')
            print(e)
        try:
            session.write_transaction(tx_create_global_user_talks)
            print('Created global user talk graph')
        except Exception as e:
            print('Creating global user talk graph failed.')
            print(e)
        try:
            session.write_transaction(tx_create_user_quotes)
            print('Created user quote graph')
        except Exception as e:
            print('Creating user quote graph failed.')
            print(e)
        try:
            session.write_transaction(tx_create_global_user_quotes)
            print('Created global user quote graph')
        except Exception as e:
            print('Creating global user quote graph failed.')
            print(e)
        try:
            session.write_transaction(tx_create_user_talks_and_quotes)
            print('Created user talk and quote graph')
        except Exception as e:
            print('Creating user talk and quote graph failed.')
            print(e)
        try:
            session.write_transaction(tx_create_global_user_talks_and_quotes)
            print('Created global user talk and quote graph')
        except Exception as e:
            print('Creating global user talk and quote graph failed.')
            print(e)

    print('Added all user to user interaction links')

def graph_create_likes(data):
    # Add likes

    def tx_create_likes(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_likes_{chunk}.json") '
            f'YIELD value '
            f'MATCH (p:post {{discourse_id: value.post_id, platform: "{dataset}"}}) '
            f'MATCH (u:user {{discourse_id: value.user_id, platform: "{dataset}"}}) '
            f'CREATE (p)<-[r:LIKES]-(u) '
        )

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'likes'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_likes, chunk, platform_name)
                    print(f'Loaded likes data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    print(f'Import likes for reply on {platform_name}, chunk #{chunk}')
                    print(e)

    print('Added all like links')

def graph_create_languages(data):
    # Add annotation languages

    def tx_create_create_languages(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_languages_{chunk}.json") '
            f'YIELD value '
            f'CREATE (lang:language {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET lang.name = value.name '
            f'SET lang.locale = value.locale '
            f'WITH lang, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'WITH lang, p, value '
            f'MERGE (p)<-[:ON_PLATFORM]-(lang) '
        )

    def tx_create_language_index(tx):
        tx.run(
            f'CREATE INDEX languages IF NOT EXISTS '
            f'FOR (lang:language) '
            f'ON (lang.discourse_id, lang.platform) '
        )

    with driver.session() as session:
        session.write_transaction(tx_create_language_index)
        print('Created language index')

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'languages'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_create_languages, chunk, platform_name)
                    print(f'Loaded language data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    print(f'Import for language on {platform_name}, chunk #{chunk}')
                    print(e)

def graph_create_codes(data):
    # Add annotation codes

    def tx_create_codes(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_codes_{chunk}.json") '
            f'YIELD value '
            f'CREATE (code:code {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET code.name = value.name '
            f'SET code.description = value.description '
            f'SET code.creator_id = value.creator_id '
            f'SET code.created_at = value.created_at '
            f'SET code.updated_at = value.updated_at '
            f'SET code.ancestry = value.ancestry '
            f'SET code.annotations_count = value.annotations_count '
            f'WITH code, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'WITH code, p, value '
            f'CREATE (p)<-[:ON_PLATFORM]-(code) '
            f'WITH code, value '
            f'MATCH (u:user {{discourse_id: value.creator_id, platform: "{dataset}"}}) '
            f'CREATE (u)-[:CREATED]->(code)'
        )

    def tx_create_code_index(tx):
        tx.run(
            f'CREATE INDEX codes IF NOT EXISTS '
            f'FOR (code:code) '
            f'ON (code.discourse_id, code.platform) '
        )

    with driver.session() as session:
        session.write_transaction(tx_create_code_index)
        print('Created code index')

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'codes'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_codes, chunk, platform_name)
                    print(f'Loaded code data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    print(f'Import for codes on {platform_name}, chunk #{chunk}')
                    print(e)

def graph_create_code_names(data):
    # Add annotation code names

    def tx_create_code_names(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_code_names_{chunk}.json") '
            f'YIELD value '
            f'CREATE (codename:codename {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET codename.name = value.name '
            f'SET codename.code_id = value.tag_id '
            f'SET codename.language_id = value.language_id '
            f'SET codename.created_at = value.created_at '
            f'WITH codename, value '
            f'MATCH (language:language {{discourse_id: value.language_id, platform: "{dataset}"}}) '
            f'MATCH (code:code {{discourse_id: value.tag_id, platform: "{dataset}"}}) '
            f'WITH codename, language, code '
            f'CREATE (codename)<-[:HAS_CODENAME]-(code) '
            f'CREATE (codename)-[:IN_LANGUAGE]->(language) '
        )

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'code_names'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_code_names, chunk, platform_name)
                    print(f'Loaded code name data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    print(f'Import for code name on {platform_name}, chunk #{chunk}')
                    print(e)

def graph_create_annotations(data):
    # Add annotations

    def tx_create_annotations(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_annotations_{chunk}.json") '
            f'YIELD value '
            f'CREATE (annotation:annotation {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET annotation.text = value.text '
            f'SET annotation.quote = value.quote '
            f'SET annotation.created_at = value.created_at '
            f'SET annotation.updated_at = value.updated_at '
            f'SET annotation.code_id = value.tag_id '
            f'SET annotation.post_id = value.post_id '
            f'SET annotation.creator_id = value.creator_id '
            f'SET annotation.type = value.type '
            f'SET annotation.topic_id = value.topic_id '
            f'WITH annotation, value '
            f'MATCH (code:code {{discourse_id: value.tag_id, platform: "{dataset}"}}) '
            f'MATCH (post:post {{discourse_id: value.post_id, platform: "{dataset}"}}) '
            f'MATCH (user:user {{discourse_id: value.creator_id, platform: "{dataset}"}}) '
            f'WITH code, post, user, annotation '
            f'CREATE (code)<-[:REFERS_TO]-(annotation) '
            f'CREATE (post)<-[:ANNOTATES]-(annotation) '
            f'CREATE (user)-[:CREATED]->(annotation) '
        )

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'annotations'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_annotations, chunk, platform_name)
                    print(f'Loaded annotations data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    print(f'Import failed for annotations on {platform_name}, chunk #{chunk}')
                    print(e)

def graph_create_corpus():
    # Define ethno-tags as corpus identifiers

    def tx_create_corpus(tx):
        tx.run(
            f'MATCH (t:tag) WHERE t.name STARTS WITH "ethno-" '
            f'SET t:corpus '
            f'WITH t '
            f'MATCH (t)<-[:TAGGED_WITH]-()<-[:IN_TOPIC]-(p:post)<-[:ANNOTATES]-()-[:REFERS_TO]->(code:code) '
            f'WITH code, t '
            f'MERGE (code)-[:IN_CORPUS]->(t)'
        )

    with driver.session() as session:
        try:
            session.write_transaction(tx_create_corpus)
            print('Added corpus labels to graph')
        except Exception as e:
            print('Adding corpus labels to graph failed.')
            print(e)

def graph_create_code_cooccurrences():
    # Create code cooccurance network between codes

    def tx_create_code_cooccurrences(tx):
        tx.run(
            f'MATCH (corpus:corpus)<-[:TAGGED_WITH]-()<-[:IN_TOPIC]-(p:post)<-[:ANNOTATES]-()-[:REFERS_TO]->(code1:code)-[:HAS_CODENAME]->(cn1:codename)-[:IN_LANGUAGE]->(l:language {{locale: "en"}}) '
            f'MATCH (corpus:corpus)<-[:TAGGED_WITH]-()<-[:IN_TOPIC]-(p:post)<-[:ANNOTATES]-()-[:REFERS_TO]->(code2:code)-[:HAS_CODENAME]->(cn2:codename)-[:IN_LANGUAGE]->(l:language {{locale: "en"}}) WHERE NOT ID(code1) = ID(code2) '
            f'WITH code1, code2, cn1, cn2, corpus, count(DISTINCT p) AS cooccurs '
            f'MERGE (code1)-[r:COOCCURS {{count: cooccurs, corpus: corpus.name}}]-(code2) '
            f'RETURN corpus.name, cn1.name, cn2.name, r.count ORDER BY r.count DESCENDING '
        )

    with driver.session() as session:
        try:
            session.write_transaction(tx_create_code_cooccurrences)
            print('Created cooccurance graph')
        except Exception as e:
            print('Creating cooccurance graph failed.')
            print(e)


def graph_create_creator_code_cooccurrences():
    pass

# Calls to update graph 
graph_clear()
print("--- %s seconds ---" % (round(time.time() - start_time,2)))
graph_create_platform(data)
print("--- %s seconds ---" % (round(time.time() - start_time,2)))
graph_create_groups(data)
print("--- %s seconds ---" % (round(time.time() - start_time,2)))
graph_create_users(data)
print("--- %s seconds ---" % (round(time.time() - start_time,2)))
graph_create_tags(data)
print("--- %s seconds ---" % (round(time.time() - start_time,2)))
graph_create_categories(data)
print("--- %s seconds ---" % (round(time.time() - start_time,2)))
graph_create_topics(data)
print("--- %s seconds ---" % (round(time.time() - start_time,2)))
graph_create_posts(data)
print("--- %s seconds ---" % (round(time.time() - start_time,2)))
graph_create_replies(data)
print("--- %s seconds ---" % (round(time.time() - start_time,2)))
graph_create_quotes(data)
print("--- %s seconds ---" % (round(time.time() - start_time,2)))
graph_create_interactions()
print("--- %s seconds ---" % (round(time.time() - start_time,2)))
graph_create_likes(data)
print("--- %s seconds ---" % (round(time.time() - start_time,2)))
graph_create_languages(data)
print("--- %s seconds ---" % (round(time.time() - start_time,2)))
graph_create_codes(data)
print("--- %s seconds ---" % (round(time.time() - start_time,2)))
graph_create_code_names(data)
print("--- %s seconds ---" % (round(time.time() - start_time,2)))
graph_create_annotations(data)
print("--- %s seconds ---" % (round(time.time() - start_time,2)))
graph_create_corpus()
print("--- %s seconds ---" % (round(time.time() - start_time,2)))
graph_create_code_cooccurrences()
print("--- %s seconds ---" % (round(time.time() - start_time,2)))

# TODO
# Add english code name to code node
# Decide how to define corpus (maybe add property for each corpus and then count?)
# Add code ancestry
# Add annotations, code relation (to local name id), annotator id
# Add code-co-occurance per ethno corpus 

# TODO FUTURE
# Add post permissions with HAS_ACCESS to groups to enable granular graph access