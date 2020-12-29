import psycopg2
import json
import os
from neo4j import GraphDatabase
from pprint import pprint


# For this script to work, neo4j must have APOC installed and the neo4j.conf file 
# must have the following properties set:
# apoc.import.file.enabled=true
# apoc.import.file.use_neo4j_config=false

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

reload_from_database = False

def get_data(db_cursor, db_name):

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
    users[-3] = {
        'id': -3,
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
    # Group 0 is 'everyone' and permission_type is an integer 1 = Full 2 = Create Post 3 = Read Only 

    groups_query = """
    SELECT 
    id, name
    FROM backup.groups
    """

    groups = {}
    db_cursor.execute(groups_query)
    group_data = db_cursor.fetchall()
    for group in group_data:
        gid = group[0]
        groups[gid] = {
            'id': gid,
            'name': group[1]
        }

    print(f'    Got {len(groups.keys())} groups')

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
    id, title, created_at, updated_at, user_id
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
    pm_set = set()
    db_cursor.execute(allowed_users_query)
    allowed_users_data = db_cursor.fetchall()
    for permission in allowed_users_data:
        tid = permission[0]
        pm_set.add(tid)
        pm_count += 1

    topics = {}
    db_cursor.execute(topics_query)
    topics_data = db_cursor.fetchall()
    lost_topics = set()
    for topic in topics_data:
        tid = topic[0]
        topics[tid] = {
            'id': tid,
            'title': 'Private message' if tid in pm_set else topic[1],
            'created_at': topic[2],
            'updated_at': topic[3],
            'user_id': -3 if tid in pm_set or topic[4] not in users.keys() else topic[4],
            'is_message_thread': True if tid in pm_set else False,
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
    private_count = 0
    db_cursor.execute(posts_query)
    posts_data = db_cursor.fetchall()
    for post in posts_data:
        pid = post[0]
        private = True if post[2] in pm_set else False
        if private:
            private_count += 1
        deleted = post[7]
        posts[pid] = {
            'id': pid,
            'user_id': -3 if private or deleted or post[1] not in users.keys() else post[1],
            'topic_id': post[2],
            'post_number': post[3],
            'raw': 'Removed content' if private or deleted else post[4],
            'created_at': post[5],
            'updated_at': post[6],
            'deleted_at': post[7],
            'hidden': post[8],
            'word_count': 0 if private or deleted else post[9],
            'wiki': post[10],
            'reads': 0 if private or deleted else post[11],
            'score': 0 if private or deleted else post[12],
            'like_count': 0 if private or deleted else post[13],
            'reply_count': post[14],
            'quote_count': post[15],
            'quotes_posts': [],
            'is_reply_to': None,
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
        posts[reply[1]]['is_reply_to'] = reply[0]
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
            'decription': code[1],
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

    stats = {
        'users': len(users.keys()),
        'groups': len(groups.keys()),
        'tags': len(tags.keys()),
        'topics': len(topics.keys()),
        'pm_threads': pm_count,
        'topics_by_deleted_users': len(lost_topics),
        'tags_applied': len(topic_tags_data),
        'posts': len(posts.keys()),
        'messages': private_count,
        'annotator_languages': language_list[:-1],
        'ethno-codes': len(list(annotator_codes.keys())),
        'ethno-code-names': len(list(annotator_code_names.keys())),
        'ethno-annotations': len(list(annotations.keys()))
    }

    return {
        'stats': stats,
        'site': site,
        'users': users,
        'groups': groups,
        'tags': tags,
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
    print('Loading new data from databases...')
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
        print(f'{len(d["topics"])} topics, of which {d["stats"]["pm_threads"]} are PM threads, and {d["stats"]["topics_by_deleted_users"]} were by deleted users.')
        print(f'{d["stats"]["tags_applied"]} tag applications to topics.')
        print(f'{len(d["posts"])} posts, of which {d["stats"]["messages"]} are private messages.')
        print(f'{len(d["replies"])} posts are replies to other posts.')
        print(f'{len(d["quotes"])} quotes.')
        print(f'{len(d["likes"])} likes.')
        print('Private message content and user identification has been omitted from the dataset.')
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

# Build Neo4j database

print(' ')
print('Building Neo4j database...')
print(' ')

uri = 'bolt://localhost:7687'
driver = GraphDatabase.driver(uri, auth=('neo4j', 'shen4yaya'))
data_path = os.path.abspath('./db/')

# Clear database function
def graph_clear():

    def tx_clear_neo4j(tx):
        tx.run('MATCH (a) DETACH DELETE a')

    with driver.session() as session:
        try:
            session.write_transaction(tx_clear_neo4j)
            print('Cleared database')
        except Exception as e:
            print(e)

# Add platforms function
def graph_create_platform(data):
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

    for platform in data.values():
        with driver.session() as session:
            try:
                session.write_transaction(tx_create_platform, platform['site']['name'])
                print(f'Loaded platform data from {platform["site"]["name"]}')
            except Exception as e:
                print(f'Import failed for platform data on {platform["site"]["name"]}')
                print(e)

    with driver.session() as session:
        session.write_transaction(tx_create_platform_index)

    print('Loaded all platforms.')

# Add user groups function
def graph_create_groups(data):

    def tx_create_groups(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_groups_{chunk}.json") '
            f'YIELD value '
            f'CREATE (g:group {{discourse_id: value.id, platform: "{dataset}"}}) '
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

    with driver.session() as session:
        session.write_transaction(tx_create_group_index)
        print('Created group index')

    print('Added all groups')

# Add users function
def graph_create_users(data):

    def tx_create_users(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_users_{chunk}.json") '
            f'YIELD value '
            f'CREATE (u:user {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET u.username = value.username '
            f'SET u.email = value.email '
            f'SET u.consent = value.consent '
            f'SET u.consent_updated = value.consent_updated '
            f'SET u.groups = value.groups '
            f'WITH u, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'WITH u, p, value '
            f'CREATE (p)<-[:ON_PLATFORM]-(u) '
            f'WITH u, value '
            f'UNWIND value.groups AS gids '
            f'MATCH (g:group {{discourse_id: gids}}) '
            f'WITH u, g '
            f'CREATE (u)-[:IN_GROUP]->(g)'
        )

    def tx_create_user_index(tx):
        tx.run(
            f'CREATE INDEX user IF NOT EXISTS '
            f'FOR (g:user) '
            f'ON (g.discourse_id, g.platform) '
        )

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

    with driver.session() as session:
        session.write_transaction(tx_create_user_index)
        print('Created user index')

    print('Added all users')

# Add tags function
def graph_create_tags(data):

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
            f'FOR (g:tag) '
            f'ON (g.discourse_id, g.platform) '
        )

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

    with driver.session() as session:
        session.write_transaction(tx_create_tag_index)
        print('Created tag index')

    print('Added all tags')

# Add topics
def graph_create_topics(data):

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
            f'WITH t, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'CREATE (p)<-[:ON_PLATFORM]-(t) '
            f'WITH t, value '
            f'MATCH (u:user {{discourse_id: value.user_id, platform: "{dataset}"}}) '
            f'CREATE (t)<-[:CREATED]-(u) '
            f'WITH t, value '
            f'UNWIND value.tags AS tagids '
            f'MATCH (tag:tag {{discourse_id: tagids}}) '
            f'CREATE (t)<-[:TAGGED_WITH]-(tag) '
        )

    def tx_create_topic_index(tx):
        tx.run(
            f'CREATE INDEX topic IF NOT EXISTS '
            f'FOR (g:topic) '
            f'ON (g.discourse_id, g.platform) '
        )

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

    with driver.session() as session:
        session.write_transaction(tx_create_topic_index)
        print('Created topic index')

    print('Added all topics')

# Add posts
def graph_create_posts(data):

    def tx_create_posts(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_posts_{chunk}.json") '
            f'YIELD value '
            f'MERGE (p:post {{discourse_id: value.id, platform: "{dataset}"}}) '
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
            f'CREATE (platform)<-[:ON_PLATFORM]-(p) '
            f'WITH p, value '
            f'MATCH (u:user {{discourse_id: value.user_id, platform: "{dataset}"}}) '
            f'CREATE (p)<-[:CREATED]-(u) '
            f'WITH p, value '
            f'MATCH (t:topic {{discourse_id: value.topic_id, platform: "{dataset}"}}) '
            f'CREATE (t)<-[:IN_TOPIC]-(p) '
            f'WITH p, value '
            f'MATCH (p2:post {{discourse_id: value.is_reply_to, platform: "{dataset}"}})'
            f'CALL apoc.do.when(value.is_reply_to IS NOT NULL, '
            f'"MERGE (p)-[r:IS_REPLY_TO]->(p2) RETURN p2",'
            f'"",'
            f'{{p:p, p2:p2}})'
            f'YIELD value AS value2 '
            f'RETURN value2 '
        )

    def tx_create_post_index(tx):
        tx.run(
            f'CREATE INDEX post IF NOT EXISTS '
            f'FOR (g:post) '
            f'ON (g.discourse_id, g.platform) '
        )

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

    with driver.session() as session:
        session.write_transaction(tx_create_post_index)
        print('Created post index')

    print('Added all posts')

# Add quotes
def graph_create_quotes(data):
    
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

# Add likes
def graph_create_likes(data):

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

# Update graph
# graph_clear()
# graph_create_platform(data)
# graph_create_groups(data)
# graph_create_users(data)
# graph_create_tags(data)
# graph_create_topics(data)
# graph_create_posts(data)
# graph_create_quotes(data)
graph_create_likes(data)

# TODO
# Add post permissions with HAS_ACCESS to groups
# Potentially scrub content and titles from non-public topics and posts
# Add annotation languages for platform
# Add codes and local code names linked to languages
# Add code ancestry
# Add code creator relation
# Add annotations, code relation (to local name id), annotator id
# Add cross-platform identity node for users
