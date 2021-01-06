const typeDefs = `
  schema {
    query: Query
  }

  type Query {
    platforms: [platform]
    tagsByPlatform(
      platform: String
    ): [tag]
    usersByPlatform(
      platform: String
    ): [user]
    corpusByPlatform(
      platform: String
    ): [tag]
    cooccurrenceByCorpus(
      tagName: String,
      platform: String
    ): [_cooccurrence]
    userInteractionGraphByCorpus(
      tagName: String,
      platform: String
    ): [_interaction]
  }

  type group {
    _id: Int!
    discourse_id: Int!
    name: String!
    platform: String!
    has_access: [category]
    users: [user]
  }

  type user {
    _id: Int!
    consent: String!
    consent_updated: String!
    discourse_id: Int!
    email: String!
    groups: String!
    platform: String!
    username: String!
    in_group: [group]
    is_global_user: [globaluser]
    created: [post]
    talked_to: [user]
    talked_or_quoted: [user]
    likes: [post]
    used_code: [code]
  }
  
  type globaluser {
    _id: Int!
    email: String!
    username: String!
    has_account_on: [platform]
    users: [user]
  }
  
  type tag {
    _id: Int!
    created_at: String!
    discourse_id: Int!
    name: String!
    platform: String!
    topic_count: Int!
    updated_at: String!
    topics: [topic]
    codes: [code]
  }
  
  type category {
    _id: Int!
    created_at: String
    discourse_id: Int!
    name: String
    name_lower: String
    parent_category_id: Int
    permissions: String
    platform: String!
    read_restricted: Boolean
    updated_at: String
    parent_category_of: [category]
    groups: [group]
    topics: [topic]
  }
  
  type topic {
    _id: Int!
    category_id: Int!
    created_at: String!
    discourse_id: Int!
    is_message_thread: Boolean!
    platform: String!
    tags: String!
    title: String!
    updated_at: String!
    user_id: Int!
    in_category: [category]
    tagged_with: [tag]
    users: [user]
    posts: [post]
  }
  
  type post {
    _id: Int!
    created_at: String!
    deleted_at: String
    discourse_id: Int!
    hidden: Boolean!
    like_count: Int!
    platform: String!
    post_number: Int!
    quote_count: Int!
    raw: String!
    reads: Int!
    reply_count: Int!
    score: String!
    topic_id: Int!
    updated_at: String!
    user_id: Int!
    wiki: Boolean!
    word_count: Int!
    in_topic: [topic]
    is_reply_to: [post]
    contains_quote_from: [post]
    users: [user]
    annotations: [annotation]
  }
  
  type language {
    _id: Int!
    discourse_id: Int!
    locale: String!
    name: String!
    platform: String!
    codenames: [codename]
  }
  
  type code {
    _id: Int!
    ancestry: String
    annotations_count: Int!
    created_at: String!
    creator_id: Int!
    description: String
    discourse_id: Int!
    name: String
    platform: String!
    updated_at: String!
    on_platform: [platform]
    has_parent_code: [code]
    has_codename: [codename]
    cooccurs: [code]
    annotations: [annotation]
    users: [user]
  }
  
  type corpus_tag {
    _id: Int!
    created_at: String!
    discourse_id: Int!
    name: String!
    platform: String!
    topic_count: Int!
    updated_at: String!
    codes: [code]
  }
  
  type codename {
    _id: Int!
    code_id: Int!
    created_at: String!
    discourse_id: Int!
    language_id: Int!
    name: String!
    platform: String!
    in_language: [language]
    codes: [code]
  }
  
  type annotation {
    _id: Int!
    code_id: Int
    created_at: String!
    creator_id: Int!
    discourse_id: Int!
    platform: String!
    post_id: Int!
    quote: String
    text: String
    topic_id: Int!
    type: String!
    updated_at: String!
    refers_to: [code] 
    annotates: [post]
  }
  
  type platform {
    _id: Int
    name: String
    url: String
    codes: [code] 
    globalusers: [globaluser]
  }

  type TALKED_TO {
   from: user!
   to: user!
   count: Int!
  }
  
  type TALKED_OR_QUOTED {
   from: user!
   to: user!
   count: Int!
  }
  
  type COOCCURS {
   from: code!
   to: code!
   corpus: String!
   count: Int!
  }
  
  type USED_CODE {
   from: user!
   to: code!
   count: Int!
  }

  type _cooccurrence {
    code1: code,
    code2: code,
    posts: [Int],
    cooccurs: Int
  }

  type _interaction {
    user1: user,
    user2: user,
    posts: [Int],
    interactions: Int
  }

`;

export default typeDefs;
