// Test query for talk graph
MATCH (alberto_global:globaluser {username: "alberto"})-[:TALKED_TO]-(hugi_global:globaluser {username: "hugi"})<-[:IS_GLOBAL_USER]-(hugi:user)-[:TALKED_TO]-(alberto:user {username: "alberto"}) RETURN alberto_global, hugi_global, hugi, alberto

// Create global user talk graph
MATCH (g1:globaluser)<-[:IS_GLOBAL_USER]-()-[:CREATED]->()-[r:IS_REPLY_TO]-()<-[:CREATED]-()-[:IS_GLOBAL_USER]->(g2:globaluser)
WITH g1, g2, count(r) AS c
MERGE (g1)-[gr:TALKED_TO]-(g2) 
SET gr.count = c

// Create local user talk graph
MATCH (u1:user)-[:CREATED]->()-[r:IS_REPLY_TO]-()<-[:CREATED]-(u2:user)
WITH u1, u2, count(r) AS c
MERGE (u1)-[ur:TALKED_TO]-(u2) 
SET ur.count = c

// Create global user talk and quote graph
MATCH (g1:globaluser)<-[:IS_GLOBAL_USER]-()-[:CREATED]->()-[r:IS_REPLY_TO|CONTAINS_QUOTE_FROM]-()<-[:CREATED]-()-[:IS_GLOBAL_USER]->(g2:globaluser)
WITH g1, g2, count(r) AS c
MERGE (g1)-[gr:TALKED_OR_QUOTED]-(g2) 
SET gr.count = c

// Create local user talk and quote graph
MATCH (u1:user)-[:CREATED]->()-[r:IS_REPLY_TO|CONTAINS_QUOTE_FROM]-()<-[:CREATED]-(u2:user)
WITH u1, u2, count(r) AS c
MERGE (u1)-[ur:TALKED_OR_QUOTED]-(u2) 
SET ur.count = c

// Create global user like graph
MATCH (g1:globaluser)<-[:IS_GLOBAL_USER]-()-[r:LIKES]->()<-[:CREATED]-()-[:IS_GLOBAL_USER]->(g2:globaluser)
WITH g1, g2, count(r) AS c
MERGE (g1)-[gr:LIKES]->(g2) 
SET gr.count = c

// Create local user like graph
MATCH (u1:user)-[r:LIKES]->()<-[:CREATED]-(u2:user)
WITH u1, u2, count(r) AS c
MERGE (u1)-[ur:LIKES]->(u2) 
SET ur.count = c