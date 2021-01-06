import {
  runQueryAndGetRecords,
  runQueryAndGetRecord,
} from '../../db/cypherUtils';

export function findNodesByLabelAnyOrg(driver, label) {
  const query = `
    MATCH (n:${label})
    RETURN n
  `;
  return runQueryAndGetRecords(driver.session(), query);
}

export function findNodeByLabelAndId(driver, label, nodeId) {
  const query = `
    MATCH (n:${label} {nodeId: $nodeId})
    RETURN n
  `;
  return runQueryAndGetRecord(driver.session(), query, { nodeId });
}

export function findNodeByLabelAndProperty(driver, label, propertyKey, propertyValue) {
  const query = `
    MATCH (n:${label} {${propertyKey}: $value})
    RETURN n
  `;
  return runQueryAndGetRecord(driver.session(), query, { value: propertyValue });
}


export function findNodesByLabelAndProperty(driver, label, propertyKey, propertyValue) {
  const query = `
    MATCH (n:${label} {${propertyKey}: $value})
    RETURN n
  `;
  return runQueryAndGetRecords(driver.session(), query, { value: propertyValue });
}

export function findCooccurringCodesForCorpus(driver, tagName, platform) {
  const query = `
    MATCH (tag:tag {name: $t, platform: $p})<-[:TAGGED_WITH]-()<-[:IN_TOPIC]-(p:post)
    MATCH (p)<-[:ANNOTATES]-()-[:REFERS_TO]->(code1:code)
    MATCH (p)<-[:ANNOTATES]-()-[:REFERS_TO]->(code2:code)
    WHERE NOT ID(code1) = ID(code2)
    RETURN DISTINCT code1, code2, COLLECT(DISTINCT p.discourse_id) AS posts, count(DISTINCT p) AS cooccurs ORDER BY cooccurs DESCENDING
  `;
  return runQueryAndGetRecords(driver.session(), query, { t: tagName, p: platform });
}

export function findUserInteractionGraphForCorpus(driver, tagName, platform) {
  const query = `
    MATCH (tag:tag {name: $t, platform: $p})<-[:TAGGED_WITH]-()<-[:IN_TOPIC]-(p:post)
    MATCH (user1:user)-[:CREATED]->(p)-[r:IS_REPLY_TO|CONTAINS_QUOTE_FROM]-()<-[:CREATED]-(user2:user)
    RETURN user1, user2, COLLECT(DISTINCT p.discourse_id) AS posts, count(DISTINCT r) AS interactions
  `;
  return runQueryAndGetRecords(driver.session(), query, { t: tagName, p: platform });
}

export function findTagByNameAndPlatform(driver, tagName, platform) {
  const query = `
    MATCH (t:tag {name: ${tagName}, platform: ${platform}})
  `;
  return runQueryAndGetRecord(driver.session(), query, { tagName, platform });
}
