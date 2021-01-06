import {
  findNodesByLabelAnyOrg,
  findNodesByLabelAndProperty,
  findCooccurringCodesForCorpus,
  findUserInteractionGraphForCorpus,
  findNodesByRelationshipAndLabel,
} from '../connectors';


const resolvers = {
  // root entry point to GraphQL service
  Query: {
    platforms(obj, __, { driver }) {
      return findNodesByLabelAnyOrg(driver, 'platform');
    },
    tagsByPlatform(obj, { platform }, { driver }) {
      return findNodesByLabelAndProperty(driver, 'tag', 'platform', platform);
    },
    usersByPlatform(obj, { platform }, { driver }) {
      return findNodesByLabelAndProperty(driver, 'user', 'platform', platform);
    },
    corpusByPlatform(obj, { platform }, { driver }) {
      return findNodesByLabelAndProperty(driver, 'user', 'tag', platform);
    },
    cooccurrenceByCorpus(obj, { tagName, platform }, { driver }) {
      return findCooccurringCodesForCorpus(driver, tagName, platform);
    },
    userInteractionGraphByCorpus(obj, { tagName, platform }, { driver }) {
      return findUserInteractionGraphForCorpus(driver, tagName, platform);
    },
  },
  user: {
    created({ nodeId }, args, { driver, viewedOrg: { platform } }) {
      return findNodesByRelationshipAndLabel({ driver, platform }, nodeId, 'CREATED', 'post');
    },
  },
};

export default resolvers;
