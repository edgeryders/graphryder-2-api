import express from 'express';
import cors from 'cors';
import { createServer } from 'http';
import { ApolloServer } from 'apollo-server-express';
import createDriver from './db/neo4jDriver';
import schema from './graphql/schema';

// Max listeners for a pub/sub
require('events').EventEmitter.defaultMaxListeners = 15;

const { NODE_ENV, PORT } = process.env;
const API_PORT = NODE_ENV && NODE_ENV.includes('prod') ? PORT || 3000 : 3100;
const app = express();

if (!NODE_ENV || NODE_ENV.includes('dev')) {
  app.use(cors());
}

async function createContext(neo4jDriver) {
  return {
    driver: neo4jDriver,
  };
}

createDriver().then((neo4jDriver) => {
  const server = new ApolloServer({
    schema,
    context: async ({ req, connection }) => {
      if (req) {
        return createContext(neo4jDriver);
      }
      return connection.context;
    },
    tracing: true,
  });

  server.applyMiddleware({ app, path: '/graphql' });

  const httpServer = createServer(app);
  server.installSubscriptionHandlers(httpServer);

  httpServer.listen(API_PORT, () => {
    console.log(`GraphQL Server is now running on http://localhost:${API_PORT}/graphql`);
    console.log(`View GraphQL Playground at http://localhost:${API_PORT}/graphql`);
  });
});
