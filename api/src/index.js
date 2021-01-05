import express from 'express';
import cors from 'cors';
import { createServer } from 'http';
import { ApolloServer } from 'apollo-server-express';
import createDriver from './db/neo4jDriver';
import schema from './graphql/schema';
import startSchedulers from './services/scheduler';

// Max listeners for a pub/sub
require('events').EventEmitter.defaultMaxListeners = 15;

const { NODE_ENV, PORT } = process.env;
const API_PORT = NODE_ENV && NODE_ENV.includes('prod') ? PORT || 3000 : 3100;
const app = express();

if (!NODE_ENV || NODE_ENV.includes('dev')) {
  app.use(cors());
}

app.use('/graphql', keycloak.middleware());

createDriver().then((neo4jDriver) => {
  const server = new ApolloServer({
    schema
  });

  server.applyMiddleware({ app, path: '/graphql' });

  const httpServer = createServer(app);
  server.installSubscriptionHandlers(httpServer);

  httpServer.listen(API_PORT, () => {
    console.log(`GraphQL Server is now running on http://localhost:${API_PORT}/graphql`);
    console.log(`View GraphQL Playground at http://localhost:${API_PORT}/graphql`);
  });

  // Start the schedulers that download data from various APIs.
  startSchedulers();
});