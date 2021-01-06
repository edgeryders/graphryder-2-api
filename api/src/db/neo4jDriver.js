import neo4j from 'neo4j-driver';
import dotenv from 'dotenv';

dotenv.config({ silent: true });

async function createDriver() {
  const driver = neo4j.driver(
    process.env.DB_URL,
    neo4j.auth.basic(
      process.env.DB_USERNAME,
      process.env.DB_PASSWORD,
    ),
  );

  return driver;
}

// TODO: Run driver.close() when node app exits.

export default createDriver;
