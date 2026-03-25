const { Kafka }                = require('@confluentinc/kafka-javascript').KafkaJS;
const { SchemaRegistryClient } = require('@confluentinc/schemaregistry');
const { getAccessToken }       = require('./token_generator');
const {
  BOOTSTRAP_SERVERS,
  KAFKA_LOGICAL_CLUSTER_ID,
  IDENTITY_POOL_ID,
  SCHEMA_REGISTRY_URL,
  SR_LOGICAL_CLUSTER_ID,
  TOPIC_WITH_SCHEMA,
} = require('./config');

const SUBJECT = `${TOPIC_WITH_SCHEMA}-value`;

// ── Shared token cache ────────────────────────────────────
let cachedToken    = null;
let tokenExpiresAt = 0;

async function getCachedToken() {
  const now = Date.now();
  if (!cachedToken || now >= tokenExpiresAt - 60000) {
    console.log('🔄 Refreshing OAuth token from Entra ID...');
    const response = await getAccessToken();
    cachedToken    = response.access_token;
    tokenExpiresAt = now + response.expires_in * 1000;
    console.log('✅ Token refreshed\n');
  }
  return cachedToken;
}

// ── JSON Schema definition ────────────────────────────────
const jsonSchema = JSON.stringify({
  $schema:    'http://json-schema.org/draft-07/schema#',
  title:      'Transaction',
  type:       'object',
  properties: {
    id:        { type: 'integer' },
    timestamp: { type: 'string' },
    amount:    { type: 'number' },
  },
  required: ['id', 'timestamp', 'amount'],
});

// ── Step 1: Register Schema ───────────────────────────────
async function registerSchema() {
  const token = await getCachedToken();

  const schemaRegistry = new SchemaRegistryClient({
    baseURLs: [SCHEMA_REGISTRY_URL],
    bearerAuthCredentials: {
      credentialsSource: 'STATIC_TOKEN',
      token:             token,
      identityPoolId:    IDENTITY_POOL_ID,
      logicalCluster:    SR_LOGICAL_CLUSTER_ID,
    },
  });

  const schemaId = await schemaRegistry.register(
    SUBJECT,
    { schemaType: 'JSON', schema: jsonSchema }
  );

  console.log(`✅ Schema registered`);
  console.log(`   Subject : ${SUBJECT}`);
  console.log(`   ID      : ${schemaId}`);
  console.log(`   Type    : JSON\n`);
}

// ── Step 2: Create Topic ──────────────────────────────────
async function createTopic() {
  const kafka = new Kafka({
    'bootstrap.servers':            BOOTSTRAP_SERVERS,
    'security.protocol':            'sasl_ssl',
    'sasl.mechanisms':              'OAUTHBEARER',
    'sasl.oauthbearer.config':      `logicalCluster=${KAFKA_LOGICAL_CLUSTER_ID},identityPoolId=${IDENTITY_POOL_ID}`,
    'oauthbearer_token_refresh_cb': async () => {
      const token = await getCachedToken();
      return {
        tokenValue: token,
        lifetime:   tokenExpiresAt,
        principal:  'admin',
        extensions: {
          logicalCluster: KAFKA_LOGICAL_CLUSTER_ID,
          identityPoolId: IDENTITY_POOL_ID,
        },
      };
    },
  });

  const admin = kafka.admin();
  await admin.connect();
  console.log('📡 Admin connected');

  // Check if topic already exists
  const existingTopics = await admin.listTopics();
  if (existingTopics.includes(TOPIC_WITH_SCHEMA)) {
    console.log(`⚠️  Topic "${TOPIC_WITH_SCHEMA}" already exists — skipping creation`);
  } else {
    await admin.createTopics({
      topics: [{
        topic:             TOPIC_WITH_SCHEMA,
        numPartitions:     3,
        replicationFactor: 3,
      }],
    });
    console.log(`✅ Topic "${TOPIC_WITH_SCHEMA}" created | partitions: 3 | replication: 3`);
  }

  await admin.disconnect();
  console.log('👋 Admin disconnected\n');
}

// ── Run Setup ─────────────────────────────────────────────
async function setup() {
  console.log('🚀 Starting setup...\n');
  try {
    await registerSchema();  // ← schema first
    await createTopic();     // ← topic second
    console.log('🎉 Setup complete! You can now run: node kafka_producer_with_schema.js');
  } catch (err) {
    console.error('❌ Setup failed:', err.message);
    process.exit(1);
  }
}

setup();