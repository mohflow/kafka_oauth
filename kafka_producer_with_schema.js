const { Kafka }               = require('@confluentinc/kafka-javascript').KafkaJS;
const { SchemaRegistryClient, JsonSerializer, SerdeType } = require('@confluentinc/schemaregistry');
const { getAccessToken }      = require('./token_generator');
const {
  BOOTSTRAP_SERVERS,
  KAFKA_LOGICAL_CLUSTER_ID,
  IDENTITY_POOL_ID,
  SCHEMA_REGISTRY_URL,
  SR_LOGICAL_CLUSTER_ID,
  CLIENT_ID,
  TOPIC_NAME,
} = require('./config');

// ── TopicNameStrategy subject ─────────────────────────────
const SUBJECT = `${TOPIC_NAME}-value`;

// ── Shared token cache ────────────────────────────────────
let cachedToken    = null;
let tokenExpiresAt = 0;

async function getCachedToken() {
  const now = Date.now();
  if (!cachedToken || now >= tokenExpiresAt - 60000) {
    console.log('Refreshing OAuth token from Entra ID...');
    const response = await getAccessToken();
    cachedToken    = response.access_token;
    tokenExpiresAt = now + response.expires_in * 1000;
    console.log('Token refreshed successfully');
  }
  return cachedToken;
}

// ── Kafka OAuthBearer Callback ────────────────────────────
async function oauthTokenRefresh() {
  const token = await getCachedToken();
  return {
    tokenValue: token,
    lifetime:   tokenExpiresAt,
    principal:  CLIENT_ID,
    extensions: {
      logicalCluster: KAFKA_LOGICAL_CLUSTER_ID,  // ← fixed
      identityPoolId: IDENTITY_POOL_ID,
    },
  };
}

// ── Schema Registry Client ────────────────────────────────
async function createSchemaRegistryClient() {
  const token = await getCachedToken();
  return new SchemaRegistryClient({
    baseURLs: [SCHEMA_REGISTRY_URL],
    bearerAuthCredentials: {
      credentialsSource: 'STATIC_TOKEN',
      token:             token,
      identityPoolId:    IDENTITY_POOL_ID,
      logicalCluster:    SR_LOGICAL_CLUSTER_ID,  // ← fixed
    },
  });
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

// ── Kafka Producer ────────────────────────────────────────
const kafka = new Kafka({
  'bootstrap.servers':            BOOTSTRAP_SERVERS,
  'security.protocol':            'sasl_ssl',
  'sasl.mechanisms':              'OAUTHBEARER',
  'sasl.oauthbearer.config':      `logicalCluster=${KAFKA_LOGICAL_CLUSTER_ID},identityPoolId=${IDENTITY_POOL_ID}`,  // ← fixed
  'oauthbearer_token_refresh_cb': oauthTokenRefresh,
});

async function startProducer() {
  const schemaRegistry = await createSchemaRegistryClient();
  const producer       = kafka.producer();

  // Register schema using TopicNameStrategy subject → "{topic}-value"
  const schemaId = await schemaRegistry.register(
    SUBJECT,
    { schemaType: 'JSON', schema: jsonSchema }
  );
  console.log(`JSON Schema registered | subject: "${SUBJECT}" | ID: ${schemaId}`);

  // Create JSON serializer
  const serializer = new JsonSerializer(schemaRegistry, SerdeType.VALUE, { useLatestVersion: true });

  await producer.connect();
  console.log('Producer connected, starting continuous send...\n');

  let messageCount = 0;

  const interval = setInterval(async () => {
    try {
      messageCount++;
      const payload = {
        id:        messageCount,
        timestamp: new Date().toISOString(),
        amount:    parseFloat((Math.random() * 1000).toFixed(2)),
      };

      const encodedValue = await serializer.serialize(TOPIC_NAME, payload);

      await producer.send({
        topic:    TOPIC_NAME,
        messages: [{ key: `key-${messageCount}`, value: encodedValue }],
      });

      console.log(`[${messageCount}] Sent:`, payload);
    } catch (err) {
      console.error('Failed to send message:', err.message);
    }
  }, 2000);

  // Graceful shutdown on Ctrl+C
  process.on('SIGINT', async () => {
    console.log('\nShutting down producer...');
    clearInterval(interval);
    await producer.disconnect();
    console.log('Producer disconnected');
    process.exit(0);
  });
}

startProducer().catch(console.error);