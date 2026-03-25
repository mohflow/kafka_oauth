const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;
const { SchemaRegistryClient, JsonDeserializer, SerdeType } = require('@confluentinc/schemaregistry');
const { getAccessToken } = require('./token_generator');
const {
  BOOTSTRAP_SERVERS,
  KAFKA_LOGICAL_CLUSTER_ID,
  IDENTITY_POOL_ID,
  SCHEMA_REGISTRY_URL,
  SR_LOGICAL_CLUSTER_ID,
  CLIENT_ID,
  TOPIC_WITH_SCHEMA,
} = require('./config');

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
    console.log('✅ Token refreshed successfully');
    console.log(`🔑 Token: ${cachedToken}\n`);
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
      logicalCluster: KAFKA_LOGICAL_CLUSTER_ID,
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
      logicalCluster:    SR_LOGICAL_CLUSTER_ID,
    },
  });
}

// ── Kafka Consumer ────────────────────────────────────────
const kafka = new Kafka({
  'bootstrap.servers':            BOOTSTRAP_SERVERS,
  'security.protocol':            'sasl_ssl',
  'sasl.mechanisms':              'OAUTHBEARER',
  'sasl.oauthbearer.config':      `logicalCluster=${KAFKA_LOGICAL_CLUSTER_ID},identityPoolId=${IDENTITY_POOL_ID}`,
  'oauthbearer_token_refresh_cb': oauthTokenRefresh,
});

async function startConsumer() {
  const schemaRegistry = await createSchemaRegistryClient();
  const deserializer   = new JsonDeserializer(schemaRegistry, SerdeType.VALUE, {});

  const consumer = kafka.consumer({
    'group.id': `${TOPIC_WITH_SCHEMA}-consumer-group`,
    kafkaJS: {
      fromBeginning: true,
    },
  });

  await consumer.connect();
  console.log('📡 Consumer connected');

  await consumer.subscribe({ topics: [TOPIC_WITH_SCHEMA] });
  console.log(`👂 Listening on topic: "${TOPIC_WITH_SCHEMA}"\n`);

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const decodedValue = await deserializer.deserialize(topic, message.value);

      console.log(`✅ Message received:`);
      console.log(`   Topic     : ${topic}`);
      console.log(`   Partition : ${partition}`);
      console.log(`   Offset    : ${message.offset}`);
      console.log(`   Key       : ${message.key?.toString()}`);
      console.log(`   Value     :`, decodedValue);
      console.log('');
    },
  });

  // Graceful shutdown on Ctrl+C
  process.on('SIGINT', async () => {
    console.log('\n🛑 Shutting down consumer...');
    await consumer.disconnect();
    console.log('👋 Consumer disconnected');
    process.exit(0);
  });
}

startConsumer().catch(console.error);