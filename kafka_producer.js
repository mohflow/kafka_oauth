const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;
const { getAccessToken } = require('./token_generator.js');
const { BOOTSTRAP_SERVERS, IDENTITY_POOL_ID, KAFKA_CLUSTER_ID,SCHEMA_REGISTRY_CLUSTER_ID} = require("./config.js");
// ── OAuthBearer Callback ──────────────────────────────────
async function oauthTokenRefresh() {
  console.log('Refreshing OAuth token from Entra ID...');

  const response = await getAccessToken();

  console.log('Token refreshed successfully');

  return {
    tokenValue: response.access_token,
    lifetime:   Date.now() + response.expires_in * 1000,
    principal:  response.client_id,
    extensions: {
      logicalCluster: KAFKA_CLUSTER_ID,
      identityPoolId: IDENTITY_POOL_ID
    }
  };
}

// ── Kafka Producer ────────────────────────────────────────
const kafka = new Kafka({
  'bootstrap.servers':            BOOTSTRAP_SERVERS,
  'security.protocol':            'sasl_ssl',
  'sasl.mechanisms':              'OAUTHBEARER',
  'sasl.oauthbearer.config':      `logicalCluster=${KAFKA_CLUSTER_ID},identityPoolId=${IDENTITY_POOL_ID}`,
  'oauthbearer_token_refresh_cb': oauthTokenRefresh,
});

// ── Continuously Running Producer ────────────────────────
async function startProducer() {
  const producer = kafka.producer();

  await producer.connect();
  console.log('Producer connected, starting continuous send...\n');

  let messageCount = 0;

  // Send a message every 2 seconds
  const interval = setInterval(async () => {
    try {
      messageCount++;
      const message = {
        key:   `key-${messageCount}`,
        value: JSON.stringify({ id: messageCount, timestamp: new Date().toISOString() }),
      };

      await producer.send({
        topic:    'test-topic',
        messages: [message],
      });

      console.log(`[${messageCount}] Sent: ${message.value}`);
    } catch (err) {
      console.error('Failed to send message:', err.message);
    }
  }, 2000);

  // Graceful shutdown on Ctrl+C
  process.on('SIGINT', async () => {
    console.log('\n Shutting down producer...');
    clearInterval(interval);
    await producer.disconnect();
    console.log('👋 Producer disconnected');
    process.exit(0);
  });
}

startProducer().catch(console.error);