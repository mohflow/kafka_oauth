const jwt = require("jsonwebtoken");
const { v4: uuidv4 } = require("uuid");
const fs = require("fs");
const https = require("https");
const querystring = require("querystring");
const { TENANT_ID, CLIENT_ID, THUMBPRINT, PRIVATE_KEY_PATH, SCOPE } = require("./config");

function hexToBase64Url(hex) {
  return Buffer.from(hex, "hex")
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

async function getAccessToken() {
  const privateKey = fs.readFileSync(PRIVATE_KEY_PATH, "utf8");
  const now = Math.floor(Date.now() / 1000);

  const clientAssertion = jwt.sign(
    {
      aud: `https://login.microsoftonline.com/${TENANT_ID}/oauth2/v2.0/token`,
      iss: CLIENT_ID,
      sub: CLIENT_ID,
      jti: uuidv4(),
      nbf: now,
      exp: now + 600,
    },
    privateKey,
    { algorithm: "RS256", header: { alg: "RS256", typ: "JWT", x5t: hexToBase64Url(THUMBPRINT) } }
  );

  const body = querystring.stringify({
    grant_type:            "client_credentials",
    client_id:             CLIENT_ID,
    client_assertion_type: "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
    client_assertion:      clientAssertion,
    scope:                 SCOPE,
  });

  return new Promise((resolve, reject) => {
    const req = https.request(
      {
        hostname: "login.microsoftonline.com",
        path:     `/${TENANT_ID}/oauth2/v2.0/token`,
        method:   "POST",
        headers:  { "Content-Type": "application/x-www-form-urlencoded", "Content-Length": Buffer.byteLength(body) },
      },
      (res) => {
        let data = "";
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => {
          const parsed = JSON.parse(data);
          parsed.error ? reject(new Error(parsed.error_description)) : resolve(parsed); 
        });
      }
    );
    req.on("error", reject);
    req.write(body);
    req.end();
  });
}

module.exports = { getAccessToken };