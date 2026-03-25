## install node libraries

npm install @confluentinc/kafka-javascript jsonwebtoken uuid 

## Command to generate private key and self-signed certificate (you will be prompted to enter metadata like Country,state etc. for certificate)
openssl req -x509 -newkey rsa:2048 \
  -keyout private_key.pem \
  -out certificate.pem \
  -days 365 \
  -nodes

## Inspect public certificate

openssl x509 -in certificate.pem -text -noout

## You can also use cerificate signed by CA. Below are the steps to generate private key, CSR (certificate Signing request)

## Generate private key
openssl genpkey -algorithm RSA -out private_key.pem -pkeyopt rsa_keygen_bits:2048

## Generate CSR (you will be prompted to enter metadata)

openssl req -new 
  -key private_key.pem \
  -out request.csr

## Inspect CSR
openssl req -in request.csr -text -noout

## You can send this request.csr to CA like DIgiCert to get the certificate which is accepted by Entra ID

## Command to get the fingerprint/thumbprint of the certificate (You will see the same thumbprint when you upload the public certificate to EntraID)

openssl x509 -in certificate.pem -noout -fingerprint -sha1