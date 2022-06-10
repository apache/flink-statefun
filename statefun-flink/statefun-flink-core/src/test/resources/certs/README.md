# Testing scenario

There are three certificate authorities: `a`, `b` and `c`. Each sign a client cert and a server cert.

There are three sets of client cert and key files

1. `a_client.crt` - signed by CA `a`, and its private key `a_client.key.p8.`, the latter of which has password of `test`
2. `b_client.crt` - signed by CA `b`, and its private key `b_client.key.p8`, the latter of which has password of `test`
3. `c_client.crt` - signed by CA `c`, and its private key `c_client.key.p8`, the latter of which does not require a password

There is one server cert and key file

1. `a_server.crt` - signed by CA `a`, and its private key `a_server.key.p8`, the latter of which has password of `test`

There are two trusted CA files

1. `a_caCerts.pem` - contains CAs `a` and `c`
2. `b_caCerts.pem` contains CAs `b` and `c`

A test server `a` is launched with a truststore containing CAs `a` and `c`

| client | server | connection accepted |
|--------|--------|---------------------|
| a      | a      | yes                 |
| b      | a      | no                  |
| c      | a      | yes                 |

# Steps for re-creating the files in this folder
## 1. create private keys for 3 separate certificate authorities

```shell 
openssl genrsa -des3 -out a_ca.key -passout pass:test 2048
```

```shell 
openssl genrsa -des3 -out b_ca.key -passout pass:test 2048
```

```shell 
openssl genrsa -des3 -out c_ca.key -passout pass:test 2048
```

## 2. generate a root certificate for each of those keys

```shell
openssl req -x509 -new -nodes -key a_ca.key -sha256 -days 36500 -out a_ca.pem -passin pass:test
```

defaults used everywhere except for CommonName. Output:

```text
    Country Name (2 letter code) [AU]:
    State or Province Name (full name) [Some-State]:
    Locality Name (eg, city) []:
    Organization Name (eg, company) [Internet Widgits Pty Ltd]:
    Organizational Unit Name (eg, section) []:
    Common Name (e.g. server FQDN or YOUR name) []:a
    Email Address []:
```

same for b and c

```shell
openssl req -x509 -new -nodes -key b_ca.key -sha256 -days 36500 -out b_ca.pem -passin pass:test
```

```shell
openssl req -x509 -new -nodes -key c_ca.key -sha256 -days 36500 -out c_ca.pem -passin pass:test
```

## 3. create PKCS8 client and server keys and CSRs

```shell
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -v1 PBE-SHA1-RC4-128 -out a_client.key.p8 -passout pass:test
openssl req -new -key a_client.key.p8 -out a_client.key.p8.csr -passin pass:test
```

again, all defaults were used, except for CommonName. Output:

```text
    You are about to be asked to enter information that will be incorporated
    into your certificate request.
    What you are about to enter is what is called a Distinguished Name or a DN.
    There are quite a few fields but you can leave some blank
    For some fields there will be a default value,
    If you enter '.', the field will be left blank.
    -----
    Country Name (2 letter code) [AU]:
    State or Province Name (full name) [Some-State]:
    Locality Name (eg, city) []:
    Organization Name (eg, company) [Internet Widgits Pty Ltd]:
    Organizational Unit Name (eg, section) []:
    Common Name (e.g. server FQDN or YOUR name) []:        
    Email Address []:
    
    Please enter the following 'extra' attributes
    to be sent with your certificate request
    A challenge password []:
    An optional company name []:
```

same for the other keys - the rest of clients and also servers

```shell
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -v1 PBE-SHA1-RC4-128 -out a_server.key.p8 -passout pass:test
openssl req -new -key a_server.key.p8 -out a_server.key.p8.csr -passin pass:test
```

```shell
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -v1 PBE-SHA1-RC4-128 -out b_client.key.p8 -passout pass:test
openssl req -new -key b_client.key.p8 -out b_client.key.p8.csr -passin pass:test
```

(note no pass for c_client)
```shell
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -v1 PBE-SHA1-RC4-128 -out c_client.key.p8 -nocrypt
openssl req -new -key c_client.key.p8 -out c_client.key.p8.csr 
```

## 4. create an extension config for servers

create server.ext

```text
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = remote-function-host
```

## 5. create certificates using our CSR, CA private keys, CA certificates and config files

```shell
openssl x509 -req -in a_client.key.p8.csr -passin pass:test -CA a_ca.pem -CAkey a_ca.key -CAcreateserial -out a_client.crt -days 36500 -sha256
```

output:

```text
    Signature ok
    subject=C = AU, ST = Some-State, O = Internet Widgits Pty Ltd, CN = a_client.csr
    Getting CA Private Key
```

same for other clients and servers (note that clients `b` and `c` are signed ba CA `b` and `c` respectively, and that the additional -extfile server.ext for
servers)

```shell
openssl x509 -req -in b_client.key.p8.csr -passin pass:test -CA b_ca.pem -CAkey b_ca.key -CAcreateserial -out b_client.crt -days 36500 -sha256
```

```shell
openssl x509 -req -in c_client.key.p8.csr -passin pass:test -CA c_ca.pem -CAkey c_ca.key -CAcreateserial -out c_client.crt -days 36500 -sha256
```

```shell
openssl x509 -req -in a_server.key.p8.csr -passin pass:test -CA a_ca.pem -CAkey a_ca.key -CAcreateserial -out a_server.crt -days 36500 -sha256 -extfile server.ext
```

## 6. create final files

combine CA `c` and `a` to form a_caCerts.pem

```shell
echo -e "$(cat c_ca.pem)\n$(cat a_ca.pem)" > a_caCerts.pem
```

combine CA `b` and `c` to form b_caCerts.pem

```shell
echo -e "$(cat b_ca.pem)\n$(cat c_ca.pem)" > b_caCerts.pem
```