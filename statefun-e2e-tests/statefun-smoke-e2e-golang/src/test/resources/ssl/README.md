1. Generate a private key for the GoLang server

```bash
openssl ecparam -genkey -name secp384r1 -out server.key
```

2. Generate a certificate from that private key.

```bash
openssl req -new -x509 -sha256 -key server.key -out server.crt -days 3650 -subj '/CN=remote-function-host'
```
