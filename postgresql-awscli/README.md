Encryption is set if ENCRYPTION_PASSWORD is set.

To decrypt later use

openssl enc -aes-256-cbc -d -a -pbkdf2 -iter 600000 -pass pass:"$ENCRYPTION_PASSWORD" -in dump.gz.enc -out dump.gz
