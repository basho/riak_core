openssl dsaparam 1024 -out params-site1.pem
openssl gendsa -out test/site1-key.pem params-site1.pem
openssl req -new -key test/site1-key.pem -out req-site1.pem -subj '/CN=US'
openssl x509 -req -in req-site1.pem -signkey test/site1-key.pem -out test/site1-cert.pem 
rm req-site1.pem params-site1.pem

openssl dsaparam 1024 -out params-site2.pem
openssl gendsa -out test/site2-key.pem params-site2.pem
openssl req -new -key test/site2-key.pem -out req-site2.pem -subj '/CN=US'
openssl x509 -req -in req-site2.pem -signkey test/site2-key.pem -out test/site2-cert.pem 
rm req-site2.pem params-site2.pem
