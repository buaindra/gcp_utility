### Ref
1. https://stackoverflow.com/questions/71423922/connecting-to-postgresql-from-python-3-running-in-cloud-shell-password-authent
2. CloudSQL Connector for Python: https://github.com/GoogleCloudPlatform/cloud-sql-python-connector


### CloudSQL - Postgres
1. gcloud sql connect postgres-instance --user=postgres --quiet
2. \d or \dt   * to check databases or tables or relations *
3. \l  * to check databases *
4. \c <database_name>  * to change the database *
5. select * from table;

### Public IP vs Private IP
1. Public IP is more secure, it uses the ssl certificate but private ip is open for other resources which are belongs to same vpc network.
2. for Public IP, you need to whitelisting your app IP to access the cloudsql, for private ip you can directly use it by providing password.
3. Cloud SQL proxy is the best solution to connect cloud sql.
