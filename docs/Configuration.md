# Configuration Guide <br/>

Configuration structure follows the 
[standard configuration](https://github.com/pip-services/pip-services3-container-node/doc/Configuration.md) 
structure. 

### <a name="persistence_couchbase"></a> Couchbase

Couchbase persistence has the following configuration properties:
- collection:                  (optional) Couchbase collection name
- connection(s):
  - discovery_key:             (optional) a key to retrieve the connection from IDiscovery
  - host:                      host name or IP address
  - port:                      port number (default: 8091)
  - uri:                       resource URI or connection string with all parameters in it
- credential(s):
  - store_key:                 (optional) a key to retrieve the credentials from ICredentialStore
  - username:                  (optional) user name
  - password:                  (optional) user password
- options:
  - max_pool_size:             (optional) maximum connection pool size (default: 2)
  - keep_alive:                (optional) enable connection keep alive (default: true)
  - connect_timeout:           (optional) connection timeout in milliseconds (default: 5000)
  - socket_timeout:            (optional) socket timeout in milliseconds (default: 360000)
  - auto_reconnect:            (optional) enable auto reconnection (default: true) (not used)
  - reconnect_interval:        (optional) reconnection interval in milliseconds (default: 1000) (not used)
  - max_page_size:             (optional) maximum page size (default: 100)
  - replica_set:               (optional) name of replica set
  - ssl:                       (optional) enable SSL connection (default: false) (not implements in this release)
  - auth_source:               (optional) authentication source
  - auth_user:                 (optional) authentication user name
  - auth_password:             (optional) authentication user password
  - debug:                

Example:
```yaml
- descriptor: "pip-services-clusters:persistence:couchbase:default:1.0"
  collection: "clusters"
  connection:
    uri: "couchbase://localhost/pipservicestest"
    host: "localhost"
    port: 8091
    database: "pipservicestest"
  credential:
    username: "user_db"
    password: "passwd_db"
```

For more information on this section read 
[Pip.Services Configuration Guide](https://github.com/pip-services/pip-services3-container-node/doc/Configuration.md#deps)