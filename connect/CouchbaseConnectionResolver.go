package connect

import (
	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cauth "github.com/pip-services3-go/pip-services3-components-go/auth"
	ccon "github.com/pip-services3-go/pip-services3-components-go/connect"
)

/*
Helper class that resolves Couchbase connection and credential parameters,
validates them and generates a connection URI.

It is able to process multiple connections to Couchbase cluster nodes.

  Configuration parameters

- connection(s):
  - discovery_key:               (optional) a key to retrieve the connection from https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/connect.idiscovery.html IDiscovery
  - host:                        host name or IP address
  - port:                        port number (default: 27017)
  - database:                    database (bucket) name
  - uri:                         resource URI or connection string with all parameters in it
- credential(s):
  - store_key:                   (optional) a key to retrieve the credentials from https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/auth.icredentialstore.html ICredentialStore
  - username:                    user name
  - password:                    user password

 References

- *:discovery:\*:\*:1.0             (optional) https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/connect.idiscovery.html IDiscovery services
- *:credential-store:\*:\*:1.0      (optional) Credential stores to resolve credentials
*/
//implements IReferenceable, IConfigurable

type CouchbaseConnectionResolver struct {
	/*
	   The connections resolver.
	*/
	ConnectionResolver *ccon.ConnectionResolver
	/*
	   The credentials resolver.
	*/
	CredentialResolver *cauth.CredentialResolver
}

func NewCouchbaseConnectionResolver() *CouchbaseConnectionResolver {
	ccr := CouchbaseConnectionResolver{}
	ccr.ConnectionResolver = ccon.NewConnectionResolver()
	ccr.CredentialResolver = cauth.NewCredentialResolver()
	return &ccr
}

/*
   Configures component by passing configuration parameters.

   - config    configuration parameters to be set.
*/
func (c *CouchbaseConnectionResolver) Configure(config cconf.ConfigParams) {
	c.ConnectionResolver.Configure(config)
	c.CredentialResolver.Configure(config)
}

//     /*
// 	Sets references to dependent components.
//
// 	- references 	references to locate the component dependencies.
//     */
//     func (c*CouchbaseConnectionResolver) SetReferences(references: IReferences) {
//         c.ConnectionResolver.setReferences(references);
//         c.CredentialResolver.setReferences(references);
//     }

//     func (c *CouchbaseConnectionResolver) validateConnection(correlationId: string, connection: ConnectionParams): any {
//         let uri = connection.getUri();
//         if (uri != null) return null;

//         let host = connection.getHost();
//         if (host == null)
//             return new ConfigException(correlationId, "NO_HOST", "Connection host is not set");

//         let port = connection.getPort();
//         if (port == 0)
//             return new ConfigException(correlationId, "NO_PORT", "Connection port is not set");

//         // let database = connection.getAsNullableString("database");
//         // if (database == null)
//         //     return new ConfigException(correlationId, "NO_DATABASE", "Connection database is not set");

//         return null;
//     }

//     func (c *CouchbaseConnectionResolver) validateConnections(correlationId: string, connections: ConnectionParams[]): any {
//         if (connections == null || connections.length == 0)
//             return new ConfigException(correlationId, "NO_CONNECTION", "Database connection is not set");

//         for (let connection of connections) {
//             let error = c.validateConnection(correlationId, connection);
//             if (error) return error;
//         }

//         return null;
//     }

//     func (c *CouchbaseConnectionResolver) composeConnection(connections: ConnectionParams[], credential: CredentialParams): CouchbaseConnectionParams {
//         let result = new CouchbaseConnectionParams();

//         if (credential) {
//             result.username = credential.getUsername();
//             if (result.username)
//                 result.password = credential.getPassword();
//         }

//         // If there is a uri then return it immediately
//         for (let connection of connections) {
//             result.uri = connection.getUri();
//             if (result.uri) return result;
//         }

//         // Define hosts
//         let hosts = "";
//         for (let connection of connections) {
//             let host = connection.getHost();
//             let port = connection.getPort();

//             if (hosts.length > 0)
//                 hosts += ",";
//             hosts += host + (port == null ? "" : ":" + port);
//         }

//         // Define database
//         let database = "";
//         for (let connection of connections) {
//             database = database || connection.getAsNullableString("database");
//         }
//         database = database || "";
//         if (database.length > 0)
//             database = "/" + database;

//         // Define additional parameters parameters
//         let options = ConfigParams.mergeConfigs(...connections).override(credential);
//         options.remove("uri");
//         options.remove("host");
//         options.remove("port");
//         options.remove("database");
//         options.remove("username");
//         options.remove("password");
//         let params = "";
//         let keys = options.getKeys();
//         for (let key of keys) {
//             if (params.length > 0)
//                 params += "&";

//             params += key;

//             let value = options.getAsString(key);
//             if (value != null)
//                 params += "=" + value;
//         }
//         if (params.length > 0)
//             params = "?" + params;

//         // Compose uri
//         result.uri = "couchbase://" + hosts + database + params;

//         return result;
//     }

//     /*
//     Resolves Couchbase connection URI from connection and credential parameters.
//
//     - correlationId     (optional) transaction id to trace execution through call chain.
//     - callback 			callback function that receives resolved URI or error.
//     */
//     func (c*CouchbaseConnectionResolver) resolve(correlationId: string, callback: (err: any, connection: CouchbaseConnectionParams) => void) {
//         let connections: ConnectionParams[];
//         let credential: CredentialParams;

//         async.parallel([
//             (callback) => {
//                 c.ConnectionResolver.resolveAll(correlationId, (err: any, result: ConnectionParams[]) => {
//                     connections = result;

//                     // Validate connections
//                     if (err == null)
//                         err = c.validateConnections(correlationId, connections);

//                     callback(err);
//                 });
//             },
//             (callback) => {
//                 c.CredentialResolver.lookup(correlationId, (err: any, result: CredentialParams) => {
//                     credential = result;

//                     // Credentials are not validated right now

//                     callback(err);
//                 });
//             }
//         ], (err) => {
//             if (err)
//                 callback(err, null);
//             else {
//                 let connection = c.composeConnection(connections, credential);
//                 callback(null, connection);
//             }
//         });
//     }

// }
