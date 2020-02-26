package connect

import (
	"strconv"
	"sync"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"
	cerr "github.com/pip-services3-go/pip-services3-commons-go/errors"
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	"github.com/pip-services3-go/pip-services3-components-go/auth"
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
	ccr.ConnectionResolver = ccon.NewEmptyConnectionResolver()
	ccr.CredentialResolver = cauth.NewEmptyCredentialResolver()
	return &ccr
}

/*
   Configures component by passing configuration parameters.

   - config    configuration parameters to be set.
*/
func (c *CouchbaseConnectionResolver) Configure(config *cconf.ConfigParams) {
	c.ConnectionResolver.Configure(config)
	c.CredentialResolver.Configure(config)
}

/*
	Sets references to dependent components.

	- references 	references to locate the component dependencies.
*/
func (c *CouchbaseConnectionResolver) SetReferences(references cref.IReferences) {
	c.ConnectionResolver.SetReferences(references)
	c.CredentialResolver.SetReferences(references)
}

func (c *CouchbaseConnectionResolver) validateConnection(correlationId string, connection *ccon.ConnectionParams) error {
	uri := connection.Uri()
	if uri != "" {
		return nil
	}

	host := connection.Host()
	if host == "" {
		return cerr.NewConfigError(correlationId, "NO_HOST", "Connection host is not set")
	}

	port := connection.Port()
	if port == 0 {
		return cerr.NewConfigError(correlationId, "NO_PORT", "Connection port is not set")
	}
	// database = connection.getAsNullableString("database");
	// if database == ""{
	//     return cerr.NewConfigError(correlationId, "NO_DATABASE", "Connection database is not set");
	// }

	return nil
}

func (c *CouchbaseConnectionResolver) validateConnections(correlationId string, connections []*ccon.ConnectionParams) error {
	if connections == nil || len(connections) == 0 {
		return cerr.NewConfigError(correlationId, "NO_CONNECTION", "Database connection is not set")
	}

	for _, connection := range connections {
		err := c.validateConnection(correlationId, connection)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *CouchbaseConnectionResolver) composeConnection(connections []*ccon.ConnectionParams, credential *cauth.CredentialParams) *CouchbaseConnectionParams {
	result := new(CouchbaseConnectionParams)

	if credential != nil {
		result.Username = credential.Username()
		if result.Username != "" {
			result.Password = credential.Password()
		}
	}

	// If there is a uri then return it immediately
	for _, connection := range connections {
		result.Uri = connection.Uri()
		if result.Uri != "" {
			return result
		}
	}

	// Define hosts
	hosts := ""
	for _, connection := range connections {
		host := connection.Host()
		port := connection.Port()

		if len(hosts) > 0 {
			hosts += ","
		}
		if port > 0 {
			host = host + ":" + strconv.FormatInt(int64(port), 10)
		}
		hosts += host
	}

	// Define database
	database := ""
	for _, connection := range connections {
		if database == "" {
			database = connection.GetAsString("database")
		}

	}

	if len(database) > 0 {
		database = "/" + database
	}

	// Define additional parameters parameters
	consConf := cdata.NewEmptyStringValueMap()
	for _, v := range connections {
		consConf.Append(v.Value())
	}
	var options *cconf.ConfigParams
	if credential != nil {
		options = cconf.NewConfigParamsFromMaps(consConf.Value(), credential.Value())
	} else {
		options = cconf.NewConfigParamsFromValue(consConf.Value())
	}
	options.Remove("uri")
	options.Remove("host")
	options.Remove("port")
	options.Remove("database")
	options.Remove("username")
	options.Remove("password")
	params := ""
	keys := options.Keys()

	for _, key := range keys {
		if len(params) > 0 {
			params += "&"
		}

		params += key

		value := options.GetAsString(key)
		if value != "" {
			params += "=" + value
		}
	}
	if len(params) > 0 {
		params = "?" + params
	}

	// Compose uri
	result.Uri = "couchbase://" + hosts + database + params

	return result
}

/*
   Resolves Couchbase connection URI from connection and credential parameters.
   Parameters:
   - correlationId     (optional) transaction id to trace execution through call chain.
   - callback 			callback function that receives resolved URI or error.
*/
func (c *CouchbaseConnectionResolver) Resolve(correlationId string) (connection *CouchbaseConnectionParams, err error) {
	var connections []*ccon.ConnectionParams
	var credential *auth.CredentialParams
	var errCred, errConn error

	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		connections, errConn = c.ConnectionResolver.ResolveAll(correlationId)
		//Validate connections
		if errConn == nil {
			errConn = c.validateConnections(correlationId, connections)
		}
	}()
	go func() {
		defer wg.Done()
		credential, errCred = c.CredentialResolver.Lookup(correlationId)
		// Credentials are not validated right now
	}()
	wg.Wait()

	if errConn != nil {
		return nil, errConn
	}
	if errCred != nil {
		return nil, errCred
	}
	connection = c.composeConnection(connections, credential)
	return connection, nil
}
