package persistence

import (
	"reflect"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cerr "github.com/pip-services3-go/pip-services3-commons-go/errors"
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	crefer "github.com/pip-services3-go/pip-services3-commons-go/refer"
	clog "github.com/pip-services3-go/pip-services3-components-go/log"
	gocb "gopkg.in/couchbase/gocb.v1"
)

/*
CouchbasePersistence abstract persistence component that stores data in Couchbase
and is based using Couchbaseose object relational mapping.

This is the most basic persistence component that is only
able to store data items of interface{} type. Specific CRUD operations
over the data items must be implemented in child classes by
accessing c._collection or c._model properties.

Configuration parameters:

- bucket:                      (optional) Couchbase bucket name
- connection(s):
  - discovery_key:             (optional) a key to retrieve the connection from connect.idiscovery.html IDiscovery
  - host:                      host name or IP address
  - port:                      port number (default: 27017)
  - uri:                       resource URI or connection string with all parameters in it
- credential(s):
  - store_key:                 (optional) a key to retrieve the credentials from auth.icredentialstore.html ICredentialStore
  - username:                  (optional) user name
  - password:                  (optional) user password
- options:
  - auto_create:               (optional) automatically create missing bucket (default: false)
  - auto_index:                (optional) automatically create primary index (default: false)
  - flush_enabled:             (optional) bucket flush enabled (default: false)
  - bucket_type:               (optional) bucket type (default: couchbase)
  - ram_quota:                 (optional) RAM quota in MB (default: 100)

 References:

- *:logger:*:*:1.0           (optional) ILogger components to pass log messages
- *:discovery:*:*:1.0        (optional) IDiscovery services
- *:credential-store:*:*:1.0 (optional) Credential stores to resolve credentials

Example:

	type MyCouchbasePersistence struct {
	  *CouchbasePersistence
	}

    func NewMyCouchbasePersistence() *MyCouchbasePersistence {
		c := MyCouchbasePersistence{}
		c.CouchbasePersistence = NewCouchbasePersistence(reflect.TypeOf(MyData{}), "mycollection");
		return &c;
    }

    func (c *MyCouchbasePersistence) GetOneById(correlationId string, id interface{}) (item interface{}, err error) {
		objectId := c.GenerateBucketId(id)

		buf := make(map[string]interface{}, 0)
		_, getErr := c.Bucket.Get(objectId, &buf)
		if getErr != nil {
			// Ignore "Key does not exist on the server" error
			if getErr == gocb.ErrKeyNotFound {
				return nil, nil
			}
			return nil, getErr
		}
		c.Logger.Trace(correlationId, "Retrieved from %s by id = %s", c.BucketName, objectId)
		item = c.ConvertFromMap(buf)
		return item, nil
	}

    func (c *IdentifiableCouchbasePersistence) Set(correlationId string, item interface{}) (result interface{}, err error) {
		if item == nil {
			return nil, nil
		}
		var newItem interface{}
		newItem = cmpersist.CloneObject(item)
		// Assign unique id if not exist
		cmpersist.GenerateObjectId(&newItem)
		id := cmpersist.GetObjectId(newItem)
		setItem := c.ConvertFromPublic(&newItem)
		objectId := c.GenerateBucketId(id)

		_, upsertErr := c.Bucket.Upsert(objectId, setItem, 0)

		if upsertErr != nil {
			return nil, upsertErr
		}

		c.Logger.Trace(correlationId, "Set in %s with id = %s", c.BucketName, id)
		c.ConvertToPublic(&newItem)
		return c.GetPtrIfNeed(newItem), nil
	}

    persistence := NewMyCouchbasePersistence();
    persistence.Configure(cconf.NewConfigParamsFromTuples(
        "host", "localhost",
        "port", 27017,
    ));

    persitence.Open("123")
         ...

	setItem, err := persistence.Set("123", MyData{ name: "ABC" })
	if setErr=== nil {
	 	item, err := persistence.GetOneById("123", setItem.Id)
        fmt.Println(item);                   // Result: { name: "ABC", Id:"..." }
    }
*/
type CouchbasePersistence struct {
	defaultConfig   *cconf.ConfigParams
	config          *cconf.ConfigParams
	references      cref.IReferences
	opened          bool
	localConnection bool
	//The dependency resolver.
	DependencyResolver *crefer.DependencyResolver
	//The logger.
	Logger *clog.CompositeLogger
	//The Couchbase connection component.
	Connection *CouchbaseConnection
	//The configuration options.
	Options *cconf.ConfigParams
	//The Couchbase cluster object.
	Cluster *gocb.Cluster
	//The Couchbase bucket name.
	BucketName string
	//The Couchbase bucket object.
	Bucket *gocb.Bucket
	// Prototype for convert
	Prototype reflect.Type
}

// NewCouchbasePersistence method are creates a new instance of the persistence component.
// Parameters:
//    - bucket    a bucket name.
// Returns:  *CouchbasePersistence pointer on new instance
func NewCouchbasePersistence(proto reflect.Type, bucket string) *CouchbasePersistence {
	cp := CouchbasePersistence{}
	cp.defaultConfig = cconf.NewConfigParamsFromTuples(
		"bucket", nil,
		"dependencies.connection", "*:connection:couchbase:*:1.0",
		"options.auto_create", false,
		"options.auto_index", true,
		"options.flush_enabled", true,
		"options.bucket_type", "couchbase",
		"options.ram_quota", "100",
	)

	cp.DependencyResolver = cref.NewDependencyResolverWithParams(cp.defaultConfig, cref.NewEmptyReferences())
	cp.Logger = clog.NewCompositeLogger()
	cp.Options = cconf.NewEmptyConfigParams()
	cp.BucketName = bucket
	cp.Prototype = proto
	return &cp
}

// Configure method are configures component by passing configuration parameters.
// - config    configuration parameters to be set.
func (c *CouchbasePersistence) Configure(config *cconf.ConfigParams) {
	config = config.SetDefaults(c.defaultConfig)
	c.config = config
	c.DependencyResolver.Configure(config)
	c.BucketName = config.GetAsStringWithDefault("bucket", c.BucketName)
	c.Options = c.Options.Override(config.GetSection("options"))
}

// SetReferences method are sets references to dependent components.
// 	- references 	references to locate the component dependencies.
func (c *CouchbasePersistence) SetReferences(references cref.IReferences) {
	c.references = references
	c.Logger.SetReferences(references)
	// Get connection
	c.DependencyResolver.SetReferences(references)
	resolve := c.DependencyResolver.GetOneOptional("connection")
	c.Connection, _ = resolve.(*CouchbaseConnection)
	// Or create a local one
	if c.Connection == nil {
		c.Connection = c.createConnection()
		c.localConnection = true
	} else {
		c.localConnection = false
	}
}

// UnsetReferences method is unsets (clears) previously set references to dependent components.
func (c *CouchbasePersistence) UnsetReferences() {
	c.Connection = nil
}

func (c *CouchbasePersistence) createConnection() *CouchbaseConnection {
	connection := NewCouchbaseConnection(c.BucketName)

	if c.config != nil {
		connection.Configure(c.config)
	}

	if c.references != nil {
		connection.SetReferences(c.references)
	}
	return connection
}

// IsOpen method are checks if the component is opened.
// Returns true if the component has been opened and false otherwise.
func (c *CouchbasePersistence) IsOpen() bool {
	return c.opened
}

// Open method are opens the component.
// - correlationId 	(optional) transaction id to trace execution through call chain.
// Return: error
// error or nil no errors occured.
func (c *CouchbasePersistence) Open(correlationId string) (err error) {
	if c.opened {
		return nil
	}

	if c.Connection == nil {
		c.Connection = c.createConnection()
		c.localConnection = true
	}

	if c.localConnection {
		err = c.Connection.Open(correlationId)
	}

	if err == nil && c.Connection == nil {
		err = cerr.NewInvalidStateError(correlationId, "NO_CONECTION", "Couchbase connection is missing")
	}

	if err == nil && !c.Connection.IsOpen() {
		err = cerr.NewConnectionError(correlationId, "CONNECT_FAILED", "Couchbase connection is not opened")
	}

	c.opened = false

	if err != nil {
		return err
	}
	c.Cluster = c.Connection.GetConnection()
	c.Bucket = c.Connection.GetBucket()
	c.BucketName = c.Connection.GetBucketName()
	c.opened = true

	return nil

}

// Close method are closes component and frees used resources.
// - correlationId 	(optional) transaction id to trace execution through call chain.
// Returns: error
// error or nil no errors occured.
func (c *CouchbasePersistence) Close(correlationId string) (err error) {
	if !c.opened {
		return nil
	}

	if c.Connection == nil {
		return cerr.NewInvalidStateError(correlationId, "NO_CONNECTION", "Couchbase connection is missing")
	}

	if c.localConnection {
		err = c.Connection.Close(correlationId)
	}
	c.opened = false
	c.Cluster = nil
	c.Bucket = nil
	return err
}

// Clear method are clears component state.
// - correlationId 	(optional) transaction id to trace execution through call chain.
// Returns: error
// error or nil no errors occured.
func (c *CouchbasePersistence) Clear(correlationId string) (err error) {
	// Return error if collection is not set
	if c.BucketName == "" {
		return cerr.NewError("Bucket name is not defined")
	}

	flushErr := c.Bucket.Manager(c.Connection.Authenticator.Username, c.Connection.Authenticator.Password).Flush()
	if flushErr != nil {
		return cerr.NewConnectionError(correlationId, "FLUSH_FAILED", "Couchbase bucket flush failed").
			WithCause(err)
	}
	return nil
}
