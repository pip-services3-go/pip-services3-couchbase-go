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
Abstract persistence component that stores data in Couchbase
and is based using Couchbaseose object relational mapping.
 *
This is the most basic persistence component that is only
able to store data items of interface{} type. Specific CRUD operations
over the data items must be implemented in child classes by
accessing c._collection or c._model properties.
 *
 Configuration parameters
 *
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
 *
 References
 *
- \*:logger:\*:\*:1.0           (optional) ILogger components to pass log messages
- \*:discovery:\*:\*:1.0        (optional) IDiscovery services
- \*:credential-store:\*:\*:1.0 (optional) Credential stores to resolve credentials

 Example

    class MyCouchbasePersistence extends CouchbasePersistence<MyData> {

      func (c* CouchbasePersistence) constructor() {
          base("mydata", "mycollection", new MyDataCouchbaseSchema());
    }

    func (c* CouchbasePersistence) getByName(correlationId: string, name: string, callback: (err, item) => void) {
        let criteria = { name: name };
        c._model.findOne(criteria, callback);
    });

    func (c* CouchbasePersistence) set(correlatonId: string, item: MyData, callback: (err) => void) {
        let criteria = { name: item.name };
        let options = { upsert: true, new: true };
        c._model.findOneAndUpdate(criteria, item, options, callback);
    }

    }

    let persistence = new MyCouchbasePersistence();
    persistence.configure(ConfigParams.fromTuples(
        "host", "localhost",
        "port", 27017
    ));

    persitence.open("123", (err) => {
         ...
    });

    persistence.set("123", { name: "ABC" }, (err) => {
        persistence.getByName("123", "ABC", (err, item) => {
            console.log(item);                   // Result: { name: "ABC" }
        });
    });
*/
// implements IReferenceable, IUnreferenceable, IConfigurable, IOpenable, ICleanable
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
	//The Couchbase N1qlQuery object.
	//Query *gocb.N1qlQuery

	Prototype reflect.Type
}

//    Creates a new instance of the persistence component.
//    - bucket    (optional) a bucket name.
func NewCouchbasePersistence(proto reflect.Type, bucket string) *CouchbasePersistence {
	cp := CouchbasePersistence{}
	cp.defaultConfig = cconf.NewConfigParamsFromTuples(
		"bucket", nil,
		"dependencies.connection", "*:connection:couchbase:*:1.0",

		// connections.*
		// credential.*

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

//    Configures component by passing configuration parameters.
// - config    configuration parameters to be set.
func (c *CouchbasePersistence) Configure(config *cconf.ConfigParams) {
	config = config.SetDefaults(c.defaultConfig)
	c.config = config
	c.DependencyResolver.Configure(config)
	c.BucketName = config.GetAsStringWithDefault("bucket", c.BucketName)
	c.Options = c.Options.Override(config.GetSection("options"))
}

/*
	Sets references to dependent components.
	 *
	- references 	references to locate the component dependencies.
*/
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

//	Unsets (clears) previously set references to dependent components.

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

// Converts object value from internal to func (c* CouchbasePersistence) format.
// - value     an object in internal format to convert.
// Returns converted object in func (c* CouchbasePersistence) format.
func (c *CouchbasePersistence) convertToPublic(value interface{}) interface{} {
	// if value && value.toJSON
	//     value = value.toJSON();
	return value
}

// Convert object value from func (c* CouchbasePersistence) to internal format.
// - value     an object in func (c* CouchbasePersistence) format to convert.
// Returns converted object in internal format.
func (c *CouchbasePersistence) convertFromPublic(value interface{}) interface{} {
	return value
}

// Checks if the component is opened.
// Returns true if the component has been opened and false otherwise.
func (c *CouchbasePersistence) IsOpen() bool {
	return c.opened
}

// Opens the component.
// - correlationId 	(optional) transaction id to trace execution through call chain.
// - callback 			callback function that receives error or nil no errors occured.
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
	//c.Query = gocb.NewN1qlQuery("")
	c.opened = true

	return nil

}

// Closes component and frees used resources.
// - correlationId 	(optional) transaction id to trace execution through call chain.
// - callback 			callback function that receives error or nil no errors occured.
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
	//c.Query = nil
	return err
}

// Clears component state.
// - correlationId 	(optional) transaction id to trace execution through call chain.
// - callback 			callback function that receives error or nil no errors occured.
func (c *CouchbasePersistence) Clear(correlationId string) (err error) {
	// Return error if collection is not set
	if c.BucketName == "" {
		return cerr.NewError("Bucket name is not defined")
	}

	flushErr := c.Bucket.Manager("", "").Flush()
	if flushErr != nil {
		return cerr.NewConnectionError(correlationId, "FLUSH_FAILED", "Couchbase bucket flush failed").
			WithCause(err)
	}
	return nil
}
