package connect

import (
	"strings"
	"time"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cerr "github.com/pip-services3-go/pip-services3-commons-go/errors"
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	clog "github.com/pip-services3-go/pip-services3-components-go/log"
	gocb "gopkg.in/couchbase/gocb.v1"
)

/*
CouchbaseConnection it is couchbase connection using plain couchbase driver.
This is the most basic persistence component that is only
able to store data items of any type. Specific CRUD operations
over the data items must be implemented in child classes by
accessing c.Connection properties.

Configuration parameters:

  - bucket:                      (optional) Couchbase bucket name
  - connection(s):
    - discovery_key:             (optional) a key to retrieve the connection from connect.idiscovery.html IDiscovery]]
    - host:                      host name or IP address
    - port:                      port number (default: 27017)
    - uri:                       resource URI or connection string with all parameters in it
  - credential(s):
    - store_key:                 (optional) a key to retrieve the credentials from auth.icredentialstore.html ICredentialStore]]
    - username:                  (optional) user name
    - password:                  (optional) user password
  - options:
    - auto_create:               (optional) automatically create missing bucket (default: false)
    - auto_index:                (optional) automatically create primary index (default: false)
    - flush_enabled:             (optional) bucket flush enabled (default: false)
    - bucket_type:               (optional) bucket type (default: couchbase)
    - ram_quota:                 (optional) RAM quota in MB (default: 100)

 References:

- *:logger:*:*:1.0           		(optional) ILogger components to pass log messages
- *:discovery:*:*:1.0        		(optional) IDiscovery services
- *:credential-store:\*:\*:1.0 		(optional) Credential stores to resolve credentials
*/
type CouchbaseConnection struct {
	defaultConfig *cconf.ConfigParams
	//The logger.
	Logger *clog.CompositeLogger
	//The connection resolver.
	ConnectionResolver *CouchbaseConnectionResolver
	//The configuration options.
	Options *cconf.ConfigParams
	//The Couchbase cluster connection object.
	Connection *gocb.Cluster
	//The Couchbase bucket name.
	BucketName string
	//The Couchbase bucket object.
	Bucket        *gocb.Bucket
	Authenticator gocb.PasswordAuthenticator
}

// NewCouchbaseConnection are creates a new instance of the connection component.
// Parameters:
//   - bucketName the name of couchbase bucket
func NewCouchbaseConnection(bucketName string) *CouchbaseConnection {
	c := CouchbaseConnection{}
	c.BucketName = bucketName
	c.defaultConfig = cconf.NewConfigParamsFromTuples(
		"bucket", nil,
		// connections.*
		// credential.*
		"options.auto_create", false,
		"options.auto_index", true,
		"options.flush_enabled", true,
		"options.bucket_type", "couchbase",
		"options.ram_quota", 100,
	)
	c.Logger = clog.NewCompositeLogger()
	c.ConnectionResolver = NewCouchbaseConnectionResolver()
	c.Options = cconf.NewEmptyConfigParams()
	return &c
}

// Configure are configures component by passing configuration parameters.
// Parameters:
//   - config    configuration parameters to be set.
func (c *CouchbaseConnection) Configure(config *cconf.ConfigParams) {
	config = config.SetDefaults(c.defaultConfig)
	c.ConnectionResolver.Configure(config)
	c.BucketName = config.GetAsStringWithDefault("bucket", c.BucketName)
	c.Options = c.Options.Override(config.GetSection("options"))
}

// SetReferences are sets references to dependent components.
// Parameters:
//   - references 	references to locate the component dependencies.
func (c *CouchbaseConnection) SetReferences(references cref.IReferences) {
	c.Logger.SetReferences(references)
	c.ConnectionResolver.SetReferences(references)
}

// IsOpen method are checks if the component is opened.
// Retrun true if the component has been opened and false otherwise.
func (c *CouchbaseConnection) IsOpen() bool {
	return c.Connection != nil
}

// Open method are opens the component.
// Parameters:
//   - correlationId  (optional) transaction id to trace execution through call chain.
// Returns: error
// error or nil no errors occured.
func (c *CouchbaseConnection) Open(correlationId string) (err error) {

	connection, resErr := c.ConnectionResolver.Resolve(correlationId)
	if resErr != nil {
		c.Logger.Error(correlationId, err, "Failed to resolve Couchbase connection")
		return resErr
	}

	c.Logger.Debug(correlationId, "Connecting to couchbase")

	cluster, conErr := gocb.Connect(connection.Uri)
	if conErr != nil {
		return conErr
	}
	c.Connection = cluster
	c.Authenticator = gocb.PasswordAuthenticator{
		Username: connection.Username,
		Password: connection.Password,
	}
	if connection.Username != "" {
		c.Connection.Authenticate(c.Authenticator)
	}
	err = nil
	newBucket := false

	autocreate := c.Options.GetAsBoolean("auto_create")
	if autocreate {

		bucketStrType := c.Options.GetAsStringWithDefault("bucket_type", "couchbase")
		bucketType := gocb.BucketType(0) // couchbase

		switch bucketStrType {
		case "couchbase":
			bucketType = gocb.BucketType(0)
			break
		case "memcached":
			bucketType = gocb.BucketType(1)
			break
		case "ephemeral":
			bucketType = gocb.BucketType(2)
			break
		}
		options := gocb.BucketSettings{
			Name:          c.BucketName,
			Password:      "",
			IndexReplicas: true,
			Replicas:      1,
			Type:          bucketType,
			Quota:         int(c.Options.GetAsLongWithDefault("ram_quota", 100)),
			FlushEnabled:  c.Options.GetAsBooleanWithDefault("flush_enabled", true),
		}

		err = c.Connection.Manager(connection.Username, connection.Password).InsertBucket(&options)

		if err != nil && err.Error() != "" && strings.Index(err.Error(), "name already exist") < 0 {
			c.Connection = nil
			c.Bucket = nil
			return err
		}

		if err == nil {
			newBucket = true
		}
		// Delay to allow couchbase to initialize the bucket
		// Otherwise opening will fail
		select {
		case <-time.After(time.Millisecond * 2000):
		}
	}

	bucket, opnErr := c.Connection.OpenBucket(c.BucketName, "")
	if opnErr != nil {
		c.Logger.Error(correlationId, err, "Failed to open bucket")
		err = cerr.NewConnectionError(correlationId, "CONNECT_FAILED", "Connection to couchbase failed").WithCause(opnErr)
		c.Bucket = nil
		c.Connection = nil
		c.Bucket = nil
		return err
	}
	c.Logger.Debug(correlationId, "Connected to couchbase bucket %s", c.BucketName)
	c.Bucket = bucket

	autoIndex := c.Options.GetAsBoolean("auto_index")
	if newBucket || autoIndex {

		err = c.Bucket.Manager("", "").CreatePrimaryIndex("", true, false)
		if err != nil {
			c.Connection = nil
			c.Bucket = nil
			return err
		}
	}

	return nil
}

// Closes component and frees used resources.
// Parameters:
//   - correlationId (optional) transaction id to trace execution through call chain.
// Returns: error
// error or null no errors occured.
func (c *CouchbaseConnection) Close(correlationId string) (err error) {
	if c.Bucket != nil {
		c.Bucket.Close()
	}
	c.Connection = nil
	c.Bucket = nil
	c.Logger.Debug(correlationId, "Disconnected from couchbase bucket %s", c.BucketName)
	return nil
}

// GetConnection method are return opened connection
func (c *CouchbaseConnection) GetConnection() *gocb.Cluster {
	return c.Connection
}

// GetBucket method are returned opened bucket
func (c *CouchbaseConnection) GetBucket() *gocb.Bucket {
	return c.Bucket
}

// GetBucketName method are returned bucket name
func (c *CouchbaseConnection) GetBucketName() string {
	return c.BucketName
}
