package persistence

import (
	"strings"
	"sync"
	"time"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cerr "github.com/pip-services3-go/pip-services3-commons-go/errors"
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	clog "github.com/pip-services3-go/pip-services3-components-go/log"
	couchcon "github.com/pip-services3-go/pip-services3-couchbase-go/connect"
	gocb "gopkg.in/couchbase/gocb.v1"
)

/*
Couchbase connection using plain couchbase driver.

This is the most basic persistence component that is only
able to store data items of any type. Specific CRUD operations
over the data items must be implemented in child classes by
accessing c._collection or c._model properties.

 Configuration parameters

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
 *\
 References

- \*:logger:\*:\*:1.0           (optional) ILogger components to pass log messages
- \*:discovery:\*:\*:1.0        (optional)IDiscovery services
- \*:credential-store:\*:\*:1.0 (optional) Credential stores to resolve credentials
*/
// IReferenceable, IConfigurable, IOpenable

type CouchbaseConnection struct {
	defaultConfig *cconf.ConfigParams
	//The logger.
	Logger *clog.CompositeLogger
	//The connection resolver.
	ConnectionResolver *couchcon.CouchbaseConnectionResolver
	//The configuration options.
	Options *cconf.ConfigParams
	//The Couchbase cluster connection object.
	Connection *gocb.Cluster
	//The Couchbase bucket name.
	BucketName string
	//The Couchbase bucket object.
	Bucket *gocb.Bucket
}

/*
   Creates a new instance of the connection component.
   - bucketName the name of couchbase bucket
*/
func NewCouchbaseConnection(bucketName string) *CouchbaseConnection {
	cc := CouchbaseConnection{}
	cc.BucketName = bucketName
	cc.defaultConfig = cconf.NewConfigParamsFromTuples(
		"bucket", nil,

		// connections.*
		// credential.*

		"options.auto_create", false,
		"options.auto_index", true,
		"options.flush_enabled", true,
		"options.bucket_type", "couchbase",
		"options.ram_quota", 100,
	)
	cc.Logger = clog.NewCompositeLogger()
	cc.ConnectionResolver = couchcon.NewCouchbaseConnectionResolver()
	cc.Options = cconf.NewEmptyConfigParams()
	return &cc
}

/*
   Configures component by passing configuration parameters.

   - config    configuration parameters to be set.
*/
func (c *CouchbaseConnection) Configure(config *cconf.ConfigParams) {
	config = config.SetDefaults(c.defaultConfig)
	c.ConnectionResolver.Configure(config)
	c.BucketName = config.GetAsStringWithDefault("bucket", c.BucketName)
	c.Options = c.Options.Override(config.GetSection("options"))
}

/*
	Sets references to dependent components.

	- references 	references to locate the component dependencies.
*/
func (c *CouchbaseConnection) SetReferences(references cref.IReferences) {
	c.Logger.SetReferences(references)
	c.ConnectionResolver.SetReferences(references)
}

/*
	Checks if the component is opened.

	Retrun true if the component has been opened and false otherwise.
*/
func (c *CouchbaseConnection) IsOpen() bool {
	// return c.Connection.readyState == 1;
	return c.Connection != nil
}

/*
	Opens the component.

	- correlationId 	(optional) transaction id to trace execution through call chain.
    - callback 			callback function that receives error or null no errors occured.
*/
func (c *CouchbaseConnection) Open(correlationId string) (err error) {

	connection, resErr := c.ConnectionResolver.Resolve(correlationId)
	if resErr != nil {
		c.Logger.Error(correlationId, err, "Failed to resolve Couchbase connection")
		return resErr
	}

	c.Logger.Debug(correlationId, "Connecting to couchbase")

	conn, conErr := gocb.Connect(connection.Uri)
	if conErr != nil {
		return conErr
	}
	c.Connection = conn
	if connection.Username != "" {
		c.Connection.Authenticate(gocb.PasswordAuthenticator{
			Username: connection.Username,
			Password: connection.Password,
		})
	}
	err = nil
	wg := sync.WaitGroup{}
	newBucket := false
	wg.Add(1)

	go func() {
		defer wg.Done()
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
				Name:         c.BucketName,
				Type:         bucketType,
				Quota:        int(c.Options.GetAsLongWithDefault("ram_quota", 100)),
				FlushEnabled: c.Options.GetAsBooleanWithDefault("flush_enabled", true),
			}
			crtErr := c.Connection.Manager("", "").InsertBucket(&options)
			if crtErr != nil {
				err = crtErr
				return
			}
			if err.Error() != "" && strings.Index(err.Error(), "name already exist") > 0 {
				err = nil
				return
			}
			newBucket = true
			// Delay to allow couchbase to initialize the bucket
			// Otherwise opening will fail
			select {
			case <-time.After(time.Millisecond * 2000):
			}
		}
	}()
	wg.Wait()

	if err != nil {
		c.Connection = nil
		c.Bucket = nil
		return err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		bucket, opnErr := c.Connection.OpenBucket(c.BucketName, "")
		if opnErr != nil {
			c.Logger.Error(correlationId, err, "Failed to open bucket")
			err = cerr.NewConnectionError(correlationId, "CONNECT_FAILED", "Connection to couchbase failed").WithCause(err)
			c.Bucket = nil
			err = opnErr
			return
		}
		c.Logger.Debug(correlationId, "Connected to couchbase bucket %s", c.BucketName)
		c.Bucket = bucket
	}()
	wg.Wait()
	if err != nil {
		c.Connection = nil
		c.Bucket = nil
		return err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		autoIndex := c.Options.GetAsBoolean("auto_index")
		if !newBucket && !autoIndex {
			return
		}

		idxErr := c.Bucket.Manager("", "").CreatePrimaryIndex("", true, false)
		if idxErr != nil {
			err = idxErr
			return
		}
	}()
	wg.Wait()
	if err != nil {
		c.Connection = nil
		c.Bucket = nil
		return err
	}
	return nil
}

/*
	Closes component and frees used resources.
	 *
	- correlationId 	(optional) transaction id to trace execution through call chain.
    - callback 			callback function that receives error or null no errors occured.
*/
func (c *CouchbaseConnection) Close(correlationId string) (err error) {
	if c.Bucket != nil {
		c.Bucket.Close()
	}
	c.Connection = nil
	c.Bucket = nil
	c.Logger.Debug(correlationId, "Disconnected from couchbase bucket %s", c.BucketName)
	return nil
}

func (c *CouchbaseConnection) GetConnection() *gocb.Cluster {
	return c.Connection
}

func (c *CouchbaseConnection) GetBucket() *gocb.Bucket {
	return c.Bucket
}

func (c *CouchbaseConnection) GetBucketName() string {
	return c.BucketName
}
