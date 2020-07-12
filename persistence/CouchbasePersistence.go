package persistence

import (
	"encoding/json"
	"math/rand"
	"reflect"
	"strconv"
	"time"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cconv "github.com/pip-services3-go/pip-services3-commons-go/convert"
	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"
	cerr "github.com/pip-services3-go/pip-services3-commons-go/errors"
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	crefer "github.com/pip-services3-go/pip-services3-commons-go/refer"
	clog "github.com/pip-services3-go/pip-services3-components-go/log"
	cmpersist "github.com/pip-services3-go/pip-services3-data-go/persistence"
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

	CollectionName string

	MaxPageSize    int
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


// ConvertFromPublic method help convert object (map) from public view by added "_c" field with collection name
// Parameters:
// 	- item *interface{} item for convert
// Returns: *interface{} converted item
func (c *CouchbasePersistence) ConvertFromPublic(item *interface{}) *interface{} {
	var value interface{} = *item
	if reflect.TypeOf(item).Kind() != reflect.Ptr {
		panic("ConvertFromPublic:Error! Item is not a pointer!")
	}

	if reflect.TypeOf(value).Kind() == reflect.Map {
		m, ok := value.(map[string]interface{})
		if ok {
			m["_c"] = c.CollectionName
			return item
		}
		return item;
	}

	if reflect.TypeOf(value).Kind() == reflect.Struct {
		jsonVal, _ := json.Marshal(*item)
		resMap := make(map[string]interface{}, 0)
		json.Unmarshal(jsonVal, &resMap)
		resMap["_c"] = c.CollectionName
		var result interface{} = resMap
		return &result
	}
	panic("ConvertFromPublic:Error! Item must to be a map[string]interface{} or struct!")
}

// ConvertToPublic method is convert object (map) to public view by exluded "_c" field
// Parameters:
// 	- item *interface{}  item for convert
// Returns: *interface{} converted item
func (c *CouchbasePersistence) ConvertToPublic(item *interface{}) {
	var value interface{} = *item
	if reflect.TypeOf(item).Kind() != reflect.Ptr {
		panic("ConvertToPublic:Error! Item is not a pointer!")
	}

	if reflect.TypeOf(value).Kind() == reflect.Map {
		m, ok := value.(map[string]interface{})
		if ok {
			delete(m, "_c")
			return
		}
	}

	if reflect.TypeOf(value).Kind() == reflect.Struct {
		return
	}
	panic("ConvertToPublic:Error! Item must to be a map[string]interface{} or struct!")
}


// GenerateBucketId method are generates unique id for specific collection in the bucket
// Parameters:
// - value a public unique id.
// Retruns a unique bucket id.
func (c *CouchbasePersistence) GenerateBucketId(value interface{}) string {
	if value == nil {
		return ""
	}
	return c.CollectionName + cconv.StringConverter.ToString(value)
}

// Generates a list of unique ids for specific collection in the bucket
// Parameters:
// - value a public unique ids.
// Retruns a unique bucket ids.
func (c *CouchbasePersistence) GenerateBucketIds(value []interface{}) []string {
	if value == nil {
		return nil
	}
	ids := make([]string, 0, 1)
	for _, v := range value {
		ids = append(ids, c.GenerateBucketId(v))
	}
	return ids
}

// GetPageByFilter method are gets a page of data items retrieved by a given filter and sorted according to sort parameters.
// This method shall be called by a public getPageByFilter method from child class that
// receives FilterParams and converts them into a filter function.
// Parameters:
// - correlationId     (optional) transaction id to trace execution through call chain.
// - filter            (optional) a filter query string after WHERE clause
// - paging            (optional) paging parameters
// - sort              (optional) sorting string after ORDER BY clause
// - sel           (optional) projection string after SELECT clause
// Returns:  page *cdata.DataPage, err error
// data page or error.
func (c *CouchbasePersistence) GetPageByFilter(correlationId string, filter string, paging *cdata.PagingParams,
	sort string, sel string) (page *cdata.DataPage, err error) {

	selectStatement := "*"
	if sel != "" {
		selectStatement = sel
	}
	statement := "SELECT " + selectStatement + " FROM `" + c.BucketName + "`"
	// Adjust max item count based on configuration
	if paging == nil {
		paging = cdata.NewEmptyPagingParams()
	}

	skip := paging.GetSkip(-1)
	take := paging.GetTake(int64(c.MaxPageSize))
	pagingEnabled := paging.Total
	collectionFilter := "_c='" + c.CollectionName + "'"

	if filter != "" {
		filter = collectionFilter + " AND " + filter
	} else {
		filter = collectionFilter
	}
	statement += " WHERE " + filter

	if sort != "" {
		statement += " ORDER BY " + sort
	}

	if skip >= 0 {
		statement += " OFFSET " + strconv.FormatInt(int64(skip), 10)
	}
	statement = statement + " LIMIT " + strconv.FormatInt(int64(take), 10)

	query := gocb.NewN1qlQuery(statement)
	// Todo: Make it configurable?
	query.Consistency(gocb.StatementPlus)
	queryResp, queryErr := c.Bucket.ExecuteN1qlQuery(query, nil)

	if queryErr != nil {
		return nil, queryErr
	}

	items := make([]interface{}, 0, 0)
	buf := make(map[string]interface{}, 0)
	for queryResp.Next(&buf) {
		var item interface{}
		if selectStatement == "*" {
			item = c.ConvertFromMap(buf[c.BucketName])
		} else {
			item = c.ConvertFromMap(buf)
		}
		items = append(items, item)
	}
	if len(items) > 0 {
		c.Logger.Trace(correlationId, "Retrieved %d from %s", len(items), c.BucketName)
	}

	if pagingEnabled {
		var total int64 = int64(len(items))
		page = cdata.NewDataPage(&total, items)
	} else {
		var total int64 = 0
		page = cdata.NewDataPage(&total, items)
	}
	return page, nil
}

// GetListByFilter method are gets a list of data items retrieved by a given filter and sorted according to sort parameters.
// This method shall be called by a public getListByFilter method from child class that
// receives FilterParams and converts them into a filter function.
// Parameters:
// - correlationId    (optional) transaction id to trace execution through call chain.
// - filter           (optional) a filter JSON object
// - paging           (optional) paging parameters
// - sort             (optional) sorting JSON object
// - select           (optional) projection JSON object
// Returns:  items []interface{}, err error
// data list or error.
func (c *CouchbasePersistence) GetListByFilter(correlationId string, filter string, sort string, sel string) (items []interface{}, err error) {

	selectStatement := "*"
	if sel != "" {
		selectStatement = sel
	}
	statement := "SELECT " + selectStatement + " FROM `" + c.BucketName + "`"
	// Adjust max item count based on configuration
	if filter != "" {
		statement += " WHERE " + filter
	}
	if sort != "" {
		statement += " ORDER BY " + sort
	}
	query := gocb.NewN1qlQuery(statement)
	// Todo: Make it configurable?
	query.Consistency(gocb.RequestPlus)
	queryResp, queryErr := c.Bucket.ExecuteN1qlQuery(query, nil)
	if queryErr != nil {
		return nil, queryErr
	}
	items = make([]interface{}, 0, 0)
	buf := make(map[string]interface{}, 0)
	for queryResp.Next(&buf) {
		var item interface{}
		if selectStatement == "*" {
			item = c.ConvertFromMap(buf[c.BucketName])
		} else {
			item = c.ConvertFromMap(buf)
		}
		items = append(items, item)
	}
	if len(items) > 0 {
		c.Logger.Trace(correlationId, "Retrieved %d from %s", len(items), c.BucketName)
	}
	return items, nil
}

// GetOneRandom method are gts a random item from items that match to a given filter.
// This method shall be called by a public getOneRandom method from child class that
// receives FilterParams and converts them into a filter function.
// Parameters:
// - correlationId     (optional) transaction id to trace execution through call chain.
// - filter            (optional) a filter JSON object
// Returns: item interface{}, err error
// a random item or error.
func (c *CouchbasePersistence) GetOneRandom(correlationId string, filter string) (item interface{}, err error) {

	statement := "SELECT COUNT(*) FROM `" + c.BucketName + "`"
	// Adjust max item count based on configuration
	if filter != "" {
		statement += " WHERE " + filter
	}

	query := gocb.NewN1qlQuery(statement)
	// Todo: Make it configurable?
	query.Consistency(gocb.RequestPlus)
	queryRes, queryErr := c.Bucket.ExecuteN1qlQuery(query, nil)

	count := queryRes.Metrics().ResultCount

	if queryErr != nil || count == 0 {
		return nil, queryErr
	}
	statement = "SELECT * FROM `" + c.BucketName + "`"
	// Adjust max item count based on configuration
	if filter != "" {
		statement += " WHERE " + filter
	}
	rand.Seed(time.Now().UnixNano())
	skip := rand.Int63n(int64(count))
	if skip < 0 {
		skip = 0
	}
	statement += " OFFSET " + strconv.FormatInt(skip, 10) + " LIMIT 1"
	query = gocb.NewN1qlQuery(statement)
	queryRes, queryErr = c.Bucket.ExecuteN1qlQuery(query, nil)
	if queryErr != nil {
		return nil, queryErr
	}
	buf := make(map[string]interface{})
	queryRes.Next(&buf)
	item = c.ConvertFromMap(buf)
	c.Logger.Trace(correlationId, "Retrieved random item from %s", c.BucketName)
	return item, nil
}

// DeleteByFilter method are deletes data items that match to a given filter.
// This method shall be called by a public deleteByFilter method from child class that
// receives FilterParams and converts them into a filter function.
// Parameters:
// - correlationId     (optional) transaction id to trace execution through call chain.
// - filter            (optional) a filter JSON object.
// Returns: error
// error or nil for success.
func (c *CouchbasePersistence) DeleteByFilter(correlationId string, filter string) (err error) {

	statement := "DELETE FROM `" + c.BucketName + "`"
	// Adjust max item count based on configuration
	if filter != "" {
		statement += " WHERE " + filter
	}

	query := gocb.NewN1qlQuery(statement)
	queryRes, queryErr := c.Bucket.ExecuteN1qlQuery(query, nil)
	if queryErr != nil {
		return queryErr
	}
	count := queryRes.Metrics().ResultCount
	c.Logger.Trace(correlationId, "Deleted %d items from %s", count, c.BucketName)
	return nil
}

// Create method are creates a data item.
// Parameters:
//  - correlation_id    (optional) transaction id to trace execution through call chain.
//  - item              an item to be created.
// Returns:  result interface{}, err error
// created item or error.
func (c *CouchbasePersistence) Create(correlationId string, item interface{}) (result interface{}, err error) {
	if item == nil {
		return nil, nil
	}
	var newItem interface{}
	newItem = cmpersist.CloneObject(item)
	// Assign unique id if not exist
	insertedItem := c.ConvertFromPublic(&newItem)
	id := cdata.IdGenerator.NextLong()
	objectId := c.GenerateBucketId(id)

	_, insErr := c.Bucket.Insert(objectId, insertedItem, 0)

	if insErr != nil {
		return nil, insErr
	}
	c.Logger.Trace(correlationId, "Created in %s with id = %s", c.BucketName, id)
	c.ConvertToPublic(&newItem)
	return c.GetPtrIfNeed(newItem), nil
}


// GetProtoPtr method are returns pointer on new prototype object for unmarshaling or decode from DB
// Returns reflect.Value
// pointer on new empty object
func (c *CouchbasePersistence) GetProtoPtr() reflect.Value {
	proto := c.Prototype
	if proto.Kind() == reflect.Ptr {
		proto = proto.Elem()
	}
	return reflect.New(proto)
}

// GetConvResult method are returns properly converted result in interface{} object from pointer in docPointer
func (c *CouchbasePersistence) GetConvResult(docPointer reflect.Value) interface{} {
	item := docPointer.Elem().Interface()
	c.ConvertToPublic(&item)
	if c.Prototype.Kind() == reflect.Ptr {
		return docPointer.Interface()
	}
	return item
}

// GetPtrIfNeed method are checks c.Prototype if need return pointer or value and returns properly results
func (c *CouchbasePersistence) GetPtrIfNeed(item interface{}) interface{} {
	if c.Prototype.Kind() == reflect.Ptr {
		newPtr := reflect.New(c.Prototype.Elem())
		newPtr.Elem().Set(reflect.ValueOf(item))
		return newPtr.Interface()
	}
	return item
}

// ConvertFromMap method are converts from map[string]interface{} to object, defined by c.Prototype
func (c *CouchbasePersistence) ConvertFromMap(buf interface{}) interface{} {
	docPointer := c.GetProtoPtr()
	jsonBuf, _ := json.Marshal(buf)
	json.Unmarshal(jsonBuf, docPointer.Interface())
	return c.GetConvResult(docPointer)
}