package persistence

import (
	"encoding/json"
	"reflect"
	"sync"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	
	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"
	refl "github.com/pip-services3-go/pip-services3-commons-go/reflect"
	cmpersist "github.com/pip-services3-go/pip-services3-data-go/persistence"
	gocb "gopkg.in/couchbase/gocb.v1"
)

/*
IdentifiableCouchbasePersistence abstract persistence component that stores data in Couchbase
and implements a number of CRUD operations over data items with unique ids.
The data items must implement IIdentifiable interface.

In basic scenarios child classes shall only override GetPageByFilter,
GetListByFilter or DeleteByFilter operations with specific filter function.
All other operations can be used out of the box.

In complex scenarios child classes can implement additional operations by
accessing c.Bucket properties.

Configuration parameters:

- bucket:                      (optional) Couchbase bucket name
- collection:                  (optional) Couchbase collection name
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
  - max_pool_size:             (optional) maximum connection pool size (default: 2)
  - keep_alive:                (optional) enable connection keep alive (default: true)
  - connect_timeout:           (optional) connection timeout in milliseconds (default: 5 sec)
  - auto_reconnect:            (optional) enable auto reconnection (default: true)
  - max_page_size:             (optional) maximum page size (default: 100)
  - debug:                     (optional) enable debug output (default: false).

References:

- *:logger:*:*:1.0           (optional) ILogger components to pass log messages components to pass log messages
- *:discovery:*:*:1.0        (optional)  IDiscovery services
- *:credential-store:*:*:1.0 (optional) Credential stores to resolve credentials

 Example:

    type MyCouchbasePersistence struct {
		 *IdentifiableCouchbasePersistence
	}

    func NewMyCouchbasePersistence()*MyCouchbasePersistence {
		c := MyCouchbasePersistence{}
		c.IdentifiableCouchbasePersistence = NewIdentifiableCouchbasePersistence(reflect.TypeOf(MyData{}), "mybucket", "mycollection")
		return &c
    }

    func (c *MyCouchbasePersistence) GetPageByFilter(correlationId string, filter *cdata.FilterParams, paging *cdata.PagingParams) (page *cbfixture.MyDataPage, err error) {
		if filter == nil {
			filter = cdata.NewEmptyFilterParams()
		}
		name := filter.GetAsString("name")
		filterCondition := ""
		if name != "" {
			filterCondition += "name='" + name + "'"
		}
		tempPage, err := c.IdentifiableCouchbasePersistence.GetPageByFilter(correlationId, filterCondition, paging, "", "")
		// Convert to MyDataPage
		dataLen := int64(len(tempPage.Data)) // For full release tempPage and delete this by GC
		data := make([]cbfixture.MyData, dataLen)
		for i, v := range tempPage.Data {
			data[i] = v.(cbfixture.MyData)
		}
		page = cbfixture.NewMyDataPage(&dataLen, data)
		return page, err
	}


    persistence := NewMyCouchbasePersistence();
    persistence.Configure(ConfigParams.fromTuples(
        "host", "localhost",
        "port", 27017,
    ));

    persitence.Open("123")
        ...
	persistence.Create("123", MyData{ id: "1", name: "ABC" })
		...
    result, err:= persistence.GetPageByFilter(
            "123",
            NewFilterParamsFromTuples("name", "ABC"),
            nil)

    fmt.Println(page.data);          // Result: { id: "1", name: "ABC" }
	persistence.DeleteById("123", "1")
        ...
*/


type IdentifiableCouchbasePersistence struct {
	*CouchbasePersistence
	
}

// NewIdentifiableCouchbasePersistence method are creates a new instance of the persistence component.
// Parameters:
//	- proto reflect.Type prototype for properly convert
//	- bucket string  couchbase bucket name
//  - collection    (optional) a collection name.
func NewIdentifiableCouchbasePersistence(proto reflect.Type, bucket string, collection string) *IdentifiableCouchbasePersistence {

	if bucket == "" {
		panic("Bucket name could not be nil")
	}

	if collection == "" {
		panic("Collection name could not be nil")
	}
	c := IdentifiableCouchbasePersistence{}
	c.CouchbasePersistence = NewCouchbasePersistence(proto, bucket)
	c.MaxPageSize = 100
	c.CollectionName = collection
	return &c
}

//  Configure method are configures component by passing configuration parameters.
// Parameters:
//  - config    configuration parameters to be set.
func (c *IdentifiableCouchbasePersistence) Configure(config *cconf.ConfigParams) {
	c.CouchbasePersistence.Configure(config)

	c.MaxPageSize = config.GetAsIntegerWithDefault("options.max_page_size", c.MaxPageSize)
	c.CollectionName = config.GetAsStringWithDefault("collection", c.CollectionName)
}



// ConvertFromPublicPartial method are converts the given object from the public partial format.
//    - value     the object to convert from the public partial format.
// Retruns the initial object.
func (c *IdentifiableCouchbasePersistence) ConvertFromPublicPartial(value *interface{}) *interface{} {
	return c.ConvertFromPublic(value)
}




// GetListByIds method are gets a list of data items retrieved by given unique ids.
// Parameters:
// - correlationId     (optional) transaction id to trace execution through call chain.
// - ids               ids of data items to be retrieved
// Returns:  items []interface{}, err error
// a data list or error.
func (c *IdentifiableCouchbasePersistence) GetListByIds(correlationId string, ids []interface{}) (items []interface{}, err error) {

	if len(ids) == 0 {
		return nil, nil
	}
	objectIds := c.GenerateBucketIds(ids)
	var opItems []gocb.BulkOp
	for _, id := range objectIds {
		mapPointer := make(map[string]interface{}, 0)
		opItems = append(opItems, &gocb.GetOp{Key: id, Value: mapPointer})
	}
	doErr := c.Bucket.Do(opItems)
	if doErr != nil {
		return nil, doErr
	}
	var i int
	for i = 0; i < len(opItems); i++ {
		if opItems[i].(*gocb.GetOp).Err != nil {
			continue
		}
		buf := opItems[i].(*gocb.GetOp).Value.(map[string]interface{})
		item := c.ConvertFromMap(buf)

		if item != nil {
			items = append(items, item)
		}
	}
	return items, nil
}

// GetOneById method are gets a data item by its unique id.
// - correlationId     (optional) transaction id to trace execution through call chain.
// - id                an id of data item to be retrieved.
// Returns:  item interface{}, err error
// data item or error.
func (c *IdentifiableCouchbasePersistence) GetOneById(correlationId string, id interface{}) (item interface{}, err error) {
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



// Create method are creates a data item.
// Parameters:
//  - correlation_id    (optional) transaction id to trace execution through call chain.
//  - item              an item to be created.
// Returns:  result interface{}, err error
// created item or error.
func (c *IdentifiableCouchbasePersistence) Create(correlationId string, item interface{}) (result interface{}, err error) {
	if item == nil {
		return nil, nil
	}
	var newItem interface{}
	newItem = cmpersist.CloneObject(item)
	// Assign unique id if not exist
	cmpersist.GenerateObjectId(&newItem)
	insertedItem := c.ConvertFromPublic(&newItem)
	id := cmpersist.GetObjectId(newItem)
	objectId := c.GenerateBucketId(id)

	_, insErr := c.Bucket.Insert(objectId, insertedItem, 0)

	if insErr != nil {
		return nil, insErr
	}
	c.Logger.Trace(correlationId, "Created in %s with id = %s", c.BucketName, id)
	c.ConvertToPublic(&newItem)
	return c.GetPtrIfNeed(newItem), nil
}

// Set method are sets a data item. If the data item exists it updates it,
// otherwise it create a new data item.
// Parameters:
// - correlation_id    (optional) transaction id to trace execution through call chain.
// - item              a item to be set.
// - callback          (optional) callback function that receives updated item or error.
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

// Update method are updates a data item.
// Parameters:
//    - correlation_id    (optional) transaction id to trace execution through call chain.
//    - item              an item to be updated.
// Returns:  result interface{}, err error
// updated item or error.
func (c *IdentifiableCouchbasePersistence) Update(correlationId string, item interface{}) (result interface{}, err error) {
	var newItem interface{}
	newItem = cmpersist.CloneObject(item)
	// Assign unique id if not exist
	cmpersist.GenerateObjectId(&newItem)
	id := cmpersist.GetObjectId(newItem)
	updateItem := c.ConvertFromPublic(&newItem)
	objectId := c.GenerateBucketId(id)

	_, repErr := c.Bucket.Replace(objectId, updateItem, 0, 0)

	if repErr != nil {
		return nil, repErr
	}
	c.Logger.Trace(correlationId, "Updated in %s with id = %s", c.BucketName, id)
	c.ConvertToPublic(&newItem)
	return c.GetPtrIfNeed(newItem), nil
}

// UpdatePartially methos are updates only few selected fields in a data item.
// Parameters:
// - correlation_id    (optional) transaction id to trace execution through call chain.
// - id                an id of data item to be updated.
// - data              a map with fields to be updated.
// Returns: result interface{}, err error
// updated item or error.
func (c *IdentifiableCouchbasePersistence) UpdatePartially(correlationId string, id interface{}, data *cdata.AnyValueMap) (item interface{}, err error) {

	if data == nil || id == nil {
		return nil, nil
	}

	objectId := c.GenerateBucketId(id)
	// Get document for update
	buf := make(map[string]interface{})
	getCas, getErr := c.Bucket.Get(objectId, &buf)
	if getErr != nil {
		return nil, getErr
	}
	// Convert from map to protype object and reject "_c" field
	newItem := c.GetProtoPtr()
	jsonBuf, _ := json.Marshal(buf)
	json.Unmarshal(jsonBuf, newItem.Interface())
	// Make changes in gets document
	if c.Prototype.Kind() == reflect.Map {
		refl.ObjectWriter.SetProperties(newItem.Elem().Interface(), data.Value())
	} else {
		refl.ObjectWriter.SetProperties(newItem.Interface(), data.Value())
	}

	_, replErr := c.Bucket.Replace(objectId, newItem.Interface(), getCas, 0)

	if replErr != nil {
		return nil, replErr
	}
	c.Logger.Trace(correlationId, "Updated partially in %s with id = %s", c.BucketName, id)
	// Convert to return type
	item = c.GetConvResult(newItem)
	return item, nil
}

// DeleteById mathod are deleted a data item by its unique id.
// Parameters:
// - correlation_id    (optional) transaction id to trace execution through call chain.
// - id                an id of the item to be deleted
// Returns: item interface{}, err error
// deleted item or error.
func (c *IdentifiableCouchbasePersistence) DeleteById(correlationId string, id interface{}) (item interface{}, err error) {

	objectId := c.GenerateBucketId(id)
	buf := make(map[string]interface{})

	_, getErr := c.Bucket.Get(objectId, &buf)
	if getErr != nil || len(buf) == 0 {
		return nil, getErr
	}
	_, remErr := c.Bucket.Remove(objectId, 0)
	if remErr != nil {
		// Ignore "Key does not exist on the server" error
		if remErr == gocb.ErrKeyNotFound {
			return nil, nil
		}
		return nil, remErr
	}
	c.Logger.Trace(correlationId, "Deleted from %s with id = %s", c.BucketName, id)
	oldItem := c.ConvertFromMap(buf)
	return oldItem, nil
}

// DeleteByIds methos are deletes multiple data items by their unique ids.
//  - correlationId     (optional) transaction id to trace execution through call chain.
//  - ids               ids of data items to be deleted.
// Returns: error
// error or nil for success.
func (c *IdentifiableCouchbasePersistence) DeleteByIds(correlationId string, ids []interface{}) (err error) {
	count := 0
	var wg sync.WaitGroup
	err = nil
	for _, id := range ids {
		wg.Add(1)
		go func(i interface{}) {
			defer wg.Done()
			objectId := c.GenerateBucketId(i)
			_, remErr := c.Bucket.Remove(objectId, 0)
			// Ignore "Key does not exist on the server" error
			if remErr != nil && remErr != gocb.ErrKeyNotFound {
				err = remErr
			}
			if remErr == nil {
				count++
			}
		}(id)
	}

	wg.Wait()
	c.Logger.Trace(correlationId, "Deleted %d items from %s", count, c.BucketName)
	return err
}


