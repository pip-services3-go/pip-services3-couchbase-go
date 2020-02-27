package persistence

import (
	"encoding/json"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"time"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cconv "github.com/pip-services3-go/pip-services3-commons-go/convert"
	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"
	refl "github.com/pip-services3-go/pip-services3-commons-go/reflect"
	cmpersist "github.com/pip-services3-go/pip-services3-data-go/persistence"
	gocb "gopkg.in/couchbase/gocb.v1"
)

// import { runInThisContext } from "vm";

/*
Abstract persistence component that stores data in Couchbase
and implements a number of CRUD operations over data items with unique ids.
The data items must implement IIdentifiable interface.

In basic scenarios child classes shall only override [[getPageByFilter,
[[getListByFilter or [[deleteByFilter operations with specific filter function.
All other operations can be used out of the box.

In complex scenarios child classes can implement additional operations by
accessing c._collection and c._model properties.
 Configuration parameters

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

 References

- *:logger:*:*:1.0           (optional) ILogger components to pass log messages components to pass log messages
- *:discovery:*:*:1.0        (optional)  IDiscovery services
- *:credential-store:*:*:1.0 (optional) Credential stores to resolve credentials

 Example

    class MyCouchbasePersistence extends CouchbasePersistence<MyData, string> {

    func (c*IdentifiableCouchbasePersistence) constructor() {
        base("mybucket", "mydata", new MyDataCouchbaseSchema());
    }

    private composeFilter(filter: FilterParams): any {
        filter = filter || new FilterParams();
        let criteria = [];
        let name = filter.getAsnilableString("name");
        if (name != nil)
            criteria.push({ name: name });
        return criteria.length > 0 ? { $and: criteria } : nil;
    }

    func (c*IdentifiableCouchbasePersistence) getPageByFilter(correlationId: string, filter: FilterParams, paging: PagingParams,
        callback: (err: any, page: DataPage<MyData>) => void) {
        base.getPageByFilter(correlationId, c.composeFilter(filter), paging, nil, nil, callback);
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

    persistence.create("123", { id: "1", name: "ABC" }, (err, item) => {
        persistence.getPageByFilter(
            "123",
            FilterParams.fromTuples("name", "ABC"),
            nil,
            (err, page) => {
                console.log(page.data);          // Result: { id: "1", name: "ABC" }

                persistence.deleteById("123", "1", (err, item) => {
                   ...
                });
            }
        )
    });
*/
// <T extends IIdentifiable<K>, K> extends
//  implements IWriter<T, K>, IGetter<T, K>, ISetter<T>

type IdentifiableCouchbasePersistence struct {
	*CouchbasePersistence

	MaxPageSize    int
	CollectionName string
}

/*
   Creates a new instance of the persistence component.
    *
   - collection    (optional) a collection name.
*/
func NewIdentifiableCouchbasePersistence(proto reflect.Type, bucket string, collection string) *IdentifiableCouchbasePersistence {

	if bucket == "" {
		panic("Bucket name could not be nil")
	}

	if collection == "" {
		panic("Collection name could not be nil")
	}
	icp := IdentifiableCouchbasePersistence{}
	icp.CouchbasePersistence = NewCouchbasePersistence(proto, bucket)
	icp.MaxPageSize = 100
	icp.CollectionName = collection
	return &icp
}

//  Configures component by passing configuration parameters.
//  - config    configuration parameters to be set.
func (c *IdentifiableCouchbasePersistence) Configure(config *cconf.ConfigParams) {
	c.CouchbasePersistence.Configure(config)

	c.MaxPageSize = config.GetAsIntegerWithDefault("options.max_page_size", c.MaxPageSize)
	c.CollectionName = config.GetAsStringWithDefault("collection", c.CollectionName)
}

// ConvertFromPublic method help convert object (map) from public view by replaced "Id" to "_id" field
// Parameters:
// 	- item *interface{}
// 	converted item
func (c *IdentifiableCouchbasePersistence) ConvertFromPublic(item *interface{}) *interface{} {
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

// ConvertToPublic method is convert object (map) to public view by replaced "_id" to "Id" field
// Parameters:
// 	- item *interface{}
// 	converted item
func (c *IdentifiableCouchbasePersistence) ConvertToPublic(item *interface{}) {
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

/*
   Converts the given object from the public partial format.
    *
   - value     the object to convert from the public partial format.
   Retruns the initial object.
*/
func (c *IdentifiableCouchbasePersistence) ConvertFromPublicPartial(value *interface{}) *interface{} {
	return c.ConvertFromPublic(value)
}

// Generates unique id for specific collection in the bucket
// - value a func (c*IdentifiableCouchbasePersistence) unique id.
// Retruns a unique bucket id.
func (c *IdentifiableCouchbasePersistence) generateBucketId(value interface{}) string {
	if value == nil {
		return ""
	}
	return c.CollectionName + cconv.StringConverter.ToString(value)
}

// Generates a list of unique ids for specific collection in the bucket
// - value a func (c*IdentifiableCouchbasePersistence) unique ids.
// Retruns a unique bucket ids.
func (c *IdentifiableCouchbasePersistence) generateBucketIds(value []interface{}) []string {
	if value == nil {
		return nil
	}
	ids := make([]string, 0, 1)
	for _, v := range value {
		ids = append(ids, c.generateBucketId(v))
	}

	return ids
}

// Gets a page of data items retrieved by a given filter and sorted according to sort parameters.

// This method shall be called by a public getPageByFilter method from child class that
// receives FilterParams and converts them into a filter function.

// - correlationId     (optional) transaction id to trace execution through call chain.
// - filter            (optional) a filter query string after WHERE clause
// - paging            (optional) paging parameters
// - sort              (optional) sorting string after ORDER BY clause
// - sel           (optional) projection string after SELECT clause
// - callback          callback function that receives a data page or error.
func (c *IdentifiableCouchbasePersistence) GetPageByFilter(correlationId string, filter string, paging *cdata.PagingParams,
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
		docPointer := c.GetProtoPtr()
		jsonBuf := make([]byte, 0, 0)
		if selectStatement == "*" {
			jsonBuf, _ = json.Marshal(buf[c.BucketName])
		} else {
			jsonBuf, _ = json.Marshal(buf)
		}
		json.Unmarshal(jsonBuf, docPointer.Interface())
		item := c.GetConvResult(docPointer, c.Prototype)
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

// Gets a list of data items retrieved by a given filter and sorted according to sort parameters.
// This method shall be called by a public getListByFilter method from child class that
// receives FilterParams and converts them into a filter function.
// - correlationId    (optional) transaction id to trace execution through call chain.
// - filter           (optional) a filter JSON object
// - paging           (optional) paging parameters
// - sort             (optional) sorting JSON object
// - select           (optional) projection JSON object
// - callback         callback function that receives a data list or error.
func (c *IdentifiableCouchbasePersistence) GetListByFilter(correlationId string, filter string, sort string, sel string) (items []interface{}, err error) {

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
		docPointer := c.GetProtoPtr()
		jsonBuf := make([]byte, 0, 0)
		if selectStatement == "*" {
			jsonBuf, _ = json.Marshal(buf[c.BucketName])
		} else {
			jsonBuf, _ = json.Marshal(buf)
		}
		json.Unmarshal(jsonBuf, docPointer.Interface())
		item := c.GetConvResult(docPointer, c.Prototype)
		items = append(items, item)
	}
	if len(items) > 0 {
		c.Logger.Trace(correlationId, "Retrieved %d from %s", len(items), c.BucketName)
	}
	return items, nil
}

// Gets a list of data items retrieved by given unique ids.
// - correlationId     (optional) transaction id to trace execution through call chain.
// - ids               ids of data items to be retrieved
// - callback         callback function that receives a data list or error.
func (c *IdentifiableCouchbasePersistence) GetListByIds(correlationId string, ids []interface{}) (items []interface{}, err error) {

	if len(ids) == 0 {
		return nil, nil
	}
	objectIds := c.generateBucketIds(ids)
	var opItems []gocb.BulkOp
	for _, id := range objectIds {
		docPointer := make(map[string]interface{}, 0)
		opItems = append(opItems, &gocb.GetOp{Key: id, Value: docPointer})
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
		docPointer := c.GetProtoPtr()
		jsonBuf, _ := json.Marshal(buf)
		json.Unmarshal(jsonBuf, docPointer.Interface())
		item := c.GetConvResult(docPointer, c.Prototype)
		if item != nil {
			items = append(items, item)
		}
	}
	return items, nil
}

// Gets a data item by its unique id.
// - correlationId     (optional) transaction id to trace execution through call chain.
// - id                an id of data item to be retrieved.
// - callback          callback function that receives data item or error.

func (c *IdentifiableCouchbasePersistence) GetOneById(correlationId string, id interface{}) (item interface{}, err error) {
	objectId := c.generateBucketId(id)

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

	docPointer := c.GetProtoPtr()
	jsonBuf, _ := json.Marshal(buf)
	json.Unmarshal(jsonBuf, docPointer.Interface())
	item = c.GetConvResult(docPointer, c.Prototype)

	return item, nil
}

// Gets a random item from items that match to a given filter.

// This method shall be called by a public getOneRandom method from child class that
// receives FilterParams and converts them into a filter function.

// - correlationId     (optional) transaction id to trace execution through call chain.
// - filter            (optional) a filter JSON object
// - callback          callback function that receives a random item or error.

func (c *IdentifiableCouchbasePersistence) GetOneRandom(correlationId string, filter string) (item interface{}, err error) {
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
	docPointer := c.GetProtoPtr()
	// select *
	jsonBuf, _ := json.Marshal(buf[c.BucketName])

	json.Unmarshal(jsonBuf, docPointer.Interface())
	item = c.GetConvResult(docPointer, c.Prototype)
	c.Logger.Trace(correlationId, "Retrieved random item from %s", c.BucketName)

	return item, nil
}

//  Creates a data item.
//  - correlation_id    (optional) transaction id to trace execution through call chain.
//  - item              an item to be created.
//  - callback          (optional) callback function that receives created item or error.
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

	objectId := c.generateBucketId(id)

	_, insErr := c.Bucket.Insert(objectId, insertedItem, 0)

	if insErr != nil {
		return nil, insErr
	}
	c.Logger.Trace(correlationId, "Created in %s with id = %s", c.BucketName, id)
	c.convertToPublic(&newItem)
	if c.Prototype.Kind() == reflect.Ptr {
		newPtr := reflect.New(c.Prototype.Elem())
		newPtr.Elem().Set(reflect.ValueOf(newItem))
		return newPtr.Interface(), nil
	}
	return newItem, nil
}

// Sets a data item. If the data item exists it updates it,
// otherwise it create a new data item.

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
	objectId := c.generateBucketId(id)

	_, upsertErr := c.Bucket.Upsert(objectId, setItem, 0)

	if upsertErr != nil {
		return nil, upsertErr
	}

	c.Logger.Trace(correlationId, "Set in %s with id = %s", c.BucketName, id)
	c.convertToPublic(&newItem)
	if c.Prototype.Kind() == reflect.Ptr {
		newPtr := reflect.New(c.Prototype.Elem())
		newPtr.Elem().Set(reflect.ValueOf(newItem))
		return newPtr.Interface(), nil
	}
	return newItem, nil
}

/*
   Updates a data item.
    *
   - correlation_id    (optional) transaction id to trace execution through call chain.
   - item              an item to be updated.
   - callback          (optional) callback function that receives updated item or error.
*/
func (c *IdentifiableCouchbasePersistence) Update(correlationId string, item interface{}) (result interface{}, err error) {
	var newItem interface{}
	newItem = cmpersist.CloneObject(item)
	// Assign unique id if not exist
	cmpersist.GenerateObjectId(&newItem)
	id := cmpersist.GetObjectId(newItem)
	updateItem := c.ConvertFromPublic(&newItem)
	objectId := c.generateBucketId(id)

	_, repErr := c.Bucket.Replace(objectId, updateItem, 0, 0)

	if repErr != nil {
		return nil, repErr
	}
	c.Logger.Trace(correlationId, "Updated in %s with id = %s", c.BucketName, id)
	c.convertToPublic(&newItem)
	if c.Prototype.Kind() == reflect.Ptr {
		newPtr := reflect.New(c.Prototype.Elem())
		newPtr.Elem().Set(reflect.ValueOf(newItem))
		return newPtr.Interface(), nil
	}
	return newItem, nil
}

// Updates only few selected fields in a data item.
// - correlation_id    (optional) transaction id to trace execution through call chain.
// - id                an id of data item to be updated.
// - data              a map with fields to be updated.
// - callback          callback function that receives updated item or error.

func (c *IdentifiableCouchbasePersistence) UpdatePartially(correlationId string, id interface{}, data *cdata.AnyValueMap) (item interface{}, err error) {

	if data == nil || id == nil {
		return nil, nil
	}

	objectId := c.generateBucketId(id)
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
	//
	replaceItem := c.convertFromPublic(newItem.Interface())
	_, replErr := c.Bucket.Replace(objectId, replaceItem, getCas, 0)

	if replErr != nil {
		return nil, replErr
	}
	c.Logger.Trace(correlationId, "Updated partially in %s with id = %s", c.BucketName, id)
	c.convertToPublic(newItem.Interface())
	// Convert to return type
	item = c.GetConvResult(newItem, c.Prototype)
	return item, nil
}

// Deleted a data item by it"s unique id.

// - correlation_id    (optional) transaction id to trace execution through call chain.
// - id                an id of the item to be deleted
// - callback          (optional) callback function that receives deleted item or error.
func (c *IdentifiableCouchbasePersistence) DeleteById(correlationId string, id interface{}) (item interface{}, err error) {

	objectId := c.generateBucketId(id)
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
	docPointer := c.GetProtoPtr()
	jsonBuf, _ := json.Marshal(buf)
	json.Unmarshal(jsonBuf, docPointer.Interface())
	oldItem := c.GetConvResult(docPointer, c.Prototype)
	c.convertToPublic(&oldItem)
	return oldItem, nil
}

// Deletes data items that match to a given filter.
// This method shall be called by a public deleteByFilter method from child class that
// receives FilterParams and converts them into a filter function.
// - correlationId     (optional) transaction id to trace execution through call chain.
// - filter            (optional) a filter JSON object.
// - callback          (optional) callback function that receives error or nil for success.
func (c *IdentifiableCouchbasePersistence) DeleteByFilter(correlationId string, filter string) (err error) {
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

//    Deletes multiple data items by their unique ids.
//    - correlationId     (optional) transaction id to trace execution through call chain.
//    - ids               ids of data items to be deleted.
//    - callback          (optional) callback function that receives error or nil for success.
func (c *IdentifiableCouchbasePersistence) DeleteByIds(correlationId string, ids []interface{}) (err error) {
	count := 0
	var wg sync.WaitGroup
	err = nil
	for _, id := range ids {
		wg.Add(1)
		go func(i interface{}) {
			defer wg.Done()
			objectId := c.generateBucketId(i)
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

// service function for return pointer on new prototype object for unmarshaling
func (c *IdentifiableCouchbasePersistence) GetProtoPtr() reflect.Value {
	proto := c.Prototype
	if proto.Kind() == reflect.Ptr {
		proto = proto.Elem()
	}
	return reflect.New(proto)
}

func (c *IdentifiableCouchbasePersistence) GetConvResult(docPointer reflect.Value, proto reflect.Type) interface{} {
	item := docPointer.Elem().Interface()
	c.ConvertToPublic(&item)
	if proto.Kind() == reflect.Ptr {
		return docPointer.Interface()
	}
	return item
}
