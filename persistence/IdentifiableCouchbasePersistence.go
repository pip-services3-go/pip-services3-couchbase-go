package persistence

// /** @module persistence */
// /** @hidden */
// let _ = require('lodash');
// /** @hidden */
// let async = require('async');

// import { ConfigParams } from 'pip-services3-commons-node';
// import { PagingParams } from 'pip-services3-commons-node';
// import { DataPage } from 'pip-services3-commons-node';
// import { AnyValueMap } from 'pip-services3-commons-node';
// import { IIdentifiable } from 'pip-services3-commons-node';
// import { IdGenerator } from 'pip-services3-commons-node';

// import { IWriter } from 'pip-services3-data-node';
// import { IGetter } from 'pip-services3-data-node';
// import { ISetter } from 'pip-services3-data-node';

// import { CouchbasePersistence } from './CouchbasePersistence';
// import { runInThisContext } from 'vm';

// /**
//  * Abstract persistence component that stores data in Couchbase
//  * and implements a number of CRUD operations over data items with unique ids.
//  * The data items must implement IIdentifiable interface.
//  *
//  * In basic scenarios child classes shall only override [[getPageByFilter]],
//  * [[getListByFilter]] or [[deleteByFilter]] operations with specific filter function.
//  * All other operations can be used out of the box.
//  *
//  * In complex scenarios child classes can implement additional operations by
//  * accessing <code>this._collection</code> and <code>this._model</code> properties.
//  * ### Configuration parameters ###
//  *
//  * - bucket:                      (optional) Couchbase bucket name
//  * - collection:                  (optional) Couchbase collection name
//  * - connection(s):
//  *   - discovery_key:             (optional) a key to retrieve the connection from [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/connect.idiscovery.html IDiscovery]]
//  *   - host:                      host name or IP address
//  *   - port:                      port number (default: 27017)
//  *   - uri:                       resource URI or connection string with all parameters in it
//  * - credential(s):
//  *   - store_key:                 (optional) a key to retrieve the credentials from [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/auth.icredentialstore.html ICredentialStore]]
//  *   - username:                  (optional) user name
//  *   - password:                  (optional) user password
//  * - options:
//  *   - max_pool_size:             (optional) maximum connection pool size (default: 2)
//  *   - keep_alive:                (optional) enable connection keep alive (default: true)
//  *   - connect_timeout:           (optional) connection timeout in milliseconds (default: 5 sec)
//  *   - auto_reconnect:            (optional) enable auto reconnection (default: true)
//  *   - max_page_size:             (optional) maximum page size (default: 100)
//  *   - debug:                     (optional) enable debug output (default: false).
//  *
//  * ### References ###
//  *
//  * - <code>\*:logger:\*:\*:1.0</code>           (optional) [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/log.ilogger.html ILogger]] components to pass log messages components to pass log messages
//  * - <code>\*:discovery:\*:\*:1.0</code>        (optional) [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/connect.idiscovery.html IDiscovery]] services
//  * - <code>\*:credential-store:\*:\*:1.0</code> (optional) Credential stores to resolve credentials
//  *
//  * ### Example ###
//  *
//  *     class MyCouchbasePersistence extends CouchbasePersistence<MyData, string> {
//  *
//  *     public constructor() {
//  *         base("mybucket", "mydata", new MyDataCouchbaseSchema());
//  *     }
//  *
//  *     private composeFilter(filter: FilterParams): any {
//  *         filter = filter || new FilterParams();
//  *         let criteria = [];
//  *         let name = filter.getAsNullableString('name');
//  *         if (name != null)
//  *             criteria.push({ name: name });
//  *         return criteria.length > 0 ? { $and: criteria } : null;
//  *     }
//  *
//  *     public getPageByFilter(correlationId: string, filter: FilterParams, paging: PagingParams,
//  *         callback: (err: any, page: DataPage<MyData>) => void): void {
//  *         base.getPageByFilter(correlationId, this.composeFilter(filter), paging, null, null, callback);
//  *     }
//  *
//  *     }
//  *
//  *     let persistence = new MyCouchbasePersistence();
//  *     persistence.configure(ConfigParams.fromTuples(
//  *         "host", "localhost",
//  *         "port", 27017
//  *     ));
//  *
//  *     persitence.open("123", (err) => {
//  *         ...
//  *     });
//  *
//  *     persistence.create("123", { id: "1", name: "ABC" }, (err, item) => {
//  *         persistence.getPageByFilter(
//  *             "123",
//  *             FilterParams.fromTuples("name", "ABC"),
//  *             null,
//  *             (err, page) => {
//  *                 console.log(page.data);          // Result: { id: "1", name: "ABC" }
//  *
//  *                 persistence.deleteById("123", "1", (err, item) => {
//  *                    ...
//  *                 });
//  *             }
//  *         )
//  *     });
//  */
// export class IdentifiableCouchbasePersistence<T extends IIdentifiable<K>, K> extends CouchbasePersistence
//     implements IWriter<T, K>, IGetter<T, K>, ISetter<T>  {
//     protected _maxPageSize: number = 100;
//     protected _collectionName: string;

//     /**
//      * Creates a new instance of the persistence component.
//      *
//      * @param collection    (optional) a collection name.
//      */
//     public constructor(bucket: string, collection: string) {
//         super(bucket);

//         if (bucket == null)
//             throw new Error("Bucket name could not be null");
//         if (collection == null)
//             throw new Error("Collection name could not be null");

//         this._collectionName = collection;
//     }

//     /**
//      * Configures component by passing configuration parameters.
//      *
//      * @param config    configuration parameters to be set.
//      */
//     public configure(config: ConfigParams): void {
//         super.configure(config);

//         this._maxPageSize = config.getAsIntegerWithDefault("options.max_page_size", this._maxPageSize);
//         this._collectionName = config.getAsStringWithDefault("collection", this._collectionName);
//     }

//     /**
//      * Converts object value from internal to public format.
//      *
//      * @param value     an object in internal format to convert.
//      * @returns converted object in public format.
//      */
//     protected convertToPublic(value: any): any {
//         if (value && value.toJSON)
//             value = value.toJSON();

//         if (value)
//             delete value._c

//         return value;
//     }

//     /**
//      * Convert object value from public to internal format.
//      *
//      * @param value     an object in public format to convert.
//      * @returns converted object in internal format.
//      */
//     protected convertFromPublic(value: any): any {
//         if (value) {
//             value = _.clone(value);
//             value._c = this._collectionName;
//         }
//         return value;
//     }

//     /**
//      * Converts the given object from the public partial format.
//      *
//      * @param value     the object to convert from the public partial format.
//      * @returns the initial object.
//      */
//     protected convertFromPublicPartial(value: any): any {
//         return this.convertFromPublic(value);
//     }

//     /**
//      * Generates unique id for specific collection in the bucket
//      * @param value a public unique id.
//      * @returns a unique bucket id.
//      */
//     protected generateBucketId(value: K): string {
//         if (value == null) return null;
//         return this._collectionName + value;
//     }

//     /**
//      * Generates a list of unique ids for specific collection in the bucket
//      * @param value a public unique ids.
//      * @returns a unique bucket ids.
//      */
//     protected generateBucketIds(value: any): string[] {
//         if (value == null) return null;
//         return _.map(value, (id) => { return this.generateBucketId(id); });
//     }

//     /**
//      * Gets a page of data items retrieved by a given filter and sorted according to sort parameters.
//      *
//      * This method shall be called by a public getPageByFilter method from child class that
//      * receives FilterParams and converts them into a filter function.
//      *
//      * @param correlationId     (optional) transaction id to trace execution through call chain.
//      * @param filter            (optional) a filter query string after WHERE clause
//      * @param paging            (optional) paging parameters
//      * @param sort              (optional) sorting string after ORDER BY clause
//      * @param select            (optional) projection string after SELECT clause
//      * @param callback          callback function that receives a data page or error.
//      */
//     protected getPageByFilter(correlationId: string, filter: any, paging: PagingParams,
//         sort: any, select: any, callback: (err: any, items: DataPage<T>) => void): void {

//         select = select && !_.isEmpty(select) ? select : "*"
//         let statement = "SELECT " + select + " FROM `" + this._bucketName + "`";

//         // Adjust max item count based on configuration
//         paging = paging || new PagingParams();
//         let skip = paging.getSkip(-1);
//         let take = paging.getTake(this._maxPageSize);
//         let pagingEnabled = paging.total;

//         let collectionFilter = "_c='" + this._collectionName + "'"
//         if (filter && !_.isEmpty(filter))
//             filter = collectionFilter + " AND " + filter;
//         else filter = collectionFilter;
//         statement += " WHERE " + filter;

//         if (sort && !_.isEmpty(sort)) statement += " ORDER BY " + sort;

//         if (skip >= 0) statement += " OFFSET " + skip;
//         statement += " LIMIT " + take;

//         let query = this._query.fromString(statement);
//         // Todo: Make it configurable?
//         query.consistency(this._query.Consistency.STATEMENT_PLUS);
//         this._bucket.query(query, [], (err, items) => {
//             if (err) {
//                 callback(err, null);
//                 return;
//             }

//             if (items != null)
//                 this._logger.trace(correlationId, "Retrieved %d from %s", items.length, this._bucketName);

//             items = _.map(items, item => select == "*" ? item[this._bucketName] : item);
//             items = _.map(items, this.convertToPublic);
//             items = _.filter(items, item => item != null);

//             if (pagingEnabled) {
//                 statement = "SELECT COUNT(*) FROM `" + this._bucketName + "`";
//                 if (filter && !_.isEmpty(filter)) statement += " WHERE " + filter;

//                 query = this._query.fromString(statement);
//                 this._bucket.query(query, [], (err, counts) => {
//                     if (err) {
//                         callback(err, null);
//                         return;
//                     }

//                     let count = counts ? counts[0]['$1'] : 0;
//                     let page = new DataPage<T>(items, count);
//                     callback(null, page);
//                 });
//             } else {
//                 let page = new DataPage<T>(items);
//                 callback(null, page);
//             }
//         });
//     }

//     /**
//      * Gets a list of data items retrieved by a given filter and sorted according to sort parameters.
//      *
//      * This method shall be called by a public getListByFilter method from child class that
//      * receives FilterParams and converts them into a filter function.
//      *
//      * @param correlationId    (optional) transaction id to trace execution through call chain.
//      * @param filter           (optional) a filter JSON object
//      * @param paging           (optional) paging parameters
//      * @param sort             (optional) sorting JSON object
//      * @param select           (optional) projection JSON object
//      * @param callback         callback function that receives a data list or error.
//      */
//     protected getListByFilter(correlationId: string, filter: any, sort: any, select: any,
//         callback: (err: any, items: T[]) => void): void {

//         select = select && !_.isEmpty(select) ? select : "*"
//         let statement = "SELECT " + select + " FROM `" + this._bucketName + "`";

//         // Adjust max item count based on configuration
//         if (filter && !_.isEmpty(filter)) statement += " WHERE " + filter;
//         if (sort && !_.isEmpty(sort)) statement += " ORDER BY " + sort;

//         let query = this._query.fromString(statement);
//         // Todo: Make it configurable?
//         query.consistency(this._query.Consistency.REQUEST_PLUS);
//         this._bucket.query(query, [], (err, items) => {
//             if (err) {
//                 callback(err, null);
//                 return;
//             }

//             if (items != null)
//                 this._logger.trace(correlationId, "Retrieved %d from %s", items.length, this._bucketName);

//             items = _.map(items, item => select == "*" ? item[this._bucketName] : item);
//             items = _.map(items, this.convertToPublic);
//             items = _.filter(items, item => item != null);

//             callback(null, items);
//         });
//     }

//     /**
//      * Gets a list of data items retrieved by given unique ids.
//      *
//      * @param correlationId     (optional) transaction id to trace execution through call chain.
//      * @param ids               ids of data items to be retrieved
//      * @param callback         callback function that receives a data list or error.
//      */
//     public getListByIds(correlationId: string, ids: K[],
//         callback: (err: any, items: T[]) => void): void {

//         let objectIds = this.generateBucketIds(ids);

//         this._bucket.getMulti(objectIds, (count, items) => {
//             // Convert to array of results
//             items = _.values(items);

//             // Define the error
//             let err = null;
//             if (count > 0 && count == objectIds.length) {
//                 err = items[0].error;

//                 // Ignore "Key does not exist on the server" error
//                 if (err && err.message && err.code == 13)
//                     err = null;
//             }

//             if (err) {
//                 callback(err, null);
//                 return;
//             }

//             items = _.map(items, item => item.value);
//             items = _.filter(items, (item) => item != null);

//             if (items != null)
//                 this._logger.trace(correlationId, "Retrieved %d from %s", items.length, this._bucketName);

//             items = _.map(items, this.convertToPublic);

//             callback(null, items);
//         });
//     }

//     /**
//      * Gets a data item by its unique id.
//      *
//      * @param correlationId     (optional) transaction id to trace execution through call chain.
//      * @param id                an id of data item to be retrieved.
//      * @param callback          callback function that receives data item or error.
//      */
//     public getOneById(correlationId: string, id: K, callback: (err: any, item: T) => void): void {
//         let objectId = this.generateBucketId(id);
//         this._bucket.get(objectId, (err, result) => {
//             // Ignore "Key does not exist on the server" error
//             if (err && err.message && err.code == 13)
//                 err = null;

//             if (!err)
//                 this._logger.trace(correlationId, "Retrieved from %s by id = %s", this._bucketName, objectId);

//             let item = result ? this.convertToPublic(result.value) : null;
//             callback(err, item);
//         });
//     }

//     /**
//      * Gets a random item from items that match to a given filter.
//      *
//      * This method shall be called by a public getOneRandom method from child class that
//      * receives FilterParams and converts them into a filter function.
//      *
//      * @param correlationId     (optional) transaction id to trace execution through call chain.
//      * @param filter            (optional) a filter JSON object
//      * @param callback          callback function that receives a random item or error.
//      */
//     protected getOneRandom(correlationId: string, filter: any, callback: (err: any, item: T) => void): void {
//         let statement = "SELECT COUNT(*) FROM `" + this._bucketName + "`";

//         // Adjust max item count based on configuration
//         if (filter && !_.isEmpty(filter)) statement += " WHERE " + filter;

//         let query = this._query.fromString(statement);
//         // Todo: Make it configurable?
//         query.consistency(this._query.Consistency.REQUEST_PLUS);
//         this._bucket.query(query, [], (err, counts) => {
//             let count = counts != null ? counts[0] : 0;

//             if (err || count == 0) {
//                 callback(err, null);
//                 return;
//             }

//             let statement = "SELECT COUNT(*) FROM `" + this._bucketName + "`";

//             // Adjust max item count based on configuration
//             if (filter && !_.isEmpty(filter)) statement += " WHERE " + filter;

//             let skip = Math.trunc(Math.random() * count);
//             statement += " OFFSET " + skip + " LIMIT 1";

//             let query = this._query.fromString(statement);
//             this._bucket.query(query, [], (err, items) => {
//                 if (items != null && items.length > 0)
//                     this._logger.trace(correlationId, "Retrieved random item from %s", this._bucketName);

//                 items = _.map(items, this.convertToPublic);

//                 callback(null, items[0]);
//             });
//         });
//     }

//     /**
//      * Creates a data item.
//      *
//      * @param correlation_id    (optional) transaction id to trace execution through call chain.
//      * @param item              an item to be created.
//      * @param callback          (optional) callback function that receives created item or error.
//      */
//     public create(correlationId: string, item: T, callback?: (err: any, item: T) => void): void {
//         if (item == null) {
//             callback(null, null);
//             return;
//         }

//         // Assign unique id
//         let newItem: any = _.clone(item);
//         newItem.id = item.id || IdGenerator.nextLong();
//         let id = newItem.id.toString();
//         let objectId = this.generateBucketId(id);
//         newItem = this.convertFromPublic(newItem);

//         this._bucket.insert(objectId, newItem, (err, result) => {
//             if (!err)
//                 this._logger.trace(correlationId, "Created in %s with id = %s", this._bucketName, id);

//             newItem = err == null ? this.convertToPublic(newItem) : null;
//             callback(err, newItem);
//         });
//     }

//     /**
//      * Sets a data item. If the data item exists it updates it,
//      * otherwise it create a new data item.
//      *
//      * @param correlation_id    (optional) transaction id to trace execution through call chain.
//      * @param item              a item to be set.
//      * @param callback          (optional) callback function that receives updated item or error.
//      */
//     public set(correlationId: string, item: T, callback?: (err: any, item: T) => void): void {
//         if (item == null) {
//             if (callback) callback(null, null);
//             return;
//         }

//         // Assign unique id
//         let newItem: any = _.clone(item);
//         newItem.id = item.id || IdGenerator.nextLong();
//         let id = newItem.id.toString();
//         let objectId = this.generateBucketId(id);
//         newItem = this.convertFromPublic(newItem);

//         this._bucket.upsert(objectId, newItem, (err, result) => {
//             if (!err)
//                 this._logger.trace(correlationId, "Set in %s with id = %s", this._bucketName, id);

//             if (callback) {
//                 newItem = err == null ? this.convertToPublic(newItem) : null;
//                 callback(err, newItem);
//             }
//         });
//     }

//     /**
//      * Updates a data item.
//      *
//      * @param correlation_id    (optional) transaction id to trace execution through call chain.
//      * @param item              an item to be updated.
//      * @param callback          (optional) callback function that receives updated item or error.
//      */
//     public update(correlationId: string, item: T, callback?: (err: any, item: T) => void): void {
//         if (item == null || item.id == null) {
//             if (callback) callback(null, null);
//             return;
//         }

//         let newItem = _.clone(item);
//         newItem = this.convertFromPublic(newItem);
//         let id = newItem.id.toString();
//         let objectId = this.generateBucketId(id);

//         this._bucket.replace(objectId, newItem, (err, result) => {
//             if (!err)
//                 this._logger.trace(correlationId, "Updated in %s with id = %s", this._bucketName, id);

//             if (callback) {
//                 newItem = err == null ? this.convertToPublic(newItem) : null;
//                 callback(err, newItem);
//             }
//         });
//     }

//     /**
//      * Updates only few selected fields in a data item.
//      *
//      * @param correlation_id    (optional) transaction id to trace execution through call chain.
//      * @param id                an id of data item to be updated.
//      * @param data              a map with fields to be updated.
//      * @param callback          callback function that receives updated item or error.
//      */
//     public updatePartially(correlationId: string, id: K, data: AnyValueMap,
//         callback?: (err: any, item: T) => void): void {

//         if (data == null || id == null) {
//             if (callback) callback(null, null);
//             return;
//         }

//         let newItem = data.getAsObject();
//         newItem = this.convertFromPublicPartial(newItem);
//         let objectId = this.generateBucketId(id);

//         // Todo: repeat until update is successful
//         this._bucket.get(objectId, (err, result) => {
//             if (err || result == null || result.value == null) {
//                 callback(err, null);
//                 return;
//             }

//             let objectValue = _.assign(result.value, newItem);

//             this._bucket.replace(objectId, objectValue, { cas: result.cas }, (err, result) => {
//                 if (!err)
//                     this._logger.trace(correlationId, "Updated partially in %s with id = %s", this._bucketName, id);

//                 if (callback) {
//                     newItem = err == null ? this.convertToPublic(objectValue) : null;
//                     callback(err, newItem);
//                 }
//             });
//         });
//     }

//     /**
//      * Deleted a data item by it's unique id.
//      *
//      * @param correlation_id    (optional) transaction id to trace execution through call chain.
//      * @param id                an id of the item to be deleted
//      * @param callback          (optional) callback function that receives deleted item or error.
//      */
//     public deleteById(correlationId: string, id: K, callback?: (err: any, item: T) => void): void {
//         let objectId = this.generateBucketId(id);
//         this._bucket.get(objectId, (err, result) => {
//             if (err || result == null || result.value == null) {
//                 callback(err, null);
//                 return;
//             }

//             let oldItem = this.convertToPublic(result.value);

//             this._bucket.remove(objectId, (err, result) => {
//                 // Ignore "Key does not exist on the server" error
//                 if (err && err.message && err.code == 13)
//                     err = null;

//                 if (!err)
//                     this._logger.trace(correlationId, "Deleted from %s with id = %s", this._bucketName, id);

//                 if (callback) {
//                     oldItem = err == null ? oldItem : null;
//                     callback(err, oldItem);
//                 }
//             });
//         });
//     }

//     /**
//      * Deletes data items that match to a given filter.
//      *
//      * This method shall be called by a public deleteByFilter method from child class that
//      * receives FilterParams and converts them into a filter function.
//      *
//      * @param correlationId     (optional) transaction id to trace execution through call chain.
//      * @param filter            (optional) a filter JSON object.
//      * @param callback          (optional) callback function that receives error or null for success.
//      */
//     public deleteByFilter(correlationId: string, filter: any, callback?: (err: any) => void): void {
//         let statement = "DELETE FROM `" + this._bucketName + "`";

//         // Adjust max item count based on configuration
//         if (filter && !_.isEmpty(filter)) statement += " WHERE " + filter;

//         let query = this._query.fromString(statement);
//         this._bucket.query(query, [], (err, counts) => {
//             if (!err) {
//                 let count = counts != null ? counts[0] : 0;
//                 this._logger.trace(correlationId, "Deleted %d items from %s", count, this._bucketName);
//             }

//             callback(err);
//         });
//     }

//     /**
//      * Deletes multiple data items by their unique ids.
//      *
//      * @param correlationId     (optional) transaction id to trace execution through call chain.
//      * @param ids               ids of data items to be deleted.
//      * @param callback          (optional) callback function that receives error or null for success.
//      */
//     public deleteByIds(correlationId: string, ids: K[], callback?: (err: any) => void): void {
//         let count = 0;
//         async.each(ids, (id, callback) => {
//             let objectId = this.generateBucketId(id);

//             this._bucket.remove(objectId, (err) => {
//                 // Ignore "Key does not exist on the server" error
//                 if (err && err.message && err.code == 13)
//                     err = null;

//                 if (err == null)
//                     count++;

//                 callback(err);
//             });
//         }, (err) => {
//             this._logger.trace(correlationId, "Deleted %d items from %s", count, this._bucketName);
//             if (callback) callback(err);
//         });
//     }
// }
