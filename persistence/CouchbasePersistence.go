package persistence

// import (

// )

// /** @module persistence */
// let async = require('async');

// import { IReferenceable } from 'pip-services3-commons-node';
// import { IUnreferenceable } from 'pip-services3-commons-node';
// import { IReferences } from 'pip-services3-commons-node';
// import { IConfigurable } from 'pip-services3-commons-node';
// import { IOpenable } from 'pip-services3-commons-node';
// import { ICleanable } from 'pip-services3-commons-node';
// import { ConfigParams } from 'pip-services3-commons-node';
// import { ConnectionException } from 'pip-services3-commons-node';
// import { InvalidStateException } from 'pip-services3-commons-node';
// import { CompositeLogger } from 'pip-services3-components-node';
// import { DependencyResolver } from 'pip-services3-commons-node';

// import { CouchbaseConnectionResolver } from '../connect/CouchbaseConnectionResolver';
// import { CouchbaseConnection } from './CouchbaseConnection';

// /**
//  * Abstract persistence component that stores data in Couchbase
//  * and is based using Couchbaseose object relational mapping.
//  *
//  * This is the most basic persistence component that is only
//  * able to store data items of any type. Specific CRUD operations
//  * over the data items must be implemented in child classes by
//  * accessing <code>this._collection</code> or <code>this._model</code> properties.
//  *
//  * ### Configuration parameters ###
//  *
//  * - bucket:                      (optional) Couchbase bucket name
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
//  *   - auto_create:               (optional) automatically create missing bucket (default: false)
//  *   - auto_index:                (optional) automatically create primary index (default: false)
//  *   - flush_enabled:             (optional) bucket flush enabled (default: false)
//  *   - bucket_type:               (optional) bucket type (default: couchbase)
//  *   - ram_quota:                 (optional) RAM quota in MB (default: 100)
//  *
//  * ### References ###
//  *
//  * - <code>\*:logger:\*:\*:1.0</code>           (optional) [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/log.ilogger.html ILogger]] components to pass log messages
//  * - <code>\*:discovery:\*:\*:1.0</code>        (optional) [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/connect.idiscovery.html IDiscovery]] services
//  * - <code>\*:credential-store:\*:\*:1.0</code> (optional) Credential stores to resolve credentials
//  *
//  * ### Example ###
//  *
//  *     class MyCouchbasePersistence extends CouchbasePersistence<MyData> {
//  *
//  *       public constructor() {
//  *           base("mydata", "mycollection", new MyDataCouchbaseSchema());
//  *     }
//  *
//  *     public getByName(correlationId: string, name: string, callback: (err, item) => void): void {
//  *         let criteria = { name: name };
//  *         this._model.findOne(criteria, callback);
//  *     });
//  *
//  *     public set(correlatonId: string, item: MyData, callback: (err) => void): void {
//  *         let criteria = { name: item.name };
//  *         let options = { upsert: true, new: true };
//  *         this._model.findOneAndUpdate(criteria, item, options, callback);
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
//  *          ...
//  *     });
//  *
//  *     persistence.set("123", { name: "ABC" }, (err) => {
//  *         persistence.getByName("123", "ABC", (err, item) => {
//  *             console.log(item);                   // Result: { name: "ABC" }
//  *         });
//  *     });
//  */
// export class CouchbasePersistence implements IReferenceable, IUnreferenceable, IConfigurable, IOpenable, ICleanable {

//     private static _defaultConfig: ConfigParams = ConfigParams.fromTuples(
//         "bucket", null,
//         "dependencies.connection", "*:connection:couchbase:*:1.0",

//         // connections.*
//         // credential.*

//         "options.auto_create", false,
//         "options.auto_index", true,
//         "options.flush_enabled", true,
//         "options.bucket_type", "couchbase",
//         "options.ram_quota", 100,
//     );

//     private _config: ConfigParams;
//     private _references: IReferences;
//     private _opened: boolean;
//     private _localConnection: boolean;

//     /**
//      * The dependency resolver.
//      */
//     protected _dependencyResolver: DependencyResolver = new DependencyResolver(CouchbasePersistence._defaultConfig);
//     /**
//      * The logger.
//      */
//     protected _logger: CompositeLogger = new CompositeLogger();
//     /**
//      * The Couchbase connection component.
//      */
//     protected _connection: CouchbaseConnection;
//     /**
//      * The configuration options.
//      */
//     protected _options: ConfigParams = new ConfigParams();

//     /**
//      * The Couchbase cluster object.
//      */
//     protected _cluster: any;
//     /**
//      * The Couchbase bucket name.
//      */
//     protected _bucketName: string;
//     /**
//      * The Couchbase bucket object.
//      */
//     protected _bucket: any;
//     /**
//      * The Couchbase N1qlQuery object.
//      */
//     protected _query: any;

//     /**
//      * Creates a new instance of the persistence component.
//      *
//      * @param bucket    (optional) a bucket name.
//      */
//     public constructor(bucket?: string) {
//         this._bucketName = bucket;
//     }

//     /**
//      * Configures component by passing configuration parameters.
//      *
//      * @param config    configuration parameters to be set.
//      */
//     public configure(config: ConfigParams): void {
//         config = config.setDefaults(CouchbasePersistence._defaultConfig);
//         this._config = config;

//         this._dependencyResolver.configure(config);

//         this._bucketName = config.getAsStringWithDefault('bucket', this._bucketName);
//         this._options = this._options.override(config.getSection("options"));
//     }

//     /**
// 	 * Sets references to dependent components.
// 	 *
// 	 * @param references 	references to locate the component dependencies.
//      */
//     public setReferences(references: IReferences): void {
//         this._references = references;
//         this._logger.setReferences(references);

//         // Get connection
//         this._dependencyResolver.setReferences(references);
//         this._connection = this._dependencyResolver.getOneOptional('connection');
//         // Or create a local one
//         if (this._connection == null) {
//             this._connection = this.createConnection();
//             this._localConnection = true;
//         } else {
//             this._localConnection = false;
//         }
//     }

//     /**
// 	 * Unsets (clears) previously set references to dependent components.
//      */
//     public unsetReferences(): void {
//         this._connection = null;
//     }

//     private createConnection(): CouchbaseConnection {
//         let connection = new CouchbaseConnection(this._bucketName);

//         if (this._config)
//             connection.configure(this._config);

//         if (this._references)
//             connection.setReferences(this._references);

//         return connection;
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
//         return value;
//     }

//     /**
//      * Convert object value from public to internal format.
//      *
//      * @param value     an object in public format to convert.
//      * @returns converted object in internal format.
//      */
//     protected convertFromPublic(value: any): any {
//         return value;
//     }

//     /**
// 	 * Checks if the component is opened.
// 	 *
// 	 * @returns true if the component has been opened and false otherwise.
//      */
//     public isOpen(): boolean {
//         return this._opened;
//     }

//     /**
// 	 * Opens the component.
// 	 *
// 	 * @param correlationId 	(optional) transaction id to trace execution through call chain.
//      * @param callback 			callback function that receives error or null no errors occured.
//      */
//     public open(correlationId: string, callback?: (err: any) => void): void {
//     	if (this._opened) {
//             callback(null);
//             return;
//         }

//         if (this._connection == null) {
//             this._connection = this.createConnection();
//             this._localConnection = true;
//         }

//         let openCurl = (err) => {
//             if (err == null && this._connection == null) {
//                 err = new InvalidStateException(correlationId, 'NO_CONNECTION', 'Couchbase connection is missing');
//             }

//             if (err == null && !this._connection.isOpen()) {
//                 err = new ConnectionException(correlationId, "CONNECT_FAILED", "Couchbase connection is not opened");
//             }

//             this._opened = false;

//             if (err) {
//                 if (callback) callback(err);
//             } else {
//                 this._cluster = this._connection.getConnection();
//                 this._bucket = this._connection.getBucket();
//                 this._bucketName = this._connection.getBucketName();

//                 let couchbase = require('couchbase');
//                 this._query = couchbase.N1qlQuery;

//                 if (callback) callback(null);
//             }
//         };

//         if (this._localConnection) {
//             this._connection.open(correlationId, openCurl);
//         } else {
//             openCurl(null);
//         }
//     }

//     /**
// 	 * Closes component and frees used resources.
// 	 *
// 	 * @param correlationId 	(optional) transaction id to trace execution through call chain.
//      * @param callback 			callback function that receives error or null no errors occured.
//      */
//     public close(correlationId: string, callback?: (err: any) => void): void {
//     	if (!this._opened) {
//             callback(null);
//             return;
//         }

//         if (this._connection == null) {
//             callback(new InvalidStateException(correlationId, 'NO_CONNECTION', 'MongoDb connection is missing'));
//             return;
//         }

//         let closeCurl = (err) => {
//             this._opened = false;
//             this._cluster = null;
//             this._bucket = null;
//             this._query = null;

//             if (callback) callback(err);
//         }

//         if (this._localConnection) {
//             this._connection.close(correlationId, closeCurl);
//         } else {
//             closeCurl(null);
//         }
//     }

//     /**
// 	 * Clears component state.
// 	 *
// 	 * @param correlationId 	(optional) transaction id to trace execution through call chain.
//      * @param callback 			callback function that receives error or null no errors occured.
//      */
//     public clear(correlationId: string, callback?: (err: any) => void): void {
//         // Return error if collection is not set
//         if (this._bucketName == null) {
//             if (callback) callback(new Error('Bucket name is not defined'));
//             return;
//         }

//         this._bucket.manager().flush((err) => {
//            if (err) {
//                 err = new ConnectionException(correlationId, "FLUSH_FAILED", "Couchbase bucket flush failed")
//                     .withCause(err);
//             }

//             if (callback) callback(err);
//         });
//     }

// }
