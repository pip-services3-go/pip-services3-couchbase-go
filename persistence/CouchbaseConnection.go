package persistence

// import (

// )

// /** @module persistence */
// let async = require('async');

// import { IReferenceable } from 'pip-services3-commons-node';
// import { IReferences } from 'pip-services3-commons-node';
// import { IConfigurable } from 'pip-services3-commons-node';
// import { IOpenable } from 'pip-services3-commons-node';
// import { ConfigParams } from 'pip-services3-commons-node';
// import { ConnectionException } from 'pip-services3-commons-node';
// import { CompositeLogger } from 'pip-services3-components-node';

// import { CouchbaseConnectionResolver } from '../connect/CouchbaseConnectionResolver';

// /**
//  * Couchbase connection using plain couchbase driver.
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
//  */
// export class CouchbaseConnection implements IReferenceable, IConfigurable, IOpenable {

//     private _defaultConfig: ConfigParams = ConfigParams.fromTuples(
//         "bucket", null,

//         // connections.*
//         // credential.*

//         "options.auto_create", false,
//         "options.auto_index", true,
//         "options.flush_enabled", true,
//         "options.bucket_type", "couchbase",
//         "options.ram_quota", 100,
//     );

//     /**
//      * The logger.
//      */
//     protected _logger: CompositeLogger = new CompositeLogger();
//     /**
//      * The connection resolver.
//      */
//     protected _connectionResolver: CouchbaseConnectionResolver = new CouchbaseConnectionResolver();
//     /**
//      * The configuration options.
//      */
//     protected _options: ConfigParams = new ConfigParams();

//     /**
//      * The Couchbase cluster connection object.
//      */
//     protected _connection: any;
//     /**
//      * The Couchbase bucket name.
//      */
//     protected _bucketName: string;
//     /**
//      * The Couchbase bucket object.
//      */
//     protected _bucket: any;

//     /**
//      * Creates a new instance of the connection component.
//      *
//      * @param bucketName the name of couchbase bucket
//      */
//     public constructor(bucketName?: string) {
//         this._bucketName = bucketName;
//     }

//     /**
//      * Configures component by passing configuration parameters.
//      *
//      * @param config    configuration parameters to be set.
//      */
//     public configure(config: ConfigParams): void {
//         config = config.setDefaults(this._defaultConfig);

//         this._connectionResolver.configure(config);

//         this._bucketName = config.getAsStringWithDefault('bucket', this._bucketName);
//         this._options = this._options.override(config.getSection("options"));
//     }

//     /**
// 	 * Sets references to dependent components.
// 	 *
// 	 * @param references 	references to locate the component dependencies.
//      */
//     public setReferences(references: IReferences): void {
//         this._logger.setReferences(references);
//         this._connectionResolver.setReferences(references);
//     }

//     /**
// 	 * Checks if the component is opened.
// 	 *
// 	 * @returns true if the component has been opened and false otherwise.
//      */
//     public isOpen(): boolean {
//         // return this._connection.readyState == 1;
//         return this._connection != null;
//     }

//     /**
// 	 * Opens the component.
// 	 *
// 	 * @param correlationId 	(optional) transaction id to trace execution through call chain.
//      * @param callback 			callback function that receives error or null no errors occured.
//      */
//     public open(correlationId: string, callback?: (err: any) => void): void {
//         this._connectionResolver.resolve(correlationId, (err, connection) => {
//             if (err) {
//                 if (callback) callback(err);
//                 else this._logger.error(correlationId, err, 'Failed to resolve Couchbase connection');
//                 return;
//             }

//             this._logger.debug(correlationId, "Connecting to couchbase");

//             let couchbase = require('couchbase');
//             this._connection = new couchbase.Cluster(connection.uri);
//             if (connection.username)
//                 this._connection.authenticate(connection.username, connection.password);

//             let newBucket = false;

//             async.series([
//                 (callback) => {
//                     let autocreate = this._options.getAsBoolean('auto_create');
//                     if (!autocreate) {
//                         callback();
//                         return;
//                     }

//                     let options = {
//                         bucketType: this._options.getAsStringWithDefault('bucket_type', 'couchbase'),
//                         ramQuotaMB: this._options.getAsLongWithDefault('ram_quota', 100),
//                         flushEnabled: this._options.getAsBooleanWithDefault('flush_enabled', true) ? 1 : 0
//                     };

//                     this._connection.manager().createBucket(this._bucketName, options, (err, result) => {
//                         newBucket = err == null;

//                         if (err && err.message && err.message.indexOf('name already exist') > 0) {
//                             callback();
//                             return;
//                         }

//                         // Delay to allow couchbase to initialize the bucket
//                         // Otherwise opening will fail
//                         if (err == null)
//                             setTimeout(() => { callback(err); }, 2000);
//                         else callback(err);
//                     });
//                 },
//                 (callback) => {
//                     this._bucket = this._connection.openBucket(this._bucketName, (err) => {
//                         if (err) {
//                             this._logger.error(correlationId, err, "Failed to open bucket");
//                             err = new ConnectionException(correlationId, "CONNECT_FAILED", "Connection to couchbase failed").withCause(err);
//                             this._bucket = null;
//                         } else {
//                             this._logger.debug(correlationId, "Connected to couchbase bucket %s", this._bucketName);
//                         }

//                         callback(err);
//                     });
//                 },
//                 (callback) => {
//                     let autoIndex = this._options.getAsBoolean('auto_index');
//                     if (!newBucket && !autoIndex) {
//                         callback();
//                         return;
//                     }

//                     this._bucket.manager().createPrimaryIndex({ ignoreIfExists: 1}, (err) => {
//                         callback(err);
//                     });
//                 }
//             ], (err) => {
//                 if (err) {
//                     this._connection = null;
//                     this._bucket = null;
//                 }

//                 callback(err);
//             });
//         });
//     }

//     /**
// 	 * Closes component and frees used resources.
// 	 *
// 	 * @param correlationId 	(optional) transaction id to trace execution through call chain.
//      * @param callback 			callback function that receives error or null no errors occured.
//      */
//     public close(correlationId: string, callback?: (err: any) => void): void {
//         if (this._bucket)
//             this._bucket.disconnect();

//         this._connection = null;
//         this._bucket = null;

//         this._logger.debug(correlationId, "Disconnected from couchbase bucket %s", this._bucketName);

//         callback(null);
//     }

//     public getConnection(): any {
//         return this._connection;
//     }

//     public getBucket(): any {
//         return this._bucket;
//     }

//     public getBucketName(): string {
//         return this._bucketName;
//     }

// }
