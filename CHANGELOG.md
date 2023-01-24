# <img src="https://uploads-ssl.webflow.com/5ea5d3315186cf5ec60c3ee4/5edf1c94ce4c859f2b188094_logo.svg" alt="Pip.Services Logo" width="200"> <br/> Couchbase components for Pip.Services in Go Changelog

## <a name="1.1.2"></a> 1.1.2 (2023-01-12) 
- Update dependencies

## <a name="1.1.1"></a> 1.1.1 (2022-01-19) 
### Bug Fixes
- Fix GetListByIds method in IdentifiableCouchbasePersistence.
## <a name="1.1.0"></a> 1.1.0 (2021-04-03) 

### Features
* Moved CouchbaseConnection to connect package
* Added ICouchbasePersistenceOverride interface to overload virtual methods

## <a name="1.0.1"></a> 1.0.1 (2020-07-12)

Initial public release

### Features

* Moved some CRUD operations from IdentifiableCouchbasePersistence to CouchbasePersistence


## <a name="1.0.0"></a> 1.0.0 (2020-03-05)

Initial public release

### Features

* **build** Factory for constructing module components
* **connect** components for creating and configuring a database connection
* **persitence** components for working with data in the database
