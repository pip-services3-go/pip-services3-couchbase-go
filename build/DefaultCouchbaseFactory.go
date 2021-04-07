package build

import (
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	cbuild "github.com/pip-services3-go/pip-services3-components-go/build"
	connect "github.com/pip-services3-go/pip-services3-couchbase-go/connect"
)

/*
Creates Couchbase components by their descriptors.
See:  Factory
See:  CouchbaseConnection
*/
type DefaultCouchbaseFactory struct {
	*cbuild.Factory
}

// NewDefaultCouchbaseFactory method are create a new instance of the factory.
func NewDefaultCouchbaseFactory() *DefaultCouchbaseFactory {
	c := &DefaultCouchbaseFactory{
		Factory: cbuild.NewFactory(),
	}

	couchbaseConnectionDescriptor := cref.NewDescriptor("pip-services", "connection", "couchbase", "*", "1.0")
	c.RegisterType(couchbaseConnectionDescriptor, connect.NewCouchbaseConnection)

	return c
}
