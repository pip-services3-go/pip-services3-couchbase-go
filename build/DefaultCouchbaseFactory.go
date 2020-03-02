package build

import (
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	cbuild "github.com/pip-services3-go/pip-services3-components-go/build"
	cbpersist "github.com/pip-services3-go/pip-services3-couchbase-go/persistence"
)

/*
Creates Couchbase components by their descriptors.
See:  Factory
See:  CouchbaseConnection
*/
type DefaultCouchbaseFactory struct {
	*cbuild.Factory
	Descriptor                    *cref.Descriptor
	CouchbaseConnectionDescriptor *cref.Descriptor
}

// NewDefaultCouchbaseFactory method are create a new instance of the factory.
func NewDefaultCouchbaseFactory() *DefaultCouchbaseFactory {
	c := DefaultCouchbaseFactory{
		Descriptor:                    cref.NewDescriptor("pip-services", "factory", "rpc", "default", "1.0"),
		CouchbaseConnectionDescriptor: cref.NewDescriptor("pip-services", "connection", "couchbase", "*", "1.0"),
		Factory:                       cbuild.NewFactory(),
	}
	c.RegisterType(c.CouchbaseConnectionDescriptor, cbpersist.NewCouchbaseConnection)
	return &c
}
