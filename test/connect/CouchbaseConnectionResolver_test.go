package test_connect

import (
	"testing"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cbcon "github.com/pip-services3-go/pip-services3-couchbase-go/connect"
	"github.com/stretchr/testify/assert"
)

func TestCouchbaseConnectionResolver(t *testing.T) {

	t.Run("CouchbaseConnectionResolver:Single Connection", SingleConnection)
	t.Run("CouchbaseConnectionResolver:Multiple Connections", MultipleConnections)
	t.Run("CouchbaseConnectionResolver:Connection with Credentials", ConnectionCredentials)

}
func SingleConnection(t *testing.T) {
	config := cconf.NewConfigParamsFromTuples(
		"connection.host", "localhost",
		"connection.port", "8092",
		"connection.database", "test",
	)

	resolver := cbcon.NewCouchbaseConnectionResolver()
	resolver.Configure(config)
	connection, err := resolver.Resolve("")
	assert.Nil(t, err)
	assert.NotNil(t, connection)
	assert.Equal(t, "couchbase://localhost:8092/test", connection.Uri)
	assert.Equal(t, connection.Username, "")
	assert.Equal(t, connection.Password, "")

}

func MultipleConnections(t *testing.T) {
	config := cconf.NewConfigParamsFromTuples(
		"connections.1.host", "host1",
		"connections.1.port", "8092",
		"connections.1.database", "test",
		"connections.2.host", "host2",
		"connections.2.port", "8092",
		"connections.2.database", "test",
	)

	resolver := cbcon.NewCouchbaseConnectionResolver()
	resolver.Configure(config)
	connection, err := resolver.Resolve("")
	assert.Nil(t, err)
	assert.NotNil(t, connection)
	assert.Equal(t, "couchbase://host1:8092,host2:8092/test", connection.Uri)
	assert.Equal(t, connection.Username, "")
	assert.Equal(t, connection.Password, "")

}

func ConnectionCredentials(t *testing.T) {
	config := cconf.NewConfigParamsFromTuples(
		"connection.host", "localhost",
		"connection.port", "8092",
		"connection.database", "test",
		"credential.username", "admin",
		"credential.password", "password123",
	)

	resolver := cbcon.NewCouchbaseConnectionResolver()
	resolver.Configure(config)
	connection, err := resolver.Resolve("")
	assert.Nil(t, err)
	assert.NotNil(t, connection)
	assert.Equal(t, "couchbase://localhost:8092/test", connection.Uri)
	assert.Equal(t, "admin", connection.Username)
	assert.Equal(t, "password123", connection.Password)

}
