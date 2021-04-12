package test_persistence

import (
	"os"
	"testing"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	connect "github.com/pip-services3-go/pip-services3-couchbase-go/connect"
	cbfixture "github.com/pip-services3-go/pip-services3-couchbase-go/test/fixture"
	assert "github.com/stretchr/testify/assert"
)

func TestDummyCouchbaseConnection(t *testing.T) {
	var connection *connect.CouchbaseConnection
	var persistence *DummyCouchbasePersistence
	var fixture *cbfixture.DummyPersistenceFixture

	couchbaseUri := os.Getenv("COUCHBASE_URI")
	couchbaseHost := os.Getenv("COUCHBASE_HOST")
	if couchbaseHost == "" {
		couchbaseHost = "localhost"
	}
	couchbasePort := os.Getenv("COUCHBASE_PORT")
	if couchbasePort == "" {
		couchbasePort = "8091"
	}
	couchbaseUser := os.Getenv("COUCHBASE_USER")
	if couchbaseUser == "" {
		couchbaseUser = "Administrator"
	}
	couchbasePass := os.Getenv("COUCHBASE_PASS")
	if couchbasePass == "" {
		couchbasePass = "password"
	}

	if couchbaseUri == "" && couchbaseHost == "" {
		return
	}

	dbConfig := cconf.NewConfigParamsFromTuples(
		"bucket", "test",
		"options.auto_create", true,
		"options.auto_index", true,
		"connection.uri", couchbaseUri,
		"connection.host", couchbaseHost,
		"connection.port", couchbasePort,
		"connection.operation_timeout", 2,
		// "connection.durability_interval", 0.0001,
		// "connection.durabilty_timeout", 4,
		"connection.detailed_errcodes", 1,
		"credential.username", couchbaseUser,
		"credential.password", couchbasePass,
	)

	connection = connect.NewCouchbaseConnection("test")
	connection.Configure(dbConfig)

	persistence = NewDummyCouchbasePersistence()
	persistence.SetReferences(cref.NewReferencesFromTuples(
		cref.NewDescriptor("pip-services", "connection", "couchbase", "default", "1.0"), connection,
	))

	fixture = cbfixture.NewDummyPersistenceFixture(persistence)

	opnConErr := connection.Open("")
	if opnConErr != nil {
		assert.Nil(t, opnConErr)
		return
	}
	defer persistence.Close("")
	persOpnErr := persistence.Open("")
	if persOpnErr != nil {
		assert.Nil(t, persOpnErr)
		return
	}
	defer connection.Close("")
	opnConErr = persistence.Clear("")
	if opnConErr != nil {
		assert.Nil(t, opnConErr)
		return
	}

	t.Run("Crud Operations", fixture.TestCrudOperations)
	persistence.Clear("")
	t.Run("Batch Operations", fixture.TestBatchOperations)
	persistence.Clear("")
	t.Run("Paging", fixture.TestPaging)
}
