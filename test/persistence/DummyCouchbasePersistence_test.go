package test_persistence

import (
	"os"
	"testing"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cbfixture "github.com/pip-services3-go/pip-services3-couchbase-go/test/fixture"
	assert "github.com/stretchr/testify/assert"
)

func TestDummyCouchbasePersistence(t *testing.T) {
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

	//     setup((done) => {
	dbConfig := cconf.NewConfigParamsFromTuples(
		"options.auto_create", false, //true
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

	persistence = NewDummyCouchbasePersistence()
	persistence.Configure(dbConfig)

	fixture = cbfixture.NewDummyPersistenceFixture(persistence)

	opnErr := persistence.Open("")
	if opnErr != nil {

		assert.Nil(t, opnErr)
		return
	}
	persistence.Clear("")
	defer persistence.Close("")

	t.Run("Crud Operations", fixture.TestCrudOperations)
	persistence.Clear("")
	t.Run("Batch Operations", fixture.TestBatchOperations)
	persistence.Clear("")
	t.Run("Paging", fixture.TestPaging)

}
