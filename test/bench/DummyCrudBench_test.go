package test_bench

import (
	"os"
	"testing"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cbpersist "github.com/pip-services3-go/pip-services3-couchbase-go/test/persistence"
	assert "github.com/stretchr/testify/assert"
)

func BenchmarkCrudDummyCouchbase(b *testing.B) {
	var persistence *cbpersist.DummyCouchbasePersistence
	var fixture *BenchmarkDummyFixture

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

	persistence = cbpersist.NewDummyCouchbasePersistence()
	persistence.Configure(dbConfig)

	fixture = NewBenchmarkDummyFixture(persistence)

	opnErr := persistence.Open("")
	if opnErr != nil {

		assert.Nil(b, opnErr)
		return
	}
	defer persistence.Close("")
	persistence.Clear("")

	b.Run("Create Operations", fixture.TestCreateOperations)
	b.Run("Update Operations", fixture.TestUpdateOperations)
	b.Run("Update Partially Operations", fixture.TestUpdatePartiallyOperations)
	b.Run("Delete Operations", fixture.TestDeleteOperations)
}
