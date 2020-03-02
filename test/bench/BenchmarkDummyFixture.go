package test_bench

import (
	"testing"

	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"
	cbfixture "github.com/pip-services3-go/pip-services3-couchbase-go/test/fixture"
	"github.com/stretchr/testify/assert"
)

type BenchmarkDummyFixture struct {
	dummy1      cbfixture.Dummy
	dummy2      cbfixture.Dummy
	persistence cbfixture.IDummyPersistence
}

func NewBenchmarkDummyFixture(persistence cbfixture.IDummyPersistence) *BenchmarkDummyFixture {
	c := BenchmarkDummyFixture{}
	c.dummy1 = cbfixture.Dummy{Id: "", Key: "Key 1", Content: "Content 1"}
	c.dummy2 = cbfixture.Dummy{Id: "", Key: "Key 2", Content: "Content 2"}
	c.persistence = persistence
	return &c
}

func (c *BenchmarkDummyFixture) TestCreateOperations(t *testing.B) {
	var dummy1 cbfixture.Dummy
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		result, err := c.persistence.Create("", c.dummy1)
		if err != nil {
			t.Errorf("Create method error %v", err)
		}
		t.StopTimer()
		dummy1 = result
		assert.NotNil(t, dummy1)
		assert.NotNil(t, dummy1.Id)
		assert.Equal(t, c.dummy1.Key, dummy1.Key)
		assert.Equal(t, c.dummy1.Content, dummy1.Content)
		t.StartTimer()
	}

}

func (c *BenchmarkDummyFixture) TestUpdateOperations(t *testing.B) {
	var dummy1 cbfixture.Dummy

	result, err := c.persistence.Create("", c.dummy1)
	if err != nil {
		t.Errorf("Create method error %v", err)
	}

	dummy1 = result
	assert.NotNil(t, dummy1)
	assert.NotNil(t, dummy1.Id)
	assert.Equal(t, c.dummy1.Key, dummy1.Key)
	assert.Equal(t, c.dummy1.Content, dummy1.Content)

	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		// Update the dummy
		dummy1.Content = "Updated Content 1"
		result, err = c.persistence.Update("", dummy1)
		t.StopTimer()
		if err != nil {
			t.Errorf("GetPageByFilter method error %v", err)
		}
		assert.NotNil(t, result)
		assert.Equal(t, dummy1.Id, result.Id)
		assert.Equal(t, dummy1.Key, result.Key)
		assert.Equal(t, dummy1.Content, result.Content)
		t.StartTimer()
	}
}

func (c *BenchmarkDummyFixture) TestUpdatePartiallyOperations(t *testing.B) {

	var dummy1 cbfixture.Dummy

	result, err := c.persistence.Create("", c.dummy1)
	if err != nil {
		t.Errorf("Create method error %v", err)
	}

	dummy1 = result
	assert.NotNil(t, dummy1)
	assert.NotNil(t, dummy1.Id)
	assert.Equal(t, c.dummy1.Key, dummy1.Key)
	assert.Equal(t, c.dummy1.Content, dummy1.Content)

	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StopTimer()
		updateMap := cdata.NewAnyValueMapFromTuples("content", "Partially Updated Content 1")
		t.StartTimer()
		result, err = c.persistence.UpdatePartially("", dummy1.Id, updateMap)
		t.StopTimer()
		if err != nil {
			t.Errorf("UpdatePartially method error %v", err)
		}
		assert.NotNil(t, result)
		assert.Equal(t, dummy1.Id, result.Id)
		assert.Equal(t, dummy1.Key, result.Key)
		assert.Equal(t, "Partially Updated Content 1", result.Content)
		t.StartTimer()
	}
}

func (c *BenchmarkDummyFixture) TestDeleteOperations(t *testing.B) {

	var dummy1 cbfixture.Dummy

	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StopTimer()
		result, err := c.persistence.Create("", c.dummy1)
		if err != nil {
			t.Errorf("Create method error %v", err)
		}

		dummy1 = result
		assert.NotNil(t, dummy1)
		assert.NotNil(t, dummy1.Id)
		assert.Equal(t, c.dummy1.Key, dummy1.Key)
		assert.Equal(t, c.dummy1.Content, dummy1.Content)
		t.StartTimer()
		// Delete the dummy
		result, err = c.persistence.DeleteById("", dummy1.Id)
		t.StopTimer()
		if err != nil {
			t.Errorf("DeleteById method error %v", err)
		}
		assert.NotNil(t, result)
		assert.Equal(t, dummy1.Id, result.Id)
		assert.Equal(t, dummy1.Key, result.Key)
		assert.Equal(t, dummy1.Content, result.Content)
		t.StartTimer()
	}
}

// func (c *BenchmarkDummyFixture) TestBatchOperations(t *testing.B) {
// 	var dummy1 Dummy
// 	var dummy2 Dummy

// 	// Create one dummy
// 	result, err := c.persistence.Create("", c.dummy1)
// 	if err != nil {
// 		t.Errorf("Create method error %v", err)
// 	}
// 	dummy1 = result
// 	assert.NotNil(t, dummy1)
// 	assert.NotNil(t, dummy1.Id)
// 	assert.Equal(t, c.dummy1.Key, dummy1.Key)
// 	assert.Equal(t, c.dummy1.Content, dummy1.Content)

// 	// Create another dummy
// 	result, err = c.persistence.Create("", c.dummy2)
// 	if err != nil {
// 		t.Errorf("Create method error %v", err)
// 	}
// 	dummy2 = result
// 	assert.NotNil(t, dummy2)
// 	assert.NotNil(t, dummy2.Id)
// 	assert.Equal(t, c.dummy2.Key, dummy2.Key)
// 	assert.Equal(t, c.dummy2.Content, dummy2.Content)

// 	// Read batch
// 	items, err := c.persistence.GetListByIds("", []string{dummy1.Id, dummy2.Id})
// 	if err != nil {
// 		t.Errorf("GetListByIds method error %v", err)
// 	}
// 	//assert.isArray(t,items)
// 	assert.NotNil(t, items)
// 	assert.Len(t, items, 2)

// 	// Delete batch
// 	err = c.persistence.DeleteByIds("", []string{dummy1.Id, dummy2.Id})
// 	if err != nil {
// 		t.Errorf("DeleteByIds method error %v", err)
// 	}
// 	assert.Nil(t, err)

// 	// Read empty batch
// 	items, err = c.persistence.GetListByIds("", []string{dummy1.Id, dummy2.Id})
// 	if err != nil {
// 		t.Errorf("GetListByIds method error %v", err)
// 	}
// 	assert.NotNil(t, items)
// 	assert.Len(t, items, 0)

// }

// func (c *BenchmarkDummyFixture) TestPaging(t *testing.B) {

// 	// Create one dummy
// 	_, err := c.persistence.Create("", c.dummy1)
// 	assert.Nil(t, err)

// 	page, err := c.persistence.GetPageByFilter(
// 		"",
// 		cdata.NewEmptyFilterParams(),
// 		cdata.NewPagingParams(0, 100, true))

// 	assert.Nil(t, err)

// 	assert.NotNil(t, page)
// 	assert.Len(t, page.Data, 1)
// 	assert.Equal(t, *page.Total, int64(1))

// }
