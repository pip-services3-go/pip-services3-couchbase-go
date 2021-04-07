package test_persistence

import (
	"reflect"

	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"

	persist "github.com/pip-services3-go/pip-services3-couchbase-go/persistence"
	cbfixture "github.com/pip-services3-go/pip-services3-couchbase-go/test/fixture"
)

type DummyMapCouchbasePersistence struct {
	persist.IdentifiableCouchbasePersistence
}

func NewDummyMapCouchbasePersistence() *DummyMapCouchbasePersistence {
	var t map[string]interface{}
	proto := reflect.TypeOf(t)
	c := &DummyMapCouchbasePersistence{}
	c.IdentifiableCouchbasePersistence = *persist.InheritIdentifiableCouchbasePersistence(c, proto, "test", "dummies")
	return c
}

func (c *DummyMapCouchbasePersistence) Create(correlationId string, item map[string]interface{}) (result map[string]interface{}, err error) {
	value, err := c.IdentifiableCouchbasePersistence.Create(correlationId, item)
	if value != nil {
		val, _ := value.(map[string]interface{})
		result = val
	}
	return result, err
}

func (c *DummyMapCouchbasePersistence) GetListByIds(correlationId string, ids []string) (items []map[string]interface{}, err error) {
	convIds := make([]interface{}, len(ids))
	for i, v := range ids {
		convIds[i] = v
	}
	result, err := c.IdentifiableCouchbasePersistence.GetListByIds(correlationId, convIds)
	items = make([]map[string]interface{}, len(result))
	for i, v := range result {
		val, _ := v.(map[string]interface{})
		items[i] = val
	}
	return items, err
}

func (c *DummyMapCouchbasePersistence) GetOneById(correlationId string, id string) (item map[string]interface{}, err error) {
	result, err := c.IdentifiableCouchbasePersistence.GetOneById(correlationId, id)

	if result != nil {
		val, _ := result.(map[string]interface{})
		item = val
	}
	return item, err
}

func (c *DummyMapCouchbasePersistence) Update(correlationId string, item map[string]interface{}) (result map[string]interface{}, err error) {
	value, err := c.IdentifiableCouchbasePersistence.Update(correlationId, item)

	if value != nil {
		val, _ := value.(map[string]interface{})
		result = val
	}
	return result, err
}

func (c *DummyMapCouchbasePersistence) UpdatePartially(correlationId string, id string, data *cdata.AnyValueMap) (item map[string]interface{}, err error) {
	result, err := c.IdentifiableCouchbasePersistence.UpdatePartially(correlationId, id, data)

	if result != nil {
		val, _ := result.(map[string]interface{})
		item = val
	}
	return item, err
}

func (c *DummyMapCouchbasePersistence) DeleteById(correlationId string, id string) (item map[string]interface{}, err error) {
	result, err := c.IdentifiableCouchbasePersistence.DeleteById(correlationId, id)

	if result != nil {
		val, _ := result.(map[string]interface{})
		item = val
	}
	return item, err
}

func (c *DummyMapCouchbasePersistence) DeleteByIds(correlationId string, ids []string) (err error) {
	convIds := make([]interface{}, len(ids))
	for i, v := range ids {
		convIds[i] = v
	}
	return c.IdentifiableCouchbasePersistence.DeleteByIds(correlationId, convIds)
}

func (c *DummyMapCouchbasePersistence) GetPageByFilter(correlationId string, filter *cdata.FilterParams, paging *cdata.PagingParams) (page *cbfixture.MapPage, err error) {

	if filter == nil {
		filter = cdata.NewEmptyFilterParams()
	}
	key := filter.GetAsString("key")
	filterCondition := ""
	if key != "" {
		filterCondition += "key='" + key + "'"
	}

	tempPage, err := c.IdentifiableCouchbasePersistence.GetPageByFilter(correlationId, filterCondition, paging, "'key' DESC", "")
	// Convert to DummyPage
	dataLen := int64(len(tempPage.Data)) // For full release tempPage and delete this by GC
	data := make([]map[string]interface{}, dataLen)
	for i, v := range tempPage.Data {
		data[i] = v.(map[string]interface{})
	}
	dataPage := cbfixture.NewMapPage(&dataLen, data)
	return dataPage, err
}
