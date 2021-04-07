package test_persistence

import (
	"reflect"

	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"
	persist "github.com/pip-services3-go/pip-services3-couchbase-go/persistence"
	cbfixture "github.com/pip-services3-go/pip-services3-couchbase-go/test/fixture"
)

type DummyRefCouchbasePersistence struct {
	persist.IdentifiableCouchbasePersistence
}

func NewDummyRefCouchbasePersistence() *DummyRefCouchbasePersistence {

	proto := reflect.TypeOf(&cbfixture.Dummy{})
	c := &DummyRefCouchbasePersistence{}
	c.IdentifiableCouchbasePersistence = *persist.InheritIdentifiableCouchbasePersistence(c, proto, "test", "dummies")
	return c
}

func (c *DummyRefCouchbasePersistence) Create(correlationId string, item *cbfixture.Dummy) (result *cbfixture.Dummy, err error) {
	value, err := c.IdentifiableCouchbasePersistence.Create(correlationId, item)

	if value != nil {
		val, _ := value.(*cbfixture.Dummy)
		result = val
	}
	return result, err
}

func (c *DummyRefCouchbasePersistence) GetListByIds(correlationId string, ids []string) (items []*cbfixture.Dummy, err error) {
	convIds := make([]interface{}, len(ids))
	for i, v := range ids {
		convIds[i] = v
	}
	result, err := c.IdentifiableCouchbasePersistence.GetListByIds(correlationId, convIds)
	items = make([]*cbfixture.Dummy, len(result))
	for i, v := range result {
		val, _ := v.(*cbfixture.Dummy)
		items[i] = val
	}
	return items, err
}

func (c *DummyRefCouchbasePersistence) GetOneById(correlationId string, id string) (item *cbfixture.Dummy, err error) {
	result, err := c.IdentifiableCouchbasePersistence.GetOneById(correlationId, id)
	if result != nil {
		val, _ := result.(*cbfixture.Dummy)
		item = val
	}
	return item, err
}

func (c *DummyRefCouchbasePersistence) Update(correlationId string, item *cbfixture.Dummy) (result *cbfixture.Dummy, err error) {
	value, err := c.IdentifiableCouchbasePersistence.Update(correlationId, item)
	if value != nil {
		val, _ := value.(*cbfixture.Dummy)
		result = val
	}
	return result, err
}

func (c *DummyRefCouchbasePersistence) UpdatePartially(correlationId string, id string, data *cdata.AnyValueMap) (item *cbfixture.Dummy, err error) {
	result, err := c.IdentifiableCouchbasePersistence.UpdatePartially(correlationId, id, data)

	if result != nil {
		val, _ := result.(*cbfixture.Dummy)
		item = val
	}
	return item, err
}

func (c *DummyRefCouchbasePersistence) DeleteById(correlationId string, id string) (item *cbfixture.Dummy, err error) {
	result, err := c.IdentifiableCouchbasePersistence.DeleteById(correlationId, id)
	if result != nil {
		val, _ := result.(*cbfixture.Dummy)
		item = val
	}
	return item, err
}

func (c *DummyRefCouchbasePersistence) DeleteByIds(correlationId string, ids []string) (err error) {
	convIds := make([]interface{}, len(ids))
	for i, v := range ids {
		convIds[i] = v
	}
	return c.IdentifiableCouchbasePersistence.DeleteByIds(correlationId, convIds)
}

func (c *DummyRefCouchbasePersistence) GetPageByFilter(correlationId string, filter *cdata.FilterParams, paging *cdata.PagingParams) (page *cbfixture.DummyRefPage, err error) {

	if filter == nil {
		filter = cdata.NewEmptyFilterParams()
	}
	key := filter.GetAsString("key")
	filterCondition := ""
	if key != "" {
		filterCondition += "key='" + key + "'"
	}

	tempPage, err := c.IdentifiableCouchbasePersistence.GetPageByFilter(correlationId, filterCondition, paging, "'key' DESC", "")

	// Convert to DummyRefPage
	dataLen := int64(len(tempPage.Data)) // For full release tempPage and delete this by GC
	data := make([]*cbfixture.Dummy, dataLen)
	for i := range tempPage.Data {
		temp := tempPage.Data[i].(*cbfixture.Dummy)
		data[i] = temp
	}
	page = cbfixture.NewDummyRefPage(&dataLen, data)
	return page, err
}
