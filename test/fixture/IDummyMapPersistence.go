package test_persistence

import cdata "github.com/pip-services3-go/pip-services3-commons-go/data"

// extends IGetter<DummyMap, String>, IWriter<DummyMap, String>, IPartialUpdater<DummyMap, String> {
type IDummyMapPersistence interface {
	GetPageByFilter(correlationId string, filter *cdata.FilterParams, paging *cdata.PagingParams) (page *MapPage, err error)
	GetListByIds(correlationId string, ids []string) (items []map[string]interface{}, err error)
	GetOneById(correlationId string, id string) (item map[string]interface{}, err error)
	Create(correlationId string, item map[string]interface{}) (result map[string]interface{}, err error)
	Update(correlationId string, item map[string]interface{}) (result map[string]interface{}, err error)
	UpdatePartially(correlationId string, id string, data *cdata.AnyValueMap) (item map[string]interface{}, err error)
	DeleteById(correlationId string, id string) (item map[string]interface{}, err error)
	DeleteByIds(correlationId string, ids []string) (err error)
}
