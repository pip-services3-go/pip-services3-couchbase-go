package test_persistence

import cdata "github.com/pip-services3-go/pip-services3-commons-go/data"

// extends IGetter<Dummy, String>, IWriter<Dummy, String>, IPartialUpdater<Dummy, String> {
type IDummyRefPersistence interface {
	GetPageByFilter(correlationId string, filter *cdata.FilterParams, paging *cdata.PagingParams) (page *DummyRefPage, err error)
	GetListByIds(correlationId string, ids []string) (items []*Dummy, err error)
	GetOneById(correlationId string, id string) (item *Dummy, err error)
	Create(correlationId string, item *Dummy) (result *Dummy, err error)
	Update(correlationId string, item *Dummy) (result *Dummy, err error)
	UpdatePartially(correlationId string, id string, data *cdata.AnyValueMap) (item *Dummy, err error)
	DeleteById(correlationId string, id string) (item *Dummy, err error)
	DeleteByIds(correlationId string, ids []string) (err error)
}
