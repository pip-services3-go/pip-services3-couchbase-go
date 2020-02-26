package test_persistence

type DummyRefPage struct {
	Total *int64   `bson:"total" json:"total"`
	Data  []*Dummy `bson:"data" json:"data"`
}

func NewEmptyDummyRefPage() *DummyRefPage {
	return &DummyRefPage{}
}

func NewDummyRefPage(total *int64, data []*Dummy) *DummyRefPage {
	return &DummyRefPage{Total: total, Data: data}
}
