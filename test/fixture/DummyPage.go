package test_persistence

type DummyPage struct {
	Total *int64  `bson:"total" json:"total"`
	Data  []Dummy `bson:"data" json:"data"`
}

func NewEmptyDummyPage() *DummyPage {
	return &DummyPage{}
}

func NewDummyPage(total *int64, data []Dummy) *DummyPage {
	return &DummyPage{Total: total, Data: data}
}
