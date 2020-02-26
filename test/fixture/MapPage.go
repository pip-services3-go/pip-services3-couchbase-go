package test_persistence

type MapPage struct {
	Total *int64                   `bson:"total" json:"total"`
	Data  []map[string]interface{} `bson:"data" json:"data"`
}

func NewEmptyMapPage() *MapPage {
	return &MapPage{}
}

func NewMapPage(total *int64, data []map[string]interface{}) *MapPage {
	return &MapPage{Total: total, Data: data}
}
