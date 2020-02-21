package connect

type CouchbaseConnectionParams struct {
	Uri      string `json: "uri"`
	Username string `json "username"`
	Password string `json: "password"`
}
