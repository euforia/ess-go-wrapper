package esswrapper

type EssVersion struct {
	Number         string `json:"number"`
	BuildHash      string `json:"build_hash"`
	BuildTimestamp string `json:"build_timestamp"`
	BuildSnapshot  bool   `json:"build_snapshot"`
	LuceneVersion  string `json:"lucene_version"`
}

type EssInfo struct {
	Status      int64      `json:"status"`
	Name        string     `json:"name"`
	ClusterName string     `json:"cluster_name"`
	Version     EssVersion `json:"version"`
	Tagline     string     `json:"tagline"`
}

/*
   Generic wrapper to elasticsearch
*/
type EssMapping struct {
	Meta             map[string]interface{} `json:"_meta"`
	DynamicTemplates []interface{}          `json:"dynamic_templates"`
}
