package esswrapper

import (
	"encoding/json"
	"fmt"
	log "github.com/golang/glog"
	elastigo "github.com/mattbaird/elastigo/lib"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

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

type EssWrapper struct {
	conn  *elastigo.Conn
	Index string
}

/*
   Create the index if it does not exist.
   Optionally apply a default mapping if mapping file supplied.
*/
func NewEssWrapper(esshost string, essport int, index string, mappingfile ...string) (*EssWrapper, error) {
	ed := EssWrapper{}

	c := elastigo.NewConn()
	c.Domain = esshost
	c.Port = fmt.Sprintf("%d", essport)

	ed.conn = c
	ed.Index = index

	exists, err := c.ExistsIndex(index, "", nil)
	if err != nil {
		if err.Error() == "record not found" {
			exists = false
		} else {
			return &ed, err
		}
	}

	if !exists && len(mappingfile) > 0 && len(mappingfile[0]) > 0 {
		return &ed, ed.initializeIndex(mappingfile[0])
	}
	return &ed, nil
}

func (e *EssWrapper) IsVersionSupported() (supported bool) {
	supported = false

	info, err := e.Info()
	if err != nil {
		log.V(0).Infof("Could not get version: %s\n", err)
		return
	}

	versionStr := strings.Join(strings.Split(info.Version.Number, ".")[:2], ".")
	verNum, err := strconv.ParseFloat(versionStr, 64)
	if err != nil {
		log.V(0).Infof("Could not get version: %s\n", err)
		return
	}

	if verNum >= 1.4 {
		supported = true
	}
	return
}

func (e *EssWrapper) applyMappingFile(mapfile string) error {
	if !e.IsVersionSupported() {
		log.Warningf("Not creating mapping. ESS version not supported. Must be > 1.4.")
		return nil
	}

	if _, err := os.Stat(mapfile); err != nil {
		return fmt.Errorf("Mapping file not found %s: %s", mapfile, err)
	}

	mdb, err := ioutil.ReadFile(mapfile)
	if err != nil {
		return err
	}
	var mapData map[string]interface{}
	if err = json.Unmarshal(mdb, &mapData); err != nil {
		return err
	}
	// Get map name from first key
	var (
		normMap  = map[string]interface{}{}
		mapname  string
		mapbytes []byte
	)
	for k, _ := range mapData {
		normMap[k] = mapData[k]
		mapname = k
		break
	}

	if mapbytes, err = json.Marshal(normMap); err != nil {
		return err
	}
	log.V(10).Infof("Mapping (%s): %s\n", mapname, mapbytes)

	b, err := e.conn.DoCommand("PUT", fmt.Sprintf("/%s/_mapping/%s", e.Index, mapname), nil, mapbytes)
	if err != nil {
		return err
	}
	log.Warningf("Updated '%s' mapping for %s: %s\n", mapname, e.Index, b)
	return nil
}

func (e *EssWrapper) Info() (info EssInfo, err error) {
	var b []byte
	b, err = e.conn.DoCommand("GET", "", nil, nil)
	err = json.Unmarshal(b, &info)
	return
}

func (e *EssWrapper) initializeIndex(mappingFile string) error {
	resp, err := e.conn.CreateIndex(e.Index)
	if err != nil {
		return err
	}
	log.V(3).Infof("Index created: %s %s\n", e.Index, resp)

	return e.applyMappingFile(mappingFile)
}

func (e *EssWrapper) AddWithId(docType, id string, data interface{}) (string, error) {
	resp, err := e.conn.Index(e.Index, docType, id, nil, data)
	if err != nil {
		log.Warningf("%s\n", err)
		return "", err
	}
	if !resp.Created {
		return "", fmt.Errorf("Failed to record job: %s", resp)
	}

	return resp.Id, nil
}

/*
   Add document with auto-generated id.
*/
func (e *EssWrapper) Add(docType string, data interface{}) (string, error) {
	return e.AddWithId(docType, "", data)
}

/*
   Args:
       docType : DTYPE_CONTAINER | DTYPE_REQUEST
       id      : document id
       data    : arbitrary data
*/
func (e *EssWrapper) Update(docType, id string, data interface{}) error {
	resp, err := e.conn.Index(e.Index, docType, id, nil, data)
	if err != nil {
		log.Warningf("%s\n", err)
		return err
	}

	log.V(10).Infof("Updated: %#v\n", data)
	b, _ := json.MarshalIndent(data, "", "  ")
	log.V(10).Infof("Updated: %s\n", b)
	log.V(10).Infof("Updated: %#v\n", resp)
	return nil
}
