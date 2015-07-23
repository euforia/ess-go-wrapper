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

type EssWrapper struct {
	conn  *elastigo.Conn
	Index string
}

/*
   Create the index if it does not exist.
   Optionally apply a mapping if mapping file is supplied.
*/
func NewEssWrapper(esshost string, essport int, index string, mappingfile ...string) (*EssWrapper, error) {

	ed := EssWrapper{conn: elastigo.NewConn(), Index: index}
	ed.conn.Domain = esshost
	ed.conn.Port = fmt.Sprintf("%d", essport)

	exists, err := ed.conn.ExistsIndex(index, "", nil)
	if err != nil {
		if err.Error() == "record not found" {
			exists = false
		} else {
			return &ed, err
		}
	}

	if !exists {
		if len(mappingfile) > 1 {
			return &ed, ed.initializeIndex(mappingfile[0])
		} else {
			return &ed, ed.initializeIndex("")
		}
	}
	return &ed, nil
}

func (e *EssWrapper) GetTypes() (types []string, err error) {
	var (
		b []byte
	)

	if b, err = e.conn.DoCommand("GET", "/"+e.Index+"/_mapping", nil, nil); err != nil {
		return
	}

	m := map[string]map[string]map[string]interface{}{}
	if err = json.Unmarshal(b, &m); err != nil {
		return
	}

	types = make([]string, len(m[e.Index]["mappings"]))
	i := 0
	for k, _ := range m[e.Index]["mappings"] {
		types[i] = k
		i++
	}

	return
}

/* Used to determine if the mapping file can be applied with the given version */
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

/* Elasticsearch instance information.  e.g. version */
func (e *EssWrapper) Info() (info EssInfo, err error) {
	var b []byte
	b, err = e.conn.DoCommand("GET", "", nil, nil)
	err = json.Unmarshal(b, &info)
	return
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

func (e *EssWrapper) Close() {
	e.conn.Close()
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

func (e *EssWrapper) applyMappingFile(mapfile string) error {
	if !e.IsVersionSupported() {
		log.Warningf("Not creating mapping. ESS version not supported. Must be > 1.4.")
		return nil
	}

	if _, err := os.Stat(mapfile); err != nil {
		log.Warningf("Not creating mapping. Mapping file not found (%s): %s", mapfile, err)
		return nil
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

func (e *EssWrapper) initializeIndex(mappingFile string) error {
	resp, err := e.conn.CreateIndex(e.Index)
	if err != nil {
		return err
	}
	log.V(3).Infof("Index created: %s %s\n", e.Index, resp)

	if len(mappingFile) > 1 {
		return e.applyMappingFile(mappingFile)
	}

	return nil
}
