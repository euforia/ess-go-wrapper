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

	if !ed.IndexExists() {
		if len(mappingfile) > 0 {
			log.V(9).Infof("Initializing with mapping file: %#v\n", mappingfile[0])
			return &ed, ed.initializeIndex(mappingfile[0])
		} else {
			return &ed, ed.initializeIndex("")
		}
	}
	return &ed, nil
}

func (e *EssWrapper) IndexExists() bool {
	_, err := e.conn.DoCommand("GET", "/"+e.Index, nil, nil)
	if err != nil {
		return false
	}
	return true
}

func (e *EssWrapper) Get(docType, id string) (elastigo.BaseResponse, error) {
	return e.conn.Get(e.Index, docType, id, nil)
}

func (e *EssWrapper) Delete(docType, id string) bool {
	resp, err := e.conn.Delete(e.Index, docType, id, nil)
	if err != nil {
		log.Errorf("%s\n", err)
		return false
	}
	return resp.Found
}

/*
	Get document by a given attribute and value using {query:{term:{attribute:value}}}
*/
func (e *EssWrapper) GetBy(docType, attribute, value string) (out []elastigo.Hit, err error) {
	// TODO: this may need a second pass
	var (
		rslt  elastigo.SearchResult
		query = fmt.Sprintf(`{"query":{"term":{"%s":"%s"}}}`, attribute, value)
	)

	log.V(10).Infof("GetBy query: %s\n", query)

	if rslt, err = e.conn.Search(e.Index, docType, nil, query); err != nil {
		return
	}

	log.V(10).Infof("Hits: %d; Total: %d\n", rslt.Hits.Len(), rslt.Hits.Total)
	out = rslt.Hits.Hits
	return
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

func (e *EssWrapper) applyMappingFile(mapfile string) (err error) {
	if !e.IsVersionSupported() {
		log.Warningf("Not creating mapping. ESS version not supported. Must be > 1.4.")
		return nil
	}

	if _, err := os.Stat(mapfile); err != nil {
		log.Warningf("Not creating mapping. Mapping file not found (%s): %s", mapfile, err)
		return nil
	}
	// Read mapping file to get map name
	mdb, err := ioutil.ReadFile(mapfile)
	if err != nil {
		return
	}
	var mapData map[string]interface{}
	if err = json.Unmarshal(mdb, &mapData); err != nil {
		return
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
		return
	}
	log.V(10).Infof("Mapping (%s): %s\n", mapname, mapbytes)

	if _, err = e.conn.DoCommand("PUT", fmt.Sprintf("/%s/_mapping/%s", e.Index, mapname), nil, mapbytes); err != nil {
		return
	}
	log.Infof("Updated '%s' mapping for index '%s'\n", mapname, e.Index)
	return nil
}

func (e *EssWrapper) initializeIndex(mappingFile string) error {
	resp, err := e.conn.CreateIndex(e.Index)
	if err != nil {
		return err
	}
	log.V(3).Infof("Index created: %s %s\n", e.Index, resp)

	if len(mappingFile) > 1 {
		log.V(6).Infof("Applying mapping file: %s\n", mappingFile)
		return e.applyMappingFile(mappingFile)
	}

	return nil
}
