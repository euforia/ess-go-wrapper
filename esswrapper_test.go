package esswrapper

import (
	"path/filepath"
	"testing"
)

var (
	testEssHost        = "localhost"
	testEssPort        = 9200
	testMappingFile, _ = filepath.Abs("test-mapping-file.json")
	testIndex          = "test_index"
)

func Test_NewEssWrapper_MappingFile(t *testing.T) {
	_, err := NewEssWrapper(testEssHost, testEssPort, testIndex, testMappingFile)
	if err != nil {
		t.Fatalf("%s", err)
	}
}

func Test_NewEssWrapper(t *testing.T) {
	_, err := NewEssWrapper(testEssHost, testEssPort, testIndex)
	if err != nil {
		t.Fatalf("%s", err)
	}
}

func Test_NewEssWrapper_Info(t *testing.T) {
	ess, _ := NewEssWrapper(testEssHost, testEssPort, testIndex)
	info, err := ess.Info()
	if err != nil {
		t.Fatalf("%s", err)
	}
	t.Logf("%#v\n", info)
}

func Test_NewEssWrapper_Cleanup(t *testing.T) {
	ess, _ := NewEssWrapper(testEssHost, testEssPort, testIndex, testMappingFile)
	ess.conn.DeleteIndex(ess.Index)
}
