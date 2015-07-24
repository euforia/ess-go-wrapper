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
	testEssWrapper, _  = NewEssWrapper(testEssHost, testEssPort, testIndex)
	testData           = map[string]string{
		"name": "test",
		"host": "test.foo.bar",
	}
	testData2 = map[string]string{
		"name": "test2",
		"host": "test.foo.bar",
	}
)

func CleanupRun() {
	testEssWrapper.conn.DeleteIndex(testEssWrapper.Index)
	testEssWrapper.Close()
}

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

	info, err := testEssWrapper.Info()
	if err != nil {
		t.Errorf("%s", err)
	} else {
		t.Logf("%#v\n", info)
	}
}

func Test_NewEssWrapper_Add(t *testing.T) {

	id, err := testEssWrapper.Add("test_type", testData)
	if err != nil {
		CleanupRun()
		t.Fatalf("%s", err)
	}
	t.Logf("%s", id)

	id, err = testEssWrapper.Add("test_type", testData2)
	if err != nil {
		CleanupRun()
		t.Fatalf("%s", err)
	}
	t.Logf("%s", id)

}

func Test_NewEssWrapper_AddWithId(t *testing.T) {

	id, err := testEssWrapper.AddWithId("test_type", "test1.id", testData)
	if err != nil {
		//	CleanupRun()
		t.Errorf("%s", err)
	} else {
		t.Logf("%s", id)
	}

	id, err = testEssWrapper.AddWithId("test_type", "test2.id", testData2)
	if err != nil {
		//	CleanupRun()
		t.Fatalf("%s", err)
	} else {
		t.Logf("%s", id)
	}

}

func Test_NewEssWrapper_GetTypes(t *testing.T) {

	types, err := testEssWrapper.GetTypes()
	if err != nil {
		t.Errorf("%s", err)
	} else {
		if len(types) < 1 {
			t.Errorf("Mismatch!")
		} else {
			t.Logf("%v", types)
		}
	}
}

func Test_NewEssWrapper_Get(t *testing.T) {
	resp, err := testEssWrapper.Get("test_type", "test1.id")
	if err != nil {
		t.Errorf("%s", err)
	} else {
		t.Logf("%v", resp)
	}

}

func Test_NewEssWrapper_GetBy(t *testing.T) {

	items, err := testEssWrapper.GetBy("test_type", "name", testData["name"])
	if err != nil {
		t.Errorf("%s", err)
	}
	if len(items) < 1 {
		t.Errorf("No items found")
	} else {
		t.Logf("%v", items)
	}

}

func Test_NewEssWrapper_Delete(t *testing.T) {
	if !testEssWrapper.Delete("test_type", "test1.id") {
		t.Errorf("Failed to delete")
	}
	CleanupRun()
}
