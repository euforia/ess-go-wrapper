Elasticsearch golang wrapper
============================

Usage
-----

```
ew, err := NewEssWrapper(host, port, index)

// OR
// Optional mapping file to apply if new index is created.
//
ew, err := NewEssWrapper(host, port, index, mappingFile)
```