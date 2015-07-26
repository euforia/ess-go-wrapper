package esswrapper

import (
	"fmt"
	elastigo "github.com/mattbaird/elastigo/lib"
)

type BaseQuery struct {
	Query interface{} `json:"query"`
}

type FilteredQuery struct {
	Filtered interface{} `json:"filtered"`
}

type BaseFilter struct {
	Filter interface{} `json:"filter"`
}

type BoolFilter struct {
	Bool interface{} `json:"bool"`
}

/*
{
    'filter': {
        'bool': {
            'must': [
                {'term': '...'},
                {'terms': ['...', '...']},
                ....
            ]
        }
    }
}
*/
type MustFilter struct {
	Must []interface{} `json:"must"`
}

func NewMustFilter(req map[string]interface{}) (fltr *BaseFilter, err error) {

	mf := MustFilter{make([]interface{}, len(req))}
	i := 0
	for k, v := range req {
		switch v.(type) {
		case []interface{}:
			val, _ := v.([]interface{})
			mf.Must[i] = map[string]interface{}{
				"terms": map[string][]interface{}{k: val},
			}
			break
		case interface{}:
			val, ok := v.(string)
			if !ok {
				err = fmt.Errorf("invalid type. must be string")
				return
			}
			mf.Must[i] = elastigo.Query().Term(k, val)
			break
		default:
			err = fmt.Errorf("invalid type: %#v", v)
			return
		}
		i++
	}
	fltr = &BaseFilter{BoolFilter{mf}}
	return
}
