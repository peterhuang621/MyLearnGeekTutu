package gee

import (
	"fmt"
	"reflect"
	"testing"
)

func buildTestRouter() *router {
	r := newRouter()
	r.addRoute("GET", "/", nil)
	r.addRoute("GET", "/hello/:name", nil)
	r.addRoute("GET", "/hello/b/c", nil)
	r.addRoute("GET", "/hi/:name", nil)
	r.addRoute("GET", "/assets/*filepath", nil)
	return r
}

func Test_newRouter(t *testing.T) {
	buildTestRouter()
}

func Test_parsePattern(t *testing.T) {
	ok := reflect.DeepEqual(parsePattern("/p/:name"), []string{"p", ":name"})
	ok = ok && reflect.DeepEqual(parsePattern("/p/*"), []string{"p", "*"})
	if !ok {
		t.Fatal("test parsePattern failed")
	}
}

func Benchmark_parsePattern(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Test_parsePattern(&testing.T{})
	}
}

func Test_router_getRoute(t *testing.T) {
	r := buildTestRouter()
	n, ps := r.getRoute("GET", "/hello/geektutu")

	if n == nil {
		t.Fatal("nil shouldn't be returned")
	}

	if n.pattern != "/hello/:name" {
		t.Fatal("should match /hello/:name")
	}

	if ps["name"] != "geektutu" {
		t.Fatal("name should be equal to 'geektutu'")
	}
	fmt.Printf("matched path: %s, params['name']: %s\n", n.pattern, ps["name"])
}
