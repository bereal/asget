package main

import (
	"fmt"
	"io"
	"log"
	"os"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/jessevdk/go-flags"
)

type Client struct {
}

type Args struct {
	Host       string `short:"H" long:"host" default:"127.0.0.1"`
	Port       int    `short:"p" long:"port" default:"3000"`
	Namespace  string `short:"n" long:"namespace" required:"yes"`
	Set        string `short:"s" long:"set" required:"yes"`
	Positional struct {
		Rest []string
	} `positional-args:"yes" required:"1"`
}

func parseFlags() (*Args, error) {
	var res Args
	_, err := flags.Parse(&res)
	return &res, err
}

func mustSucceed(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

func printMap(w io.Writer, val map[interface{}]interface{}) {
	io.WriteString(w, "{")
	i := 0
	for k, v := range val {
		if i != 0 {
			io.WriteString(w, ",")
		}
		i++
		printValue(w, k)
		io.WriteString(w, ":")
		printValue(w, v)
	}
	io.WriteString(w, "}")
}

func printList(w io.Writer, val []interface{}) {
	io.WriteString(w, "[")
	for i, v := range val {
		if i != 0 {
			io.WriteString(w, ",")
		}
		printValue(w, v)
	}
	io.WriteString(w, "]")
}

func printValue(w io.Writer, val interface{}) {
	switch val := val.(type) {
	case map[interface{}]interface{}:
		printMap(w, val)
	case []interface{}:
		printList(w, val)
	case int:
		fmt.Fprintf(w, "\"%d\"", val)
	case string:
		fmt.Fprintf(w, "\"%s\"", val)
	case map[string]interface{}:
		m := make(map[interface{}]interface{}, len(val))
		for k, v := range val {
			m[k] = v
		}
		printMap(w, m)
	default:
		fmt.Fprintf(w, "Unknown: %T\n", val)
	}
}

func main() {
	args, err := parseFlags()
	mustSucceed(err)

	client, err := as.NewClient(args.Host, args.Port)
	mustSucceed(err)

	key, err := as.NewKey(args.Namespace, args.Set, args.Positional.Rest[0])
	mustSucceed(err)

	record, err := client.Get(nil, key)
	mustSucceed(err)

	printValue(os.Stdout, map[string]interface{}(record.Bins))
	fmt.Println("")
}
