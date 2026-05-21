package main

import (
	"flag"
	"fmt"
	"os"

	"kafka-json-loader/internal/loader"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var configPath string
	flag.StringVar(&configPath, "config", "", "Path to the JSON payload file.")
	flag.StringVar(&configPath, "c", "", "Path to the JSON payload file.")
	flag.Parse()

	if configPath == "" {
		return fmt.Errorf("missing required -config argument")
	}

	request, err := loader.LoadRequest(configPath)
	if err != nil {
		return err
	}

	count, err := loader.Send(request)
	if err != nil {
		return err
	}

	fmt.Printf("sent %d message(s) to topic %q\n", count, request.Topic)
	return nil
}
