package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/jing-zhou/kube2haproxy/app"
	"github.com/jing-zhou/kube2haproxy/app/options"

	"k8s.io/apiserver/pkg/util/flag"
	"k8s.io/apiserver/pkg/util/logs"

	"github.com/spf13/pflag"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	config := options.NewProxyServerConfig()
	config.AddFlags(pflag.CommandLine)

	flag.InitFlags()
	logs.InitLogs()
	defer logs.FlushLogs()

	s, err := app.NewProxyServerDefault(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	if err = s.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
