package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	nraws "github.com/newrelic/go-agent/v3/integrations/nrawssdk-v2"
	"github.com/newrelic/go-agent/v3/newrelic"
)

var (
	tableName string
	region    string
)

func init() {
	flag.StringVar(&tableName, "table", "", "The `name` of the DynamoDB table to list item from.")
	flag.StringVar(&region, "region", "", "The `region` of your AWS project.")
}

// Record holds info about the records returned by Scan
type Record struct {
	ID  string
	URL []string
}

func main() {
	flag.Parse()
	if len(tableName) == 0 {
		flag.PrintDefaults()
		log.Fatalf("invalid parameters, table name required")
	}
	if len(region) == 0 {
		flag.PrintDefaults()
		log.Fatalf("invalid parameters, region name required")
	}

	// Create a New Relic application. This will look for your license key in an
	// environment variable called NEW_RELIC_LICENSE_KEY. This example turns on
	// Distributed Tracing, but that's not required.
	app, err := newrelic.NewApplication(
		newrelic.ConfigFromEnvironment(),
		newrelic.ConfigAppName("Go Dynamo App"),
		newrelic.ConfigInfoLogger(os.Stdout),
		newrelic.ConfigDistributedTracerEnabled(true),
	)

	if nil != err {
		fmt.Println(err)
		os.Exit(1)
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// Not necessary for monitoring a production application with a lot of data.
	app.WaitForConnection(5 * time.Second)

	// Instrument all new AWS clients with New Relic
	nraws.AppendMiddlewares(&cfg.APIOptions, nil)
	// Using the Config value, create the DynamoDB client
	client := dynamodb.NewFromConfig(cfg)

	param := &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	}
	txn := app.StartTransaction("dynamoScan")
	// defer txn.End()
	//Add custom attribute
	txn.AddAttribute("table", tableName)

	ctx := newrelic.NewContext(context.Background(), txn)

	scan, err := client.Scan(ctx, param)

	if err != nil {
		log.Fatalf("Query API call failed: %s", err)
	}

	txn.End()
	var records []Record
	err = attributevalue.UnmarshalListOfMaps(scan.Items, &records)
	if err != nil {
		log.Fatalf("unable to unmarshal records: %v", err)
	}
	for _, record := range records {
		log.Printf("Record : %v", record)
	}

	// Wait for shut down to ensure data gets flushed
	app.Shutdown(5 * time.Second)

}
