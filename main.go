package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type Brand struct {
	CommonName       string `json:"commonName"`
	CorporateName    string `json:"corporateName"`
	ExampleUPC       string `json:"exampleUPC"`
	UPCCompanyPrefix string `json:"upcCompanyPrefix"`
	GS1CompanyPrefix string `json:"gs1CompanyPrefix"`
}

func main() {

	// Initialize a session in us-west-2 that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials.
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-west-2")},
	)
	if err != nil {
		os.Exit(1)
	}

	dynamoTable := "brands"

	//Create DynamoDB client
	svc := dynamodb.New(sess)

	csvFile, _ := os.Open("./data/sampleBrands.csv")
	reader := csv.NewReader(bufio.NewReader(csvFile))

	var brands []Brand
	for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}
		brands = append(brands, Brand{
			CommonName:       strings.TrimSpace(line[0]),
			CorporateName:    strings.TrimSpace(line[1]),
			ExampleUPC:       strings.TrimSpace(line[2]),
			UPCCompanyPrefix: strings.TrimSpace(line[3]),
			GS1CompanyPrefix: strings.TrimSpace(line[4]),
		})

	}

	// Can be useful for debugging
	//brandJSON, _ := json.Marshal(brands)
	//fmt.Println(string(brandJSON))

	for _, b := range brands {
		brandDynamoMapped, err := dynamodbattribute.MarshalMap(b)

		if err != nil {
			fmt.Println("Got error marshalling map:")
			fmt.Println(err.Error())
			os.Exit(1)
		}

		input := &dynamodb.PutItemInput{
			Item:      brandDynamoMapped,
			TableName: aws.String(dynamoTable),
		}

		_, err = svc.PutItem(input)

		if err != nil {
			fmt.Println("Got error calling PutItem:")
			fmt.Println(err.Error())
			os.Exit(1)
		}

		fmt.Printf("Successfully added %v to table %v\n", b.CommonName, dynamoTable)

	}

}
