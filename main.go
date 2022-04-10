package main

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
)

func main() {
	tableName := "my-table4"
	ctx := context.TODO()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: "http://localhost:8000"}, nil
			},
		)),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID: "dummy", SecretAccessKey: "dummy", SessionToken: "dummy",
				Source: "Hard-coded credentials; values are irrelevant for local DynamoDB",
			},
		}),
	)

	if err != nil {
		panic(err)
	}

	db := dynamodb.NewFromConfig(cfg)
	ok, err := existTable(ctx, db, tableName)
	if err != nil {
		panic(err)
	}
	if !ok {
		fmt.Println("create table while not exists")
		_, err := db.CreateTable(ctx, &dynamodb.CreateTableInput{
			AttributeDefinitions: []types.AttributeDefinition{
				{
					AttributeName: aws.String("id"),
					AttributeType: types.ScalarAttributeTypeS,
				},
				{
					AttributeName: aws.String("date"),
					AttributeType: types.ScalarAttributeTypeN,
				},
			},
			KeySchema: []types.KeySchemaElement{
				{
					AttributeName: aws.String("id"),
					KeyType:       types.KeyTypeHash,
				},
				{
					AttributeName: aws.String("date"),
					KeyType:       types.KeyTypeRange,
				},
			},
			TableName:   aws.String(tableName),
			BillingMode: types.BillingModePayPerRequest,
		})
		if err != nil {
			panic(err)
		}

		fmt.Println("waiting table initial")
		err = waitForTable(ctx, db, tableName)
		if err != nil {
			panic(err)
		}
	} else {
		fmt.Println("table exists")
	}

	{
		fmt.Println("put item")
		_, err = db.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &tableName,
			Item: map[string]types.AttributeValue{
				"id":    &types.AttributeValueMemberS{Value: "123"},
				"date":  &types.AttributeValueMemberN{Value: "20220410"},
				"name":  &types.AttributeValueMemberS{Value: "John"},
				"email": &types.AttributeValueMemberS{Value: "john@a.com"},
			},
		})
		if err != nil {
			panic(err)
		}
	}

	{
		out, err := db.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &tableName,
			Key: map[string]types.AttributeValue{
				"id":   &types.AttributeValueMemberS{Value: "123"},
				"date": &types.AttributeValueMemberN{Value: "20220410"},
			},
		})
		if err != nil {
			panic(err)
		}

		fmt.Printf("%v\n", out.Item["email"].(*types.AttributeValueMemberS).Value)
	}

	{
		out, err := db.Query(context.TODO(), &dynamodb.QueryInput{
			TableName:              &tableName,
			KeyConditionExpression: aws.String("id = :hashKey and #date >= :rangeKey"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":hashKey":  &types.AttributeValueMemberS{Value: "123"},
				":rangeKey": &types.AttributeValueMemberN{Value: "20220410"},
			},
			ExpressionAttributeNames: map[string]string{
				"#date": "date",
			},
		})
		if err != nil {
			panic(err)
		}
		fmt.Printf("%v\n", out.Items[0]["email"].(*types.AttributeValueMemberS).Value)
	}
}

func existTable(ctx context.Context, db *dynamodb.Client, tn string) (bool, error) {
	_, err := db.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tn),
	})

	if err != nil {
		var apiErr *types.ResourceNotFoundException
		if errors.As(err, &apiErr) {
			return false, nil
		} else {
			return false, err
		}
	}

	return true, nil
}

func waitForTable(ctx context.Context, db *dynamodb.Client, tn string) error {
	w := dynamodb.NewTableExistsWaiter(db)
	err := w.Wait(ctx,
		&dynamodb.DescribeTableInput{
			TableName: aws.String(tn),
		},
		2*time.Minute,
		func(o *dynamodb.TableExistsWaiterOptions) {
			o.MaxDelay = 5 * time.Second
			o.MinDelay = 5 * time.Second
		})
	if err != nil {
		return errors.Wrap(err, "timed out while waiting for table to become active")
	}

	return err
}
