package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type Config struct {
	QueueName   string `json:"queueName"`
	LogFileName string `json: "logFileName"`
}

// GetQueueURL gets the URL of an Amazon SQS queue
// Inputs:
//
//	sess is the current session, which provides configuration for the SDK's service clients
//	queueName is the name of the queue
//
// Output:
//
//	If success, the URL of the queue and nil
//	Otherwise, an empty string and an error
func GetQueueURL(sess *session.Session, queue *string) (*sqs.GetQueueUrlOutput, error) {

	svc := sqs.New(sess)

	urlResult, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: queue,
	})

	if err != nil {
		return nil, err
	}

	return urlResult, nil
}

// GetMessages gets the messages from an Amazon SQS queue
// Inputs:
//
//	sess is the current session, which provides configuration for the SDK's service clients
//	queueURL is the URL of the queue
//	timeout is how long, in seconds, the message is unavailable to other consumers
//
// Output:
//
//	If success, the latest message and nil
//	Otherwise, nil and an error from the call to ReceiveMessage
func GetMessages(sess *session.Session, queueURL *string) (*sqs.ReceiveMessageOutput, error) {
	// Create an SQS service client
	svc := sqs.New(sess)
	receiveMessageInput := &sqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: aws.Int64(10), // Adjust the number of messages to receive as needed
		VisibilityTimeout:   aws.Int64(60), // Adjust the visibility timeout as needed
		WaitTimeSeconds:     aws.Int64(5),  // Adjust the wait time as needed
	}

	result, err := svc.ReceiveMessage(receiveMessageInput)
	if err != nil {
		fmt.Println("Failed to receive messages from SQS:", err)
		return nil, err
	}

	return result, nil
}

func main() {

	data := make(map[string]string) // Data structure to store key-value pairs
	var order []string              // Slice to maintain the order of keys
	var mutex sync.Mutex            // Mutex for synchronizing access to data and order

	configFile, err := os.Open("../config.json")
	// if we os.Open returns an error then handle it
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully Opened config.json")
	// defer the closing of our jsonFile so that we can parse it later on
	defer configFile.Close()

	// read our opened jsonFile as a byte array.
	byteValue, _ := ioutil.ReadAll(configFile)

	var config Config

	// we unmarshal our byteArray which contains our
	// jsonFile's content into 'config' which we defined above
	json.Unmarshal(byteValue, &config)
	fmt.Println(config)

	logFile, err := os.OpenFile(config.LogFileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal("Failed to open log file:", err)
	}
	defer logFile.Close()

	// Set the log output to the log file
	log.SetOutput(logFile)

	// Create a session that gets credential values from ~/.aws/credentials
	// and the default region from ~/.aws/config
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	// Get URL of queue
	urlResult, err := GetQueueURL(sess, &config.QueueName)
	if err != nil {
		fmt.Println("Got an error getting the queue URL:")
		fmt.Println(err)
		return
	}

	queueURL := urlResult.QueueUrl
	messageChannel := make(chan *sqs.Message, 1) // Buffered channel with capacity 1

	go func() {
		for {
			// Receive messages from SQS
			msgResult, err := GetMessages(sess, queueURL)
			if err != nil {
				fmt.Println("Got an error receiving messages:")
				fmt.Println(err)
				return
			}

			for _, message := range msgResult.Messages {
				messageChannel <- message
			}
		}
	}()

	for message := range messageChannel {
		go processMessage(message, sess, queueURL, &data, &order, &mutex)
		time.Sleep(1 * time.Second) // Introduce a delay between consecutive message processing
	}
}

func processMessage(message *sqs.Message, sess *session.Session, queueURL *string, data *map[string]string, order *[]string, mutex *sync.Mutex) {

	// Process the message
	action, key, value, err := parseMessageBody(*message.Body)
	if err != nil {
		fmt.Println("Failed to parse message body:", err)
		deleteMessage(message, sess, queueURL)
		return
	}

	mutex.Lock() // Acquire lock before modifying data and order
	// Perform actions based on the extracted values
	switch action {
	case "AddItem":
		// Process the AddItem action with key and value
		fmt.Println("AddItem - Key:", key, "Value:", value)
		// Check if the key already exists
		if _, exists := (*data)[key]; exists {
			// Key already exists, overwrite the value
			oldValue := (*data)[key]
			log.Printf("AddItem - Key: %s already exists. Overwriting value. Old value: %s, New value: %s\n", key, oldValue, value)
		} else {
			// Key doesn't exist, add the key-value pair
			*order = append(*order, key)
			// Log the output to the file with timestamp
			log.Printf("AddItem - Key: %s, Value: %s\n", key, value)
		}
		(*data)[key] = value
	case "DeleteItem":
		// Process the DeleteItem action with key
		fmt.Println("DeleteItem - Key:", key)
		if _, exists := (*data)[key]; !exists {
			// Key does not exist, log the error
			log.Printf("DeleteItem - Key does not exist: %s\n", key)
			break
		}
		delete(*data, key)
		// Delete the key from the order slice
		for i, k := range *order {
			if k == key {
				*order = append((*order)[:i], (*order)[i+1:]...)
				break
			}
		}
		log.Printf("DeleteItem - Key: %s\n", key)
	case "GetItem":
		// Process the GetItem action with key
		fmt.Println("GetItem - Key:", key)

		value, exists := (*data)[key]
		if !exists {
			// Key does not exist, log the error
			log.Printf("GetItem - Key does not exist: %s\n", key)
			break
		}
		log.Printf("GetItem - Key: %s, Value: %s\n", key, value)
	case "GetAllItems":
		// Process the GetAllItems action
		fmt.Println("GetAllItems")
		log.Printf("GetAllItems ")
		for _, key := range *order {
			value := (*data)[key]
			log.Printf("Key: %s, Value: %s ", key, value)
		}
		log.Printf("\n")
	default:
		fmt.Println("Unknown action:", action)
	}

	mutex.Unlock() // Release lock after modifying data and order
	deleteMessage(message, sess, queueURL)

}

// function to delete the message from the queue
func deleteMessage(message *sqs.Message, sess *session.Session, queueURL *string) {
	svc := sqs.New(sess)
	// Delete the message from the queue after processing
	deleteMessageInput := &sqs.DeleteMessageInput{
		QueueUrl:      queueURL,
		ReceiptHandle: message.ReceiptHandle,
	}
	_, err := svc.DeleteMessage(deleteMessageInput)
	if err != nil {
		fmt.Println("Failed to delete message from SQS:", err)
	}
}

func parseMessageBody(body string) (action, key, value string, err error) {
	// Parse the message body and extract the action, key, and value
	// Implement your parsing logic here based on the message body format
	// For example, you can split the body string and extract the values

	// Example parsing logic assuming the message body format is "Action: xxx, Key: xxx, Value: xxx"
	parts := strings.Split(body, ",")
	for _, part := range parts {
		pair := strings.Split(part, ":")
		if len(pair) != 2 {
			return "", "", "", fmt.Errorf("invalid message body format")
		}
		field := strings.TrimSpace(pair[0])
		sentValue := strings.TrimSpace(pair[1])
		switch field {
		case "Action":
			action = sentValue
		case "Key":
			key = sentValue
		case "Value":
			value = sentValue
		}
	}

	// Validate the extracted values
	if action == "" {
		return "", "", "", fmt.Errorf("missing required values")
	}

	return action, key, value, nil
}
