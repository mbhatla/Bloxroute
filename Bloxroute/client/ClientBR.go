package main

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type Config struct {
	QueueName string `json:"queueName"`
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
//	Otherwise, an empty string and an error from the call to
func GetQueueURL(sess *session.Session, queue *string) (*sqs.GetQueueUrlOutput, error) {
	// Create an SQS service client
	svc := sqs.New(sess)

	result, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: queue,
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

// SendMsg sends a message to an Amazon SQS queue
// Inputs:
//
//	sess is the current session, which provides configuration for the SDK's service clients
//	queueURL is the URL of the queue
//
// Output:
//
//	If success, nil
//	Otherwise, an error from the call to SendMessage
func SendMsg(sess *session.Session, queueURL *string, action string, key string, value string) error {
	// Create an SQS service client
	svc := sqs.New(sess)
	messageBody := fmt.Sprintf("Action: %s, Key: %s, Value: %s", action, key, value)
	duplicationId, err := generateRandomString(10)
	_, err = svc.SendMessage(&sqs.SendMessageInput{
		MessageBody:            aws.String(messageBody),
		MessageGroupId:         aws.String("Allclients"),
		MessageDeduplicationId: aws.String(duplicationId),
		QueueUrl:               queueURL,
	})

	if err != nil {
		return err
	}

	return nil
}

// Generates a random string to use in dedulicationID, we want no deduplication, even if the content of the message is same
func generateRandomString(length int) (string, error) {
	buffer := make([]byte, length)
	_, err := rand.Read(buffer)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(buffer)[:length], nil
}

// Capture the user input
func getInput(*bufio.Reader) (string, string, string) {

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Please Choose an Action")
	fmt.Println("------------------------")
	fmt.Println("1. Add Item")
	fmt.Println("2. Delete Item")
	fmt.Println("3. Get Item")
	fmt.Println("4. Get All Items")
	fmt.Println("5. Exit")

	for {
		fmt.Print("Please Make a Choice(1-5)->  ")

		char, _, err := reader.ReadRune()

		if err != nil {
			fmt.Println(err)
		}

		// Emptying the newline from the buffer
		_, _, _ = reader.ReadLine()

		switch char {
		case '1':
			fmt.Print("Please Enter the Key for the Item to Add -> ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key) // Remove leading/trailing white space

			if key == "" {
				fmt.Println("Key cannot be empty.")
				continue
			}

			if !isValidInput(key) {
				fmt.Println("Key must contain only alphanumeric characters.")
				continue
			}

			fmt.Print("Please Enter the Value for the Item to Add -> ")
			value, _ := reader.ReadString('\n')
			// convert CRLF to LF
			value = strings.Replace(value, "\n", "", -1)

			if !isValidInput(value) {
				fmt.Println("Value must contain only alphanumeric characters.")
				continue
			}

			return "AddItem", key, value
		case '2':
			fmt.Print("Please Enter the Key for the Item to Delete -> ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key) // Remove leading/trailing white space

			if key == "" {
				fmt.Println("Key cannot be empty.")
				continue
			}

			if !isValidInput(key) {
				fmt.Println("Key must contain only alphanumeric characters.")
				continue
			}

			return "DeleteItem", key, ""
		case '3':
			fmt.Print("Please Enter the Key for the Item to Get -> ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key) // Remove leading/trailing white space

			if key == "" {
				fmt.Println("Key cannot be empty.")
				continue
			}

			if !isValidInput(key) {
				fmt.Println("Key must contain only alphanumeric characters.")
				continue
			}

			return "GetItem", key, ""
		case '4':
			return "GetAllItems", "", ""
		case '5':
			return "Quit", "", ""
		default:
			fmt.Println("Please Make a Valid Choice(1-5)")
		}
	}

}

func isValidInput(input string) bool {
	match, _ := regexp.MatchString("^[a-zA-Z0-9]*$", input)
	return match
}

func main() {
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

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	// Get URL of queue
	result, err := GetQueueURL(sess, &config.QueueName)
	if err != nil {
		fmt.Println("Got an error getting the queue URL:")
		fmt.Println(err)
		return
	}

	queueURL := result.QueueUrl
	reader := bufio.NewReader(os.Stdin)
	for {
		action, key, value := getInput(reader)
		fmt.Println(action, key, value)
		if action == "Quit" {
			return
		}
		err = SendMsg(sess, queueURL, action, key, value)
		if err != nil {
			fmt.Println("Got an error sending the message:")
			fmt.Println(err)
			return
		}

		fmt.Println("Sent message to queue ")

	}
}
