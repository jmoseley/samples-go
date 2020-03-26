package main

import (
	"context"
	"flag"
	"time"

	"go.temporal.io/temporal/client"
	"go.uber.org/zap"

	"github.com/temporalio/temporal-go-samples/cancelactivity"
)

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	var workflowID string
	flag.StringVar(&workflowID, "w", "workflowID-to-cancel", "w is the workflowID of the workflow to be canceled.")
	flag.Parse()

	// The client is a heavyweight object that should be created once per process.
	c, err := client.NewClient(client.Options{
		HostPort: client.DefaultHostPort,
	})
	if err != nil {
		logger.Fatal("Unable to create client", zap.Error(err))
	}

	workflowOptions := client.StartWorkflowOptions{
		ID:                              workflowID,
		TaskList:                        "cancel-activity-task-list",
		ExecutionStartToCloseTimeout:    time.Minute,
		DecisionTaskStartToCloseTimeout: time.Minute,
	}

	we, err := c.ExecuteWorkflow(context.Background(), workflowOptions, cancelactivity.Workflow)
	if err != nil {
		logger.Error("Unable to execute workflow", zap.Error(err))
	} else {
		logger.Info("Started workflow", zap.String("WorkflowID", we.GetID()), zap.String("RunID", we.GetRunID()))
	}

	// Close connection, clean up resources.
	_ = c.CloseConnection()
}