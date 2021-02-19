package expense

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

var (
	expenseServerHostPort = "http://localhost:8099"
)

// SampleExpenseWorkflow workflow definition
func SampleExpenseWorkflow(ctx workflow.Context, expenseID string, companyID int) (result string, err error) {
	// step 1, create new expense report
	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
		HeartbeatTimeout:       time.Second * 20,
	}
	ctx1 := workflow.WithActivityOptions(ctx, ao)
	logger := workflow.GetLogger(ctx)

	err = workflow.ExecuteActivity(ctx1, CreateExpenseActivity, expenseID).Get(ctx1, nil)
	if err != nil {
		logger.Error("Failed to create expense report", "Error", err)
		return "", err
	}

	// step 2, wait for the expense report to be approved (or rejected)
	ao = workflow.ActivityOptions{
		ScheduleToStartTimeout: 10 * time.Minute,
		StartToCloseTimeout:    10 * time.Minute,
	}
	ctx2 := workflow.WithActivityOptions(ctx, ao)
	// Notice that we set the timeout to be 10 minutes for this sample demo. If the expected time for the activity to
	// complete (waiting for human to approve the request) is longer, you should set the timeout accordingly so the
	// Temporal system will wait accordingly. Otherwise, Temporal system could mark the activity as failure by timeout.
	var status string
	err = workflow.ExecuteActivity(ctx2, WaitForDecisionActivity, expenseID).Get(ctx2, &status)
	if err != nil {
		return "", err
	}

	if status != "APPROVED" {
		logger.Info("Workflow completed.", "ExpenseStatus", status)
		return "", nil
	}

	logger.Info(fmt.Sprint("Sending signal for payment for expense ", expenseID))

	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToCloseTimeout: time.Minute * 1,
	})

	var sendPaymentActivityResult string
	sendPaymentErr := workflow.ExecuteActivity(activityCtx,
		SignalWithStartExpenseApprovedActivity, expenseID,
		companyID).Get(ctx, &sendPaymentActivityResult)
	if sendPaymentErr != nil {
		return "", err
	}
	logger.Info(fmt.Sprint("Sent signal for expense ", expenseID, sendPaymentActivityResult))

	return "COMPLETED", nil
}

func SendPaymentsWorkflow(ctx workflow.Context, companyID int) (result string, err error) {
	logger := workflow.GetLogger(ctx)
	logger.Info(fmt.Sprint("Payments workflow for company ", companyID))

	var expenseIDsToPay []string
	var doneWaiting bool

	channel := workflow.GetSignalChannel(ctx, "ExpenseApproved")
	channelSelector := workflow.NewSelector(ctx)
	channelSelector.AddReceive(channel, func(c workflow.ReceiveChannel, more bool) {
		if !doneWaiting {
			var expenseID string
			c.Receive(ctx, &expenseID)
			logger.Info(fmt.Sprint("Received signal for expense ", expenseID))
			expenseIDsToPay = append(expenseIDsToPay, expenseID)
		} else {
			logger.Info(fmt.Sprint("Ignoring signal because payments are sent"))
		}
	})

	timerFuture := workflow.NewTimer(ctx, time.Second*30)
	channelSelector.AddFuture(timerFuture, func(f workflow.Future) {
		doneWaiting = true
	})
	for !doneWaiting {
		channelSelector.Select(ctx)
	}

	logger.Info(fmt.Sprint("Sending payments for ", len(expenseIDsToPay), " expenses"))
	// This is my "bulk" api call, just for the sake of the POC.
	for _, expenseID := range expenseIDsToPay {
		logger.Info(fmt.Sprint("Sending payment for expense ", expenseID))
		ao := workflow.ActivityOptions{
			ScheduleToStartTimeout: time.Minute,
			StartToCloseTimeout:    time.Minute,
			HeartbeatTimeout:       time.Second * 20,
		}
		ctx1 := workflow.WithActivityOptions(ctx, ao)
		err = workflow.ExecuteActivity(ctx1, PaymentActivity, expenseID).Get(ctx1, nil)
		if err != nil {
			logger.Info("Payment Failed", "Error", err)
		}
	}

	logger.Info(fmt.Sprint("Workflow completed for companyID ", companyID))

	return "", workflow.NewContinueAsNewError(ctx, SendPaymentsWorkflow, companyID)
}
