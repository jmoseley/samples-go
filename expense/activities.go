package expense

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

type (
	ContextKey string
)

const (
	ClientContextKey ContextKey = "Client"
)

func CreateExpenseActivity(ctx context.Context, expenseID string) error {
	if len(expenseID) == 0 {
		return errors.New("expense id is empty")
	}

	resp, err := http.Get(expenseServerHostPort + "/create?is_api_call=true&id=" + expenseID)
	if err != nil {
		return err
	}
	body, err := ioutil.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return err
	}

	if string(body) == "SUCCEED" {
		activity.GetLogger(ctx).Info("Expense created.", "ExpenseID", expenseID)
		return nil
	}

	return errors.New(string(body))
}

// waitForDecisionActivity waits for the expense decision. This activity will complete asynchronously. When this method
// returns error activity.ErrResultPending, the Temporal Go SDK recognize this error, and won't mark this activity
// as failed or completed. The Temporal server will wait until Client.CompleteActivity() is called or timeout happened
// whichever happen first. In this sample case, the CompleteActivity() method is called by our dummy expense server when
// the expense is approved.
func WaitForDecisionActivity(ctx context.Context, expenseID string) (string, error) {
	if len(expenseID) == 0 {
		return "", errors.New("expense id is empty")
	}

	logger := activity.GetLogger(ctx)

	// save current activity info so it can be completed asynchronously when expense is approved/rejected
	activityInfo := activity.GetInfo(ctx)
	formData := url.Values{}
	formData.Add("task_token", string(activityInfo.TaskToken))

	registerCallbackURL := expenseServerHostPort + "/registerCallback?id=" + expenseID
	resp, err := http.PostForm(registerCallbackURL, formData)
	if err != nil {
		logger.Info("waitForDecisionActivity failed to register callback.", "Error", err)
		return "", err
	}
	body, err := ioutil.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return "", err
	}

	status := string(body)
	if status == "SUCCEED" {
		// register callback succeed
		logger.Info("Successfully registered callback.", "ExpenseID", expenseID)

		// ErrActivityResultPending is returned from activity's execution to indicate the activity is not completed when it returns.
		// activity will be completed asynchronously when Client.CompleteActivity() is called.
		return "", activity.ErrResultPending
	}

	logger.Warn("Register callback failed.", "ExpenseStatus", status)
	return "", fmt.Errorf("register callback failed status:%s", status)
}

func SignalWithStartExpenseApprovedActivity(
	ctx context.Context,
	expenseID string,
	companyID int,
) (string, error) {
	c := ctx.Value(ClientContextKey).(client.Client)
	workflowID := fmt.Sprintf(
		"%s:%d",
		"batch_payment",
		companyID,
	)
	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: "expense",
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    5,
		},
	}
	activity.GetLogger(ctx).Info("Signaling for payment", "Company ID", companyID, "Expense ID", expenseID, "WorkflowID", workflowID)
	wr, err := c.SignalWithStartWorkflow(
		ctx, workflowID, "ExpenseApproved", expenseID,
		workflowOptions, SendPaymentsWorkflow, companyID)

	if err != nil {
		activity.GetLogger(ctx).Error("Unable to signal with start workflow", "Error", err)
		return "", err
	}
	activity.GetLogger(ctx).Info("Signaled and started Workflow", "WorkflowID", wr.GetID(), "RunID", wr.GetRunID())

	return "success", nil
}

func PaymentActivity(ctx context.Context, expenseID string) error {
	if len(expenseID) == 0 {
		return errors.New("expense id is empty")
	}

	resp, err := http.Get(expenseServerHostPort + "/action?is_api_call=true&type=payment&id=" + expenseID)
	if err != nil {
		return err
	}
	body, err := ioutil.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return err
	}

	if string(body) == "SUCCEED" {
		activity.GetLogger(ctx).Info("paymentActivity succeed", "ExpenseID", expenseID)
		return nil
	}

	return errors.New(string(body))
}
