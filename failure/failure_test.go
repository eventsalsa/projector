package failure

import (
	"errors"
	"fmt"
	"testing"
)

var errCause = errors.New("connection reset")

func TestRetryableAndPermanentClassify(t *testing.T) {
	retryable := Retryable(errCause)
	if !IsRetryable(retryable) {
		t.Fatal("Retryable(err) is not reported as retryable")
	}
	if IsPermanent(retryable) {
		t.Fatal("Retryable(err) is reported as permanent")
	}

	permanent := Permanent(errCause)
	if !IsPermanent(permanent) {
		t.Fatal("Permanent(err) is not reported as permanent")
	}
	if IsRetryable(permanent) {
		t.Fatal("Permanent(err) is reported as retryable")
	}
}

func TestClassificationPreservesTheCause(t *testing.T) {
	for name, classified := range map[string]error{
		"retryable": Retryable(errCause),
		"permanent": Permanent(errCause),
	} {
		t.Run(name, func(t *testing.T) {
			if !errors.Is(classified, errCause) {
				t.Fatalf("errors.Is(%v, cause) = false, want true", classified)
			}
			if classified.Error() != errCause.Error() {
				t.Fatalf("Error() = %q, want %q", classified.Error(), errCause.Error())
			}
		})
	}
}

func TestClassificationSurvivesWrapping(t *testing.T) {
	wrapped := fmt.Errorf("handle event 7: %w", Retryable(errCause))
	if !IsRetryable(wrapped) {
		t.Fatal("wrapped retryable error is not reported as retryable")
	}
	if !errors.Is(wrapped, errCause) {
		t.Fatal("wrapped classified error does not unwrap to its cause")
	}
}

func TestOutermostClassificationWins(t *testing.T) {
	reclassified := Retryable(Permanent(errCause))
	if !IsRetryable(reclassified) {
		t.Fatal("outer Retryable should win")
	}
	if IsPermanent(reclassified) {
		t.Fatal("inner Permanent should not win over the outer Retryable")
	}

	reclassified = Permanent(Retryable(errCause))
	if !IsPermanent(reclassified) {
		t.Fatal("outer Permanent should win")
	}
	if IsRetryable(reclassified) {
		t.Fatal("inner Retryable should not win over the outer Permanent")
	}
}

func TestNilErrorsAreUnclassified(t *testing.T) {
	if Retryable(nil) != nil {
		t.Fatal("Retryable(nil) should return nil")
	}
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) should return nil")
	}
	if IsRetryable(nil) || IsPermanent(nil) {
		t.Fatal("nil should not be classified")
	}
	if IsRetryable(errCause) || IsPermanent(errCause) {
		t.Fatal("a plain error should not be classified")
	}
}
