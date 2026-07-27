package transformer

import (
	"strings"
	"testing"
)

func TestTransform_Valid(t *testing.T) {
	script := `
		function transform(payload) {
			payload.mutated = true;
			payload.count = payload.count + 1;
			return payload;
		}
	`
	input := `{"count": 1, "name": "test"}`
	expected := `{"count":2,"mutated":true,"name":"test"}`

	out, err := Transform(script, input)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if out != expected {
		t.Errorf("expected %s, got %s", expected, out)
	}
}

func TestTransform_SyntaxError(t *testing.T) {
	script := `
		function transform(payload) {
			return {; // syntax error
		}
	`
	input := `{"count": 1}`
	_, err := Transform(script, input)
	if err == nil {
		t.Fatal("expected syntax error, got nil")
	}
	if !strings.Contains(err.Error(), "compile error") {
		t.Errorf("expected compile error, got %v", err)
	}
}

func TestTransform_Timeout(t *testing.T) {
	script := `
		function transform(payload) {
			while(true) {} // malicious infinite loop
			return payload;
		}
	`
	input := `{"count": 1}`
	
	// This should return an error within ~50ms, not hang the test forever.
	_, err := Transform(script, input)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	
	if !strings.Contains(err.Error(), "execution timeout") {
		t.Errorf("expected 'execution timeout' error, got: %v", err)
	}
}
