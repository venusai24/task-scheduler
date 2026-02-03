package main

import (
	"os"
	"testing"
)

func TestExtractScript(t *testing.T) {
	tests := []struct {
		name     string
		yaml     string
		expected string
	}{
		{
			name: "Simple Script",
			yaml: `
name: test
script: echo hello
`,
			expected: "echo hello",
		},
		{
			name: "Multiline Block",
			yaml: `
script: |
  echo line 1
  echo line 2
`,
			expected: "  echo line 1\n  echo line 2\n",
		},
		{
			name: "Nested indentation",
			yaml: `
task:
  script:
    echo nested
`,
			expected: "    echo nested\n",
		},
		{
			name:     "Quoted string",
			yaml:     `script: "echo quoted"`,
			expected: "echo quoted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractScript(tt.yaml)
			if got != tt.expected {
				t.Errorf("Expected:\n%q\nGot:\n%q", tt.expected, got)
			}
		})
	}
}

func TestInjectSecrets(t *testing.T) {
	os.Setenv("TEST_SECRET", "super-secret")
	defer os.Unsetenv("TEST_SECRET")

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Replace Env Var",
			input:    "echo $TEST_SECRET",
			expected: "echo super-secret",
		},
		{
			name:     "Keep Missing Var",
			input:    "echo $MISSING_VAR",
			expected: "echo $MISSING_VAR",
		},
		{
			name:     "Braces syntax",
			input:    "echo ${TEST_SECRET}",
			expected: "echo super-secret",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := injectSecrets(tt.input)
			if got != tt.expected {
				t.Errorf("Expected:\n%q\nGot:\n%q", tt.expected, got)
			}
		})
	}
}
