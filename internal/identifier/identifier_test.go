package identifier

import (
	"regexp"
	"testing"
)

func TestNewReturnsUUIDv4Shape(t *testing.T) {
	t.Parallel()

	value, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	pattern := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	if !pattern.MatchString(value) {
		t.Fatalf("identifier = %q", value)
	}
}
