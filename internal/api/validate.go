package api

import (
	"encoding/json"
	"errors"
	"net/mail"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/EfeDurmaz16/notification-system/internal/store"
)

type notificationInput struct {
	Recipient   string            `json:"recipient"`
	Channel     string            `json:"channel"`
	Content     *string           `json:"content"`
	Priority    string            `json:"priority"`
	ScheduledAt *time.Time        `json:"scheduledAt"`
	Template    *string           `json:"template"`
	Variables   templateVariables `json:"variables"`
}

type templateVariables map[string]string

// encoding/json otherwise coerces a null map value to the empty string.
func (v *templateVariables) UnmarshalJSON(data []byte) error {
	var input map[string]*string
	if err := json.Unmarshal(data, &input); err != nil {
		return err
	}
	if input == nil {
		return errors.New("variables must be an object")
	}
	values := make(templateVariables, len(input))
	for name, value := range input {
		if value == nil {
			return errors.New("template variable must be a string")
		}
		values[name] = *value
	}
	*v = values
	return nil
}

var phone = regexp.MustCompile(`^\+[1-9][0-9]{1,14}$`)
var varName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)

func validateItem(raw []byte) store.Item {
	var in notificationInput
	if err := strictJSON(raw, &in); err != nil {
		return store.Item{Error: "invalid notification fields or timestamp"}
	}
	if in.ScheduledAt != nil {
		// PostgreSQL stores microseconds. Keep the stored instant representable
		// as an RFC 3339 year, including after applying the client's UTC offset.
		utc := in.ScheduledAt.UTC().Round(time.Microsecond)
		if in.ScheduledAt.Year() == 0 || utc.Year() < 1 || utc.Year() > 9999 {
			return store.Item{Error: "scheduledAt must fall in years 0001 to 9999 UTC"}
		}
		in.ScheduledAt = &utc
	}
	if !validRecipient(in.Channel, in.Recipient) {
		return store.Item{Error: "invalid channel or recipient"}
	}
	if in.Priority != "high" && in.Priority != "normal" && in.Priority != "low" {
		return store.Item{Error: "priority must be high, normal, or low"}
	}
	if (in.Content == nil) == (in.Template == nil) {
		return store.Item{Error: "provide exactly one of content or template"}
	}
	limit := 256
	switch in.Channel {
	case "sms":
		limit = 160
	case "email":
		limit = 10000
	}
	var content string
	if in.Content != nil {
		if len(in.Variables) > 0 {
			return store.Item{Error: "variables require a template"}
		}
		content = *in.Content
	} else {
		text, err := render(*in.Template, in.Variables, limit*utf8.UTFMax)
		if err != nil {
			return store.Item{Error: err.Error()}
		}
		content = text
	}
	if strings.TrimSpace(content) == "" || utf8.RuneCountInString(content) > limit || strings.ContainsRune(content, 0) {
		return store.Item{Error: "content is empty, too long, or contains a null character"}
	}
	return store.Item{Recipient: in.Recipient, Channel: in.Channel, Priority: in.Priority, Content: content, ScheduledAt: in.ScheduledAt}
}

func validRecipient(channel, recipient string) bool {
	switch channel {
	case "sms":
		return phone.MatchString(recipient)
	case "email":
		if len(recipient) > 254 || strings.ContainsRune(recipient, 0) {
			return false
		}
		addr, err := mail.ParseAddress(recipient)
		return err == nil && addr.Address == recipient
	case "push":
		return len(recipient) > 0 && len(recipient) <= 512 && strings.IndexFunc(recipient, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) < 0
	default:
		return false
	}
}

// render accepts named placeholders only. Values are opaque strings and are
// never evaluated as templates. Bound expansion before copying a replacement.
func render(tmpl string, vars map[string]string, maxBytes int) (string, error) {
	var out strings.Builder
	for {
		before, rest, found := strings.Cut(tmpl, "{{")
		if strings.Contains(before, "}}") {
			return "", errors.New("unmatched template delimiter")
		}
		if out.Len()+len(before) > maxBytes {
			return "", errors.New("rendered content too long")
		}
		out.WriteString(before)
		if !found {
			break
		}
		name, tail, end := strings.Cut(rest, "}}")
		if !end || !varName.MatchString(name) {
			return "", errors.New("invalid template placeholder")
		}
		value, ok := vars[name]
		if !ok {
			return "", errors.New("missing template variable: " + name)
		}
		if out.Len()+len(value) > maxBytes {
			return "", errors.New("rendered content too long")
		}
		out.WriteString(value)
		tmpl = tail
	}
	return out.String(), nil
}
