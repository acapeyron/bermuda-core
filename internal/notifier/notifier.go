package notifier

import (
	"fmt"
	"net/http"
	"net/url"
)

type TelegramNotifier struct {
	token  string
	chatID string
}

func New(token, chatID string) *TelegramNotifier {
	return &TelegramNotifier{token: token, chatID: chatID}
}

func (t *TelegramNotifier) Send(msg string) error {
	endpoint := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", t.token)

	resp, err := http.PostForm(endpoint, url.Values{
		"chat_id": {t.chatID},
		"text":    {msg},
	})
	if err != nil {
		return fmt.Errorf("http error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("telegram returned status: %d", resp.StatusCode)
	}

	return nil
}
