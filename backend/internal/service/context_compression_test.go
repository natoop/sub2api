package service

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func newContextCompressionServiceForTest(strategy string, triggerTokens, keepLastMessages, keepLastTokens int, resolver *ModelPricingResolver) *ContextCompressionService {
	return NewContextCompressionService(&config.Config{
		Gateway: config.GatewayConfig{
			ContextCompression: config.ContextCompressionConfig{
				Enabled:          true,
				Strategy:         strategy,
				TriggerTokens:    triggerTokens,
				KeepLastMessages: keepLastMessages,
				KeepLastTokens:   keepLastTokens,
			},
		},
	}, resolver)
}

func TestTruncateTextPreservesUTF8(t *testing.T) {
	got := truncateText("你好世界🙂abc", 3)

	require.True(t, utf8.ValidString(got))
	require.Equal(t, "你好世...", got)
}

func TestCompressAnthropicBodyCountsStaticRequestTokens(t *testing.T) {
	svc := newContextCompressionServiceForTest(config.CompressionStrategyTruncate, 20, 1, 100, nil)
	messages := []any{
		map[string]any{"role": "user", "content": []any{map[string]any{"type": "text", "text": "old question"}}},
		map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "text", "text": "old answer"}}},
		map[string]any{"role": "user", "content": []any{map[string]any{"type": "text", "text": "latest question"}}},
	}
	body, err := json.Marshal(map[string]any{
		"model":    "claude-test",
		"system":   strings.Repeat("static instruction ", 80),
		"messages": messages,
	})
	require.NoError(t, err)

	compressedBody, compressed := svc.CompressAnthropicBody(body, messages, "claude-test", PlatformAnthropic)

	require.True(t, compressed)
	require.Len(t, gjson.GetBytes(compressedBody, "messages").Array(), 1)
	require.Equal(t, "latest question", gjson.GetBytes(compressedBody, "messages.0.content.0.text").String())
}

func TestModelWindowCapsCompressionBudget(t *testing.T) {
	pricingService := &PricingService{
		pricingData: map[string]*LiteLLMModelPricing{
			"tiny-model": {
				InputCostPerToken: 1,
				MaxInputTokens:    80,
			},
		},
	}
	billingService := &BillingService{pricingService: pricingService}
	resolver := NewModelPricingResolver(nil, billingService)
	svc := newContextCompressionServiceForTest(config.CompressionStrategyTruncate, 1000, 20, 1000, resolver)

	messages := make([]any, 0, 8)
	for i := 0; i < 8; i++ {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		messages = append(messages, map[string]any{
			"role":    role,
			"content": strings.Repeat("window pressure ", 12),
		})
	}
	body, err := json.Marshal(map[string]any{"model": "tiny-model", "messages": messages})
	require.NoError(t, err)

	compressedBody, compressed := svc.CompressChatCompletionsBody(body, "tiny-model", PlatformOpenAI)

	require.True(t, compressed)
	require.Less(t, len(gjson.GetBytes(compressedBody, "messages").Array()), len(messages))
}

func TestTruncateCCMessagesDropsOrphanToolMessages(t *testing.T) {
	messages := []any{
		map[string]any{"role": "user", "content": "use a tool"},
		map[string]any{
			"role": "assistant",
			"tool_calls": []any{map[string]any{
				"id":   "call_1",
				"type": "function",
				"function": map[string]any{
					"name":      "lookup",
					"arguments": "{}",
				},
			}},
		},
		map[string]any{"role": "tool", "tool_call_id": "call_1", "content": "tool result"},
	}

	got, compressed := truncateCCMessages(messages, 1, 100)

	require.True(t, compressed)
	require.Len(t, got, 1)
	require.Equal(t, "user", got[0].(map[string]any)["role"])
	require.NotContains(t, stringifyForTest(t, got), `"role":"tool"`)
}

func TestSingleOversizedLatestMessageIsTrimmed(t *testing.T) {
	messages := []any{
		map[string]any{"role": "user", "content": strings.Repeat("0123456789", 200)},
	}

	got, compressed := truncateMessages(messages, 20, 10)

	require.True(t, compressed)
	require.Less(t, estimateTokensFromMessagesSlice(got), estimateTokensFromMessagesSlice(messages))
	content := got[0].(map[string]any)["content"].(string)
	require.True(t, utf8.ValidString(content))
	require.True(t, strings.HasSuffix(content, "..."))
}

func stringifyForTest(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	return string(raw)
}
