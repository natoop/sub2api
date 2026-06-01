package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"unicode/utf8"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// ContextCompressionService 上下文压缩服务
// 在请求转发到上游之前，对消息上下文进行截断压缩，减少上游 token 消耗。
type ContextCompressionService struct {
	cfg      *config.ContextCompressionConfig
	resolver *ModelPricingResolver
}

// ContextCompressionOptions 是分组级上下文压缩覆盖配置。
// 空策略和 0 数值表示继承全局 gateway.context_compression 配置。
type ContextCompressionOptions struct {
	Strategy         string
	TriggerTokens    int
	KeepLastMessages int
	KeepLastTokens   int
	GroupID          *int64
}

type resolvedContextCompressionOptions struct {
	Strategy         string
	TriggerTokens    int
	KeepLastMessages int
	KeepLastTokens   int
	GroupID          *int64
}

// NewContextCompressionService 创建上下文压缩服务
func NewContextCompressionService(cfg *config.Config, resolver *ModelPricingResolver) *ContextCompressionService {
	if cfg == nil {
		return &ContextCompressionService{
			cfg:      &config.ContextCompressionConfig{Enabled: false},
			resolver: resolver,
		}
	}
	return &ContextCompressionService{
		cfg:      &cfg.Gateway.ContextCompression,
		resolver: resolver,
	}
}

// IsEnabled 检查压缩功能是否对当前请求生效
func (s *ContextCompressionService) IsEnabled(platform, model string) bool {
	if s.cfg == nil || !s.cfg.Enabled {
		return false
	}
	if !s.cfg.IsPlatformEnabled(platform) {
		return false
	}
	if !s.cfg.IsModelEnabled(model) {
		return false
	}
	return true
}

func contextCompressionOptionsFromGroup(group *Group) *ContextCompressionOptions {
	if group == nil || !group.ContextCompressionEnabled {
		return nil
	}
	return &ContextCompressionOptions{
		Strategy:         group.ContextCompressionStrategy,
		TriggerTokens:    group.ContextCompressionTriggerTokens,
		KeepLastMessages: group.ContextCompressionKeepLastMessages,
		KeepLastTokens:   group.ContextCompressionKeepLastTokens,
		GroupID:          contextCompressionGroupID(group),
	}
}

func contextCompressionGroupID(group *Group) *int64 {
	if group == nil || group.ID <= 0 {
		return nil
	}
	id := group.ID
	return &id
}

func (s *ContextCompressionService) resolveOptions(options *ContextCompressionOptions) resolvedContextCompressionOptions {
	strategy := ""
	triggerTokens := 0
	keepLast := 0
	keepTokens := 0
	if options != nil {
		strategy = options.Strategy
		triggerTokens = options.TriggerTokens
		keepLast = options.KeepLastMessages
		keepTokens = options.KeepLastTokens
	}
	if s.cfg != nil {
		if strings.TrimSpace(strategy) == "" {
			strategy = s.cfg.Strategy
		}
		if triggerTokens <= 0 {
			triggerTokens = s.cfg.TriggerTokens
		}
		if keepLast <= 0 {
			keepLast = s.cfg.KeepLastMessages
		}
		if keepTokens <= 0 {
			keepTokens = s.cfg.KeepLastTokens
		}
	}

	strategy = strings.ToLower(strings.TrimSpace(strategy))
	if strategy != config.CompressionStrategySummarize && strategy != config.CompressionStrategyTruncate {
		strategy = config.CompressionStrategyTruncate
	}
	if triggerTokens <= 0 {
		triggerTokens = 64000
	}
	if keepLast <= 0 {
		keepLast = 20
	}
	if keepTokens <= 0 {
		keepTokens = 32000
	}
	return resolvedContextCompressionOptions{
		Strategy:         strategy,
		TriggerTokens:    triggerTokens,
		KeepLastMessages: keepLast,
		KeepLastTokens:   keepTokens,
		GroupID:          contextCompressionResolvedGroupID(options),
	}
}

func contextCompressionResolvedGroupID(options *ContextCompressionOptions) *int64 {
	if options == nil || options.GroupID == nil || *options.GroupID <= 0 {
		return nil
	}
	id := *options.GroupID
	return &id
}

type requestCompressionBudget struct {
	MessageTokens int
	StaticTokens  int
	TotalTokens   int
	TriggerTokens int
	KeepTokens    int
}

func (s *ContextCompressionService) resolveRequestBudget(body []byte, fieldName, model string, messages []any, resolved resolvedContextCompressionOptions) requestCompressionBudget {
	messageTokens := estimateTokensFromMessagesSlice(messages)
	staticTokens := estimateStaticTokensFromBody(body, fieldName)
	triggerTokens := resolved.TriggerTokens
	keepTokens := resolved.KeepLastTokens

	if maxInputTokens := s.resolveModelMaxInputTokens(model, resolved.GroupID); maxInputTokens > 0 {
		usableInputTokens := maxInputTokens - contextCompressionSafetyTokens(maxInputTokens)
		if usableInputTokens < 1 {
			usableInputTokens = maxInputTokens
		}
		if usableInputTokens > 0 && triggerTokens > usableInputTokens {
			triggerTokens = usableInputTokens
		}

		messageBudget := usableInputTokens - staticTokens
		if messageBudget < 1 {
			messageBudget = 1
		}
		if keepTokens > messageBudget {
			keepTokens = messageBudget
		}
	}

	return requestCompressionBudget{
		MessageTokens: messageTokens,
		StaticTokens:  staticTokens,
		TotalTokens:   messageTokens + staticTokens,
		TriggerTokens: triggerTokens,
		KeepTokens:    keepTokens,
	}
}

func (s *ContextCompressionService) resolveModelMaxInputTokens(model string, groupID *int64) int {
	if s == nil || s.resolver == nil || strings.TrimSpace(model) == "" {
		return 0
	}
	resolved := s.resolver.Resolve(context.Background(), PricingInput{
		Model:   model,
		GroupID: groupID,
	})
	if (resolved == nil || resolved.BasePricing == nil) && groupID != nil {
		resolved = s.resolver.Resolve(context.Background(), PricingInput{Model: model})
	}
	if resolved == nil || resolved.BasePricing == nil {
		return 0
	}
	return resolved.BasePricing.MaxInputTokens
}

func contextCompressionSafetyTokens(maxInputTokens int) int {
	if maxInputTokens <= 0 {
		return 0
	}
	safety := maxInputTokens / 20
	if safety < 1024 {
		safety = 1024
	}
	if safety > 8192 {
		safety = 8192
	}
	if safety >= maxInputTokens {
		safety = maxInputTokens / 10
		if safety < 1 {
			safety = 1
		}
	}
	return safety
}

// CompressAnthropicBody 对 Anthropic 格式的请求体进行上下文压缩
// 返回压缩后的 body 和是否执行了压缩
func (s *ContextCompressionService) CompressAnthropicBody(body []byte, messages []any, model, platform string) ([]byte, bool) {
	return s.compressAnthropicBody(body, messages, model, platform, nil)
}

// CompressAnthropicBodyForGroup 使用分组级覆盖参数对 Anthropic 请求体进行上下文压缩。
func (s *ContextCompressionService) CompressAnthropicBodyForGroup(body []byte, messages []any, model, platform string, group *Group) ([]byte, bool) {
	if group == nil || !group.ContextCompressionEnabled {
		return body, false
	}
	return s.compressAnthropicBody(body, messages, model, platform, contextCompressionOptionsFromGroup(group))
}

func (s *ContextCompressionService) compressAnthropicBody(body []byte, messages []any, model, platform string, options *ContextCompressionOptions) ([]byte, bool) {
	if !s.IsEnabled(platform, model) {
		return body, false
	}

	resolved := s.resolveOptions(options)
	budget := s.resolveRequestBudget(body, "messages", model, messages, resolved)
	if budget.TotalTokens <= budget.TriggerTokens {
		return body, false
	}

	var newMessages []any
	var compressed bool
	switch resolved.Strategy {
	case config.CompressionStrategySummarize:
		newMessages, compressed = truncateAndSummarize(messages, resolved.KeepLastMessages, budget.KeepTokens)
	default:
		newMessages, compressed = truncateMessages(messages, resolved.KeepLastMessages, budget.KeepTokens)
	}
	if !compressed {
		return body, false
	}

	newBody, err := replaceMessagesInBody(body, newMessages)
	if err != nil {
		slog.Warn("context_compression: failed to replace messages in body", "error", err)
		return body, false
	}

	slog.Info("context_compression: messages compressed",
		"strategy", resolved.Strategy,
		"original_tokens", budget.TotalTokens,
		"message_tokens", budget.MessageTokens,
		"static_tokens", budget.StaticTokens,
		"original_count", len(messages),
		"compressed_count", len(newMessages),
	)

	return newBody, true
}

// ========================
// Token 估算
// ========================

// estimateTokensFromMessagesSlice 估算消息列表中所有文本的 token 数
// 使用启发式算法: 字符数 / 4 ≈ token 数 (英文)，中文 rune / 2 ≈ token 数
func estimateTokensFromMessagesSlice(messages []any) int {
	total := 0
	for _, msg := range messages {
		total += estimateTokensFromValue(msg)
	}
	return total
}

func estimateStaticTokensFromBody(body []byte, conversationField string) int {
	if len(body) == 0 {
		return 0
	}
	staticFields := []string{
		"system",
		"instructions",
		"tools",
		"tool_choice",
		"response_format",
		"reasoning",
		"thinking",
		"context_management",
	}

	total := 0
	for _, field := range staticFields {
		if field == conversationField {
			continue
		}
		result := gjson.GetBytes(body, field)
		if !result.Exists() {
			continue
		}
		total += estimateTokensFromJSONRaw(result.Raw)
	}
	return total
}

func estimateTokensFromJSONRaw(raw string) int {
	if strings.TrimSpace(raw) == "" {
		return 0
	}
	var v any
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return estimateTokensFromString(raw)
	}
	return estimateTokensFromValue(v)
}

func estimateTokensFromValue(v any) int {
	switch val := v.(type) {
	case string:
		return estimateTokensFromString(val)
	case map[string]any:
		total := 0
		for _, childVal := range val {
			total += estimateTokensFromValue(childVal)
		}
		return total
	case []any:
		total := 0
		for _, item := range val {
			total += estimateTokensFromValue(item)
		}
		return total
	default:
		// 数字、bool、null 等非文本值，token 数极少
		if val != nil {
			s := fmt.Sprintf("%v", val)
			return estimateTokensFromString(s)
		}
		return 0
	}
}

func estimateTokensFromString(s string) int {
	if len(s) == 0 {
		return 0
	}
	// 计算中文/全角字符占比
	runeCount := utf8.RuneCountInString(s)
	if runeCount == 0 {
		return 0
	}
	cjkCount := 0
	for _, r := range s {
		if isCJK(r) {
			cjkCount++
		}
	}

	byteLen := len(s)
	// 中文字符 token 化率约 1.5-2 字符/token，英文约 4 字符/token
	nonCJK := runeCount - cjkCount
	estimated := (cjkCount * 10 / 15) + (nonCJK * 10 / 40) // 简化: cjk/1.5 + ascii/4.0
	if estimated < 1 {
		estimated = byteLen / 4
		if estimated < 1 {
			estimated = 1
		}
	}
	return estimated
}

func isCJK(r rune) bool {
	return (r >= 0x4E00 && r <= 0x9FFF) || // CJK Unified Ideographs
		(r >= 0x3400 && r <= 0x4DBF) || // CJK Unified Ideographs Extension A
		(r >= 0x20000 && r <= 0x2A6DF) || // CJK Unified Ideographs Extension B
		(r >= 0x2A700 && r <= 0x2B73F) || // CJK Unified Ideographs Extension C
		(r >= 0x2B740 && r <= 0x2B81F) || // CJK Unified Ideographs Extension D
		(r >= 0x2B820 && r <= 0x2CEAF) || // CJK Unified Ideographs Extension E
		(r >= 0xF900 && r <= 0xFAFF) || // CJK Compatibility Ideographs
		(r >= 0x2F800 && r <= 0x2FA1F) || // CJK Compatibility Ideographs Supplement
		(r >= 0x3000 && r <= 0x303F) || // CJK Symbols and Punctuation
		(r >= 0xFF00 && r <= 0xFFEF) || // Halfwidth and Fullwidth Forms
		(r >= 0x3040 && r <= 0x309F) || // Hiragana
		(r >= 0x30A0 && r <= 0x30FF) || // Katakana
		(r >= 0xAC00 && r <= 0xD7AF) // Hangul Syllables
}

// ========================
// 消息截断
// ========================

// truncateMessages 从旧到新截断消息，保留最近 keepLast 条且 token 数不超过 keepTokens
func truncateMessages(messages []any, keepLast, keepTokens int) ([]any, bool) {
	if len(messages) <= keepLast {
		total := estimateTokensFromMessagesSlice(messages)
		if total <= keepTokens {
			return messages, false
		}
	}

	// 从后往前累加 token，找到截断点
	// 策略：保留最近的消息，同时满足 keepLast 和 keepTokens 两个约束
	cutIdx := len(messages)
	keptTokens := 0
	keptCount := 0

	for i := len(messages) - 1; i >= 0; i-- {
		msgTokens := estimateTokensFromValue(messages[i])
		if keptCount >= keepLast {
			break
		}
		if keptTokens+msgTokens > keepTokens && keptCount > 0 {
			break
		}
		keptTokens += msgTokens
		keptCount++
		cutIdx = i
	}

	if cutIdx >= len(messages) {
		return messages, false
	}
	if cutIdx == 0 {
		shrunk, changed := shrinkMessagesToTokenBudget(messages, keepTokens)
		if changed {
			return ensureAnthropicStartsWithUser(shrunk), true
		}
		return messages, false
	}

	kept := messages[cutIdx:]

	// 确保第一条消息是 user（如果不是，在开头插入占位消息）
	if len(kept) > 0 {
		firstMsg, ok := kept[0].(map[string]any)
		if ok {
			role, _ := firstMsg["role"].(string)
			if role == "assistant" {
				placeholder := map[string]any{
					"role":    "user",
					"content": []any{map[string]any{"type": "text", "text": "(Earlier conversation omitted)"}},
				}
				kept = append([]any{placeholder}, kept...)
			}
		}
	}

	kept, _ = sanitizeToolContinuity(kept)
	kept = ensureAnthropicStartsWithUser(kept)
	return kept, true
}

func ensureAnthropicStartsWithUser(messages []any) []any {
	if len(messages) == 0 {
		return messages
	}
	firstMsg, ok := messages[0].(map[string]any)
	if !ok {
		return messages
	}
	role, _ := firstMsg["role"].(string)
	if role != "assistant" {
		return messages
	}
	placeholder := map[string]any{
		"role":    "user",
		"content": []any{map[string]any{"type": "text", "text": "(Earlier conversation omitted)"}},
	}
	return append([]any{placeholder}, messages...)
}

func ensureChatStartsAfterSystem(messages []any) []any {
	firstConversationIdx := -1
	for i, msg := range messages {
		msgMap, ok := msg.(map[string]any)
		if !ok {
			firstConversationIdx = i
			break
		}
		role, _ := msgMap["role"].(string)
		if role != "system" && role != "developer" {
			firstConversationIdx = i
			break
		}
	}
	if firstConversationIdx < 0 {
		return messages
	}

	msgMap, ok := messages[firstConversationIdx].(map[string]any)
	if !ok {
		return messages
	}
	role, _ := msgMap["role"].(string)
	if role != "assistant" && role != "tool" {
		return messages
	}

	placeholder := map[string]any{
		"role":    "user",
		"content": "(Earlier conversation omitted)",
	}
	result := make([]any, 0, len(messages)+1)
	result = append(result, messages[:firstConversationIdx]...)
	result = append(result, placeholder)
	result = append(result, messages[firstConversationIdx:]...)
	return result
}

func shrinkMessagesToTokenBudget(messages []any, keepTokens int) ([]any, bool) {
	if keepTokens <= 0 {
		keepTokens = 1
	}
	if estimateTokensFromMessagesSlice(messages) <= keepTokens {
		return messages, false
	}

	cloned, ok := cloneAny(messages).([]any)
	if !ok {
		return messages, false
	}

	changed := false
	maxRunes := keepTokens * 4
	if maxRunes < 1 {
		maxRunes = 1
	}
	for maxRunes >= 1 && estimateTokensFromMessagesSlice(cloned) > keepTokens {
		if truncateMessageTextContent(cloned, maxRunes) {
			changed = true
		}
		if maxRunes == 1 {
			break
		}
		maxRunes = maxRunes / 2
		if maxRunes < 1 {
			maxRunes = 1
		}
	}

	if !changed {
		return messages, false
	}
	cloned, _ = sanitizeToolContinuity(cloned)
	return cloned, true
}

func cloneAny(v any) any {
	raw, err := json.Marshal(v)
	if err != nil {
		return v
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return v
	}
	return out
}

func truncateMessageTextContent(messages []any, maxRunes int) bool {
	changed := false
	for _, msg := range messages {
		msgMap, ok := msg.(map[string]any)
		if !ok {
			continue
		}
		if truncateContentValue(msgMap, "content", maxRunes) {
			changed = true
		}
	}
	return changed
}

func truncateContentValue(m map[string]any, key string, maxRunes int) bool {
	content, exists := m[key]
	if !exists {
		return false
	}
	changed := false
	switch c := content.(type) {
	case string:
		next := truncateText(c, maxRunes)
		if next != c {
			m[key] = next
			changed = true
		}
	case []any:
		for _, item := range c {
			itemMap, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if truncateBlockTextContent(itemMap, maxRunes) {
				changed = true
			}
		}
	}
	return changed
}

func truncateBlockTextContent(block map[string]any, maxRunes int) bool {
	changed := false
	if text, ok := block["text"].(string); ok {
		next := truncateText(text, maxRunes)
		if next != text {
			block["text"] = next
			changed = true
		}
	}
	if truncateContentValue(block, "content", maxRunes) {
		changed = true
	}
	return changed
}

func sanitizeToolContinuity(messages []any) ([]any, bool) {
	seenToolIDs := make(map[string]struct{})
	result := make([]any, 0, len(messages))
	changed := false

	for _, msg := range messages {
		msgMap, ok := msg.(map[string]any)
		if !ok {
			result = append(result, msg)
			continue
		}
		role, _ := msgMap["role"].(string)
		if role == "tool" {
			toolID := toolMessageID(msgMap)
			if toolID != "" {
				if _, ok := seenToolIDs[toolID]; !ok {
					changed = true
					continue
				}
			}
		}

		nextMsg := msg
		if sanitized, msgChanged := removeOrphanToolResultBlocks(msgMap, seenToolIDs); msgChanged {
			nextMsg = sanitized
			changed = true
		}
		result = append(result, nextMsg)
		collectToolUseIDs(msgMap, seenToolIDs)
	}

	return result, changed
}

func toolMessageID(msg map[string]any) string {
	for _, key := range []string{"tool_call_id", "tool_use_id", "id"} {
		if v, ok := msg[key].(string); ok && strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func collectToolUseIDs(msg map[string]any, seen map[string]struct{}) {
	if toolCalls, ok := msg["tool_calls"].([]any); ok {
		for _, call := range toolCalls {
			callMap, ok := call.(map[string]any)
			if !ok {
				continue
			}
			if id, ok := callMap["id"].(string); ok && id != "" {
				seen[id] = struct{}{}
			}
		}
	}
	content, ok := msg["content"].([]any)
	if !ok {
		return
	}
	for _, block := range content {
		blockMap, ok := block.(map[string]any)
		if !ok {
			continue
		}
		blockType, _ := blockMap["type"].(string)
		if blockType != "tool_use" {
			continue
		}
		if id, ok := blockMap["id"].(string); ok && id != "" {
			seen[id] = struct{}{}
		}
	}
}

func removeOrphanToolResultBlocks(msg map[string]any, seen map[string]struct{}) (any, bool) {
	content, ok := msg["content"].([]any)
	if !ok {
		return msg, false
	}

	filtered := make([]any, 0, len(content))
	changed := false
	for _, block := range content {
		blockMap, ok := block.(map[string]any)
		if !ok {
			filtered = append(filtered, block)
			continue
		}
		blockType, _ := blockMap["type"].(string)
		if blockType != "tool_result" {
			filtered = append(filtered, block)
			continue
		}
		toolUseID, _ := blockMap["tool_use_id"].(string)
		if toolUseID == "" {
			filtered = append(filtered, block)
			continue
		}
		if _, ok := seen[toolUseID]; ok {
			filtered = append(filtered, block)
			continue
		}
		changed = true
	}
	if !changed {
		return msg, false
	}

	next, ok := cloneAny(msg).(map[string]any)
	if !ok {
		return msg, false
	}
	if len(filtered) == 0 {
		filtered = []any{map[string]any{"type": "text", "text": "(Earlier tool result omitted)"}}
	}
	next["content"] = filtered
	return next, true
}

// ========================
// Body 中 messages 字段替换
// ========================

// replaceArrayFieldInBody 将数组替换到请求体的指定字段
func replaceArrayFieldInBody(body []byte, fieldName string, arr []any) ([]byte, error) {
	arrJSON, err := json.Marshal(arr)
	if err != nil {
		return nil, fmt.Errorf("marshal array: %w", err)
	}
	newBody, err := sjson.SetRawBytes(body, fieldName, arrJSON)
	if err != nil {
		return nil, fmt.Errorf("set %s in body: %w", fieldName, err)
	}
	return newBody, nil
}

// replaceMessagesInBody 将压缩后的消息数组写回请求体的 messages 字段
func replaceMessagesInBody(body []byte, messages []any) ([]byte, error) {
	return replaceArrayFieldInBody(body, "messages", messages)
}

// ========================
// ParsedRequest 助手
// ========================

// CompressAnthropicParsedRequest 对 ParsedRequest 进行上下文压缩
// 压缩后同时更新 Body 和 Messages 字段
func (s *ContextCompressionService) CompressAnthropicParsedRequest(parsed *ParsedRequest) bool {
	newBody, compressed := s.CompressAnthropicBody(
		parsed.Body,
		parsed.Messages,
		parsed.Model,
		PlatformAnthropic,
	)
	if !compressed {
		return false
	}

	parsed.Body = newBody
	// 同时更新 Messages 字段（从压缩后的 body 中重新提取）
	if msgs := extractMessagesFromBody(newBody); msgs != nil {
		parsed.Messages = msgs
	}
	return true
}

// CompressAnthropicParsedRequestForGroup 使用分组级覆盖参数对 ParsedRequest 进行上下文压缩。
func (s *ContextCompressionService) CompressAnthropicParsedRequestForGroup(parsed *ParsedRequest, group *Group) bool {
	if group == nil || !group.ContextCompressionEnabled {
		return false
	}
	newBody, compressed := s.CompressAnthropicBodyForGroup(
		parsed.Body,
		parsed.Messages,
		parsed.Model,
		PlatformAnthropic,
		group,
	)
	if !compressed {
		return false
	}

	parsed.Body = newBody
	if msgs := extractMessagesFromBody(newBody); msgs != nil {
		parsed.Messages = msgs
	}
	return true
}

// extractMessagesFromBody 从请求体中提取 messages 数组
func extractMessagesFromBody(body []byte) []any {
	var req struct {
		Messages []any `json:"messages"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil
	}
	return req.Messages
}

// CountMessageTokens 导出 token 估算函数，供外部使用
func (s *ContextCompressionService) CountMessageTokens(messages []any) int {
	return estimateTokensFromMessagesSlice(messages)
}

// ========================
// Summarize 策略
// ========================

// summarizeOldMessages 将旧消息压缩为紧凑的对话摘要文本
// 适用于 Anthropic 和 Chat Completions 格式
func summarizeOldMessages(messages []any, maxTokens ...int) string {
	if len(messages) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("[Earlier conversation summary]\n")

	for _, msg := range messages {
		msgMap, ok := msg.(map[string]any)
		if !ok {
			continue
		}

		role, _ := msgMap["role"].(string)
		content := msgMap["content"]

		switch role {
		case "user":
			sb.WriteString("User: ")
			writeCompactContent(&sb, content)
		case "assistant":
			sb.WriteString("Assistant: ")
			writeCompactContent(&sb, content)
		case "system":
			sb.WriteString("System: ")
			writeCompactContent(&sb, content)
		case "tool":
			sb.WriteString("Tool output: ")
			writeCompactContent(&sb, content)
		default:
			sb.WriteString(fmt.Sprintf("%s: ", role))
			writeCompactContent(&sb, content)
		}
		sb.WriteString("\n")
	}

	sb.WriteString("[/Earlier conversation summary]")
	summary := sb.String()
	if len(maxTokens) > 0 && maxTokens[0] > 0 {
		return truncateTextToEstimatedTokens(summary, maxTokens[0])
	}
	return summary
}

// writeCompactContent 将消息内容写为紧凑文本
func writeCompactContent(sb *strings.Builder, content any) {
	switch c := content.(type) {
	case string:
		sb.WriteString(truncateText(c, 500))
	case []any:
		for i, block := range c {
			if i > 0 {
				sb.WriteString(" ")
			}
			blockMap, ok := block.(map[string]any)
			if !ok {
				continue
			}
			blockType, _ := blockMap["type"].(string)
			switch blockType {
			case "text":
				if txt, ok := blockMap["text"].(string); ok {
					sb.WriteString(truncateText(txt, 300))
				}
			case "tool_use":
				name, _ := blockMap["name"].(string)
				sb.WriteString(fmt.Sprintf("[tool:%s]", name))
			case "tool_result":
				sb.WriteString("[tool_result]")
			case "image":
				sb.WriteString("[image]")
			case "image_url":
				sb.WriteString("[image_url]")
			default:
				sb.WriteString(fmt.Sprintf("[%s]", blockType))
			}
		}
	default:
		if content != nil {
			sb.WriteString(truncateText(fmt.Sprintf("%v", content), 200))
		}
	}
}

// truncateText 截断文本到指定最大字符数
func truncateText(text string, maxChars int) string {
	if maxChars <= 0 {
		return ""
	}
	if utf8.RuneCountInString(text) <= maxChars {
		return text
	}
	for idx := range text {
		if maxChars == 0 {
			return text[:idx] + "..."
		}
		maxChars--
	}
	return text
}

func truncateTextToEstimatedTokens(text string, maxTokens int) string {
	if maxTokens <= 0 || estimateTokensFromString(text) <= maxTokens {
		return text
	}
	runes := []rune(text)
	lo, hi := 0, len(runes)
	for lo < hi {
		mid := (lo + hi + 1) / 2
		candidate := string(runes[:mid]) + "..."
		if estimateTokensFromString(candidate) <= maxTokens {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	if lo <= 0 {
		return "..."
	}
	return string(runes[:lo]) + "..."
}

// buildSummarySystemMessage 构建包含摘要的 system 消息
func buildSummarySystemMessage(summary string) map[string]any {
	return map[string]any{
		"role":    "user",
		"content": summary,
	}
}

// buildSummaryAnthropicMessage 构建 Anthropic 格式的摘要消息
func buildSummaryAnthropicMessage(summary string) map[string]any {
	return map[string]any{
		"role": "user",
		"content": []any{
			map[string]any{"type": "text", "text": summary},
		},
	}
}

// ========================
// Chat Completions & Responses 格式支持
// ========================

// CompressChatCompletionsBody 对 Chat Completions 格式的请求体进行上下文压缩
func (s *ContextCompressionService) CompressChatCompletionsBody(body []byte, model, platform string) ([]byte, bool) {
	return s.compressBodyField(body, model, platform, "messages", nil)
}

// CompressChatCompletionsBodyForGroup 使用分组级覆盖参数对 Chat Completions 请求体进行上下文压缩。
func (s *ContextCompressionService) CompressChatCompletionsBodyForGroup(body []byte, model, platform string, group *Group) ([]byte, bool) {
	if group == nil || !group.ContextCompressionEnabled {
		return body, false
	}
	return s.compressBodyField(body, model, platform, "messages", contextCompressionOptionsFromGroup(group))
}

// CompressResponsesBody 对 OpenAI Responses 格式的请求体进行上下文压缩
func (s *ContextCompressionService) CompressResponsesBody(body []byte, model, platform string) ([]byte, bool) {
	return s.compressBodyField(body, model, platform, "input", nil)
}

// CompressResponsesBodyForGroup 使用分组级覆盖参数对 OpenAI Responses 请求体进行上下文压缩。
func (s *ContextCompressionService) CompressResponsesBodyForGroup(body []byte, model, platform string, group *Group) ([]byte, bool) {
	if group == nil || !group.ContextCompressionEnabled {
		return body, false
	}
	return s.compressBodyField(body, model, platform, "input", contextCompressionOptionsFromGroup(group))
}

// compressBodyField 通用压缩方法，对指定字段名（messages 或 input）的数组进行截断
func (s *ContextCompressionService) compressBodyField(body []byte, model, platform, fieldName string, options *ContextCompressionOptions) ([]byte, bool) {
	if !s.IsEnabled(platform, model) {
		return body, false
	}

	messages := extractArrayFieldFromBody(body, fieldName)
	if messages == nil {
		return body, false
	}

	resolved := s.resolveOptions(options)
	budget := s.resolveRequestBudget(body, fieldName, model, messages, resolved)
	if budget.TotalTokens <= budget.TriggerTokens {
		return body, false
	}

	var newMessages []any
	var compressed bool
	switch resolved.Strategy {
	case config.CompressionStrategySummarize:
		newMessages, compressed = truncateCCAndSummarize(messages, resolved.KeepLastMessages, budget.KeepTokens)
	default:
		newMessages, compressed = truncateCCMessages(messages, resolved.KeepLastMessages, budget.KeepTokens)
	}
	if !compressed {
		return body, false
	}

	newBody, err := replaceArrayFieldInBody(body, fieldName, newMessages)
	if err != nil {
		slog.Warn("context_compression: failed to replace messages in body", "field", fieldName, "error", err)
		return body, false
	}

	slog.Info("context_compression: messages compressed",
		"strategy", resolved.Strategy,
		"field", fieldName,
		"original_tokens", budget.TotalTokens,
		"message_tokens", budget.MessageTokens,
		"static_tokens", budget.StaticTokens,
		"original_count", len(messages),
		"compressed_count", len(newMessages),
	)

	return newBody, true
}

// extractArrayFieldFromBody 从请求体提取指定字段的数组
func extractArrayFieldFromBody(body []byte, fieldName string) []any {
	result := gjson.GetBytes(body, fieldName)
	if !result.Exists() || !result.IsArray() {
		return nil
	}
	var arr []any
	if err := json.Unmarshal([]byte(result.Raw), &arr); err != nil {
		return nil
	}
	return arr
}

// extractChatMessagesFromBody 从 Chat Completions 请求体提取 messages 数组
func extractChatMessagesFromBody(body []byte) []any {
	return extractArrayFieldFromBody(body, "messages")
}

// ========================
// Summarize 策略：截断 + 摘要
// ========================

// truncateAndSummarize Anthropic 格式：截断旧消息并用摘要替代
func truncateAndSummarize(messages []any, keepLast, keepTokens int) ([]any, bool) {
	if len(messages) <= keepLast {
		total := estimateTokensFromMessagesSlice(messages)
		if total <= keepTokens {
			return messages, false
		}
	}

	// 找到截断点
	cutIdx := len(messages)
	keptTokens := 0
	keptCount := 0

	for i := len(messages) - 1; i >= 0; i-- {
		msgTokens := estimateTokensFromValue(messages[i])
		if keptCount >= keepLast {
			break
		}
		if keptTokens+msgTokens > keepTokens && keptCount > 0 {
			break
		}
		keptTokens += msgTokens
		keptCount++
		cutIdx = i
	}

	if cutIdx >= len(messages) {
		return messages, false
	}
	if cutIdx == 0 {
		shrunk, changed := shrinkMessagesToTokenBudget(messages, keepTokens)
		if changed {
			return ensureAnthropicStartsWithUser(shrunk), true
		}
		return messages, false
	}

	recentMsgs := messages[cutIdx:]
	oldMsgs := messages[:cutIdx]

	// 生成旧消息摘要
	summary := summarizeOldMessages(oldMsgs, summaryTokenBudget(keepTokens))
	if summary == "" {
		return messages, false
	}

	// 构建结果：摘要 + 最近消息
	result := make([]any, 0, 1+len(recentMsgs))
	result = append(result, buildSummaryAnthropicMessage(summary))
	result = append(result, recentMsgs...)
	result, _ = sanitizeToolContinuity(result)
	result = ensureAnthropicStartsWithUser(result)
	return result, true
}

// truncateCCAndSummarize Chat Completions 格式：截断旧消息并用摘要替代
func truncateCCAndSummarize(messages []any, keepLast, keepTokens int) ([]any, bool) {
	if len(messages) <= keepLast {
		total := estimateTokensFromMessagesSlice(messages)
		if total <= keepTokens {
			return messages, false
		}
	}

	// 提取 system 消息（始终保留）
	var systemMsgs []any
	var nonSystemMsgs []any
	for _, msg := range messages {
		msgMap, ok := msg.(map[string]any)
		if ok {
			role, _ := msgMap["role"].(string)
			if role == "system" {
				systemMsgs = append(systemMsgs, msg)
				continue
			}
		}
		nonSystemMsgs = append(nonSystemMsgs, msg)
	}

	// 在非 system 消息中找截断点
	cutIdx := len(nonSystemMsgs)
	keptTokens := estimateTokensFromMessagesSlice(systemMsgs)
	keptCount := 0

	for i := len(nonSystemMsgs) - 1; i >= 0; i-- {
		msgTokens := estimateTokensFromValue(nonSystemMsgs[i])
		if keptCount >= keepLast {
			break
		}
		if keptTokens+msgTokens > keepTokens && keptCount > 0 {
			break
		}
		keptTokens += msgTokens
		keptCount++
		cutIdx = i
	}

	if cutIdx >= len(nonSystemMsgs) {
		return messages, false
	}
	if cutIdx == 0 {
		shrunk, changed := shrinkMessagesToTokenBudget(messages, keepTokens)
		if changed {
			return ensureChatStartsAfterSystem(shrunk), true
		}
		return messages, false
	}

	recentMsgs := nonSystemMsgs[cutIdx:]
	oldMsgs := nonSystemMsgs[:cutIdx]

	summary := summarizeOldMessages(oldMsgs, summaryTokenBudget(keepTokens))
	if summary == "" {
		return messages, false
	}

	// 构建结果：system + 摘要（作为 system 消息）+ 最近消息
	result := make([]any, 0, len(systemMsgs)+1+len(recentMsgs))
	result = append(result, systemMsgs...)
	result = append(result, buildSummarySystemMessage(summary))
	result = append(result, recentMsgs...)
	result, _ = sanitizeToolContinuity(result)
	result = ensureChatStartsAfterSystem(result)
	return result, true
}

func summaryTokenBudget(keepTokens int) int {
	if keepTokens <= 0 {
		return 256
	}
	budget := keepTokens / 4
	if budget < 128 {
		budget = 128
	}
	if budget > 4096 {
		budget = 4096
	}
	return budget
}

// truncateCCMessages Chat Completions 格式的消息截断
// 与 Anthropic 格式的主要区别：system 消息需要特殊保留
func truncateCCMessages(messages []any, keepLast, keepTokens int) ([]any, bool) {
	if len(messages) <= keepLast {
		total := estimateTokensFromMessagesSlice(messages)
		if total <= keepTokens {
			return messages, false
		}
	}

	// 提取 system 消息（始终保留）
	var systemMsgs []any
	var nonSystemMsgs []any
	for _, msg := range messages {
		msgMap, ok := msg.(map[string]any)
		if ok {
			role, _ := msgMap["role"].(string)
			if role == "system" {
				systemMsgs = append(systemMsgs, msg)
				continue
			}
		}
		nonSystemMsgs = append(nonSystemMsgs, msg)
	}

	// 对非 system 消息进行截断
	cutIdx := len(nonSystemMsgs)
	keptTokens := estimateTokensFromMessagesSlice(systemMsgs)
	keptCount := 0

	for i := len(nonSystemMsgs) - 1; i >= 0; i-- {
		msgTokens := estimateTokensFromValue(nonSystemMsgs[i])
		if keptCount >= keepLast {
			break
		}
		if keptTokens+msgTokens > keepTokens && keptCount > 0 {
			break
		}
		keptTokens += msgTokens
		keptCount++
		cutIdx = i
	}

	if cutIdx >= len(nonSystemMsgs) {
		return messages, false
	}
	if cutIdx == 0 {
		shrunk, changed := shrinkMessagesToTokenBudget(messages, keepTokens)
		if changed {
			shrunk, _ = sanitizeToolContinuity(shrunk)
			return ensureChatStartsAfterSystem(shrunk), true
		}
		return messages, false
	}

	kept := nonSystemMsgs[cutIdx:]

	// 确保第一条消息是 user
	if len(kept) > 0 {
		firstMsg, ok := kept[0].(map[string]any)
		if ok {
			role, _ := firstMsg["role"].(string)
			if role == "assistant" || role == "tool" {
				placeholder := map[string]any{
					"role":    "user",
					"content": "(Earlier conversation omitted)",
				}
				kept = append([]any{placeholder}, kept...)
			}
		}
	}

	// 合并 system + 截断后的消息
	result := make([]any, 0, len(systemMsgs)+len(kept))
	result = append(result, systemMsgs...)
	result = append(result, kept...)
	result, _ = sanitizeToolContinuity(result)
	result = ensureChatStartsAfterSystem(result)
	return result, true
}

// ShouldCompress 检查消息列表是否需要压缩
func (s *ContextCompressionService) ShouldCompress(messages []any, model, platform string) bool {
	if !s.IsEnabled(platform, model) {
		return false
	}
	total := estimateTokensFromMessagesSlice(messages)
	trigger := s.cfg.TriggerTokens
	if trigger <= 0 {
		trigger = 64000
	}
	return total > trigger
}
