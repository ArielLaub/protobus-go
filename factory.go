package protobus

import (
	"encoding/json"
	"fmt"
	"sync"
)

// RequestContainer holds a decoded request.
type RequestContainer struct {
	Method string                 `json:"method"`
	Data   map[string]interface{} `json:"data"`
	Actor  string                 `json:"actor,omitempty"`
}

// ResponseContainer holds a decoded response.
type ResponseContainer struct {
	Method string                 `json:"method"`
	Result map[string]interface{} `json:"result,omitempty"`
	Error  *ResponseError         `json:"error,omitempty"`
}

// ResponseError represents an error in a response.
type ResponseError struct {
	Message string `json:"message"`
	Code    string `json:"code"`
}

// EventContainer holds a decoded event.
type EventContainer struct {
	Type  string                 `json:"type"`
	Data  map[string]interface{} `json:"data"`
	Topic string                 `json:"topic,omitempty"`
}

// MessageFactory handles encoding/decoding of messages.
type MessageFactory struct {
	mu                   sync.RWMutex
	initialized          bool
	typeRegistry         *CustomTypeRegistry
	hasCustomTypesCache  map[string]bool
	protoSources         map[string]string
}

// NewMessageFactory creates a new MessageFactory.
func NewMessageFactory() *MessageFactory {
	return &MessageFactory{
		typeRegistry:        GetTypeRegistry(),
		hasCustomTypesCache: make(map[string]bool),
		protoSources:        make(map[string]string),
	}
}

// Init initializes the factory.
func (f *MessageFactory) Init() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.initialized = true
	logDebug("MessageFactory initialized")
	return nil
}

// IsInitialized returns whether the factory is initialized.
func (f *MessageFactory) IsInitialized() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.initialized
}

// Parse registers a proto source for a service.
func (f *MessageFactory) Parse(protoSource, serviceName string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.initialized {
		logWarn("MessageFactory not initialized, auto-initializing")
		f.initialized = true
	}

	f.protoSources[serviceName] = protoSource
	logDebug("Registered proto source for service: %s", serviceName)
}

// RegisterType registers a custom type.
func (f *MessageFactory) RegisterType(ct CustomType) {
	f.typeRegistry.Register(ct)
	logDebug("Registered custom type: %s", ct.Name())
}

// checkHasCustomTypes checks if data contains custom type fields.
func (f *MessageFactory) checkHasCustomTypes(data interface{}) bool {
	if data == nil {
		return false
	}

	switch v := data.(type) {
	case map[string]interface{}:
		for key, val := range v {
			if f.typeRegistry.IsCustomType(key) {
				return true
			}
			if f.checkHasCustomTypes(val) {
				return true
			}
		}
	case []interface{}:
		for _, item := range v {
			if f.checkHasCustomTypes(item) {
				return true
			}
		}
	}
	return false
}

// preprocessForEncode processes data before encoding.
func (f *MessageFactory) preprocessForEncode(data interface{}, typeName string) interface{} {
	if data == nil {
		return data
	}

	// Check cache
	if typeName != "" {
		f.mu.RLock()
		hasCustom, cached := f.hasCustomTypesCache[typeName]
		f.mu.RUnlock()

		if cached && !hasCustom {
			return data
		}

		if !cached {
			hasCustom = f.checkHasCustomTypes(data)
			f.mu.Lock()
			f.hasCustomTypesCache[typeName] = hasCustom
			f.mu.Unlock()

			if !hasCustom {
				return data
			}
		}
	}

	switch v := data.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{})
		for key, val := range v {
			if ct, ok := f.typeRegistry.Get(key); ok {
				encoded, err := ct.Encode(val)
				if err != nil {
					logWarn("Failed to encode custom type %s: %v", key, err)
					result[key] = val
				} else {
					result[key] = encoded
				}
			} else {
				result[key] = f.preprocessForEncode(val, "")
			}
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = f.preprocessForEncode(item, "")
		}
		return result
	}
	return data
}

// postprocessAfterDecode processes data after decoding.
func (f *MessageFactory) postprocessAfterDecode(data interface{}, typeName string) interface{} {
	if data == nil {
		return data
	}

	// Check cache
	if typeName != "" {
		f.mu.RLock()
		hasCustom, cached := f.hasCustomTypesCache[typeName]
		f.mu.RUnlock()

		if cached && !hasCustom {
			return data
		}
	}

	switch v := data.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{})
		for key, val := range v {
			if ct, ok := f.typeRegistry.Get(key); ok {
				decoded, err := ct.Decode(val)
				if err != nil {
					logWarn("Failed to decode custom type %s: %v", key, err)
					result[key] = val
				} else {
					result[key] = decoded
				}
			} else {
				result[key] = f.postprocessAfterDecode(val, "")
			}
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = f.postprocessAfterDecode(item, "")
		}
		return result
	}
	return data
}

// BuildRequest builds a request message.
func (f *MessageFactory) BuildRequest(method string, data map[string]interface{}, actor string) ([]byte, error) {
	if !f.initialized {
		return nil, ErrNotInitialized
	}

	envelope := map[string]interface{}{
		"method": method,
		"data":   f.preprocessForEncode(data, method),
	}
	if actor != "" {
		envelope["actor"] = actor
	}

	return json.Marshal(envelope)
}

// BuildResponse builds a response message.
func (f *MessageFactory) BuildResponse(method string, result interface{}, err error) ([]byte, error) {
	if !f.initialized {
		return nil, ErrNotInitialized
	}

	envelope := map[string]interface{}{
		"method": method,
	}

	if err != nil {
		code := "UNKNOWN_ERROR"
		if he, ok := GetHandledError(err); ok {
			code = he.Code
		}
		envelope["error"] = map[string]interface{}{
			"message": err.Error(),
			"code":    code,
		}
	} else {
		envelope["result"] = map[string]interface{}{
			"data": f.preprocessForEncode(result, method),
		}
	}

	return json.Marshal(envelope)
}

// BuildEvent builds an event message.
func (f *MessageFactory) BuildEvent(eventType string, data map[string]interface{}, topic string) ([]byte, error) {
	if !f.initialized {
		return nil, ErrNotInitialized
	}

	envelope := map[string]interface{}{
		"type": eventType,
		"data": f.preprocessForEncode(data, eventType),
	}
	if topic != "" {
		envelope["topic"] = topic
	}

	return json.Marshal(envelope)
}

// DecodeMessage decodes a raw message.
func (f *MessageFactory) DecodeMessage(data []byte) (map[string]interface{}, error) {
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("failed to decode message: %w", err)
	}
	return result, nil
}

// DecodeRequest decodes a request message.
func (f *MessageFactory) DecodeRequest(data []byte) (*RequestContainer, error) {
	decoded, err := f.DecodeMessage(data)
	if err != nil {
		return nil, err
	}

	method, _ := decoded["method"].(string)
	actor, _ := decoded["actor"].(string)

	var reqData map[string]interface{}
	if d, ok := decoded["data"].(map[string]interface{}); ok {
		reqData = f.postprocessAfterDecode(d, method).(map[string]interface{})
	}

	return &RequestContainer{
		Method: method,
		Data:   reqData,
		Actor:  actor,
	}, nil
}

// DecodeResponse decodes a response message.
func (f *MessageFactory) DecodeResponse(data []byte) (*ResponseContainer, error) {
	decoded, err := f.DecodeMessage(data)
	if err != nil {
		return nil, err
	}

	method, _ := decoded["method"].(string)

	resp := &ResponseContainer{
		Method: method,
	}

	if errObj, ok := decoded["error"].(map[string]interface{}); ok {
		resp.Error = &ResponseError{
			Message: fmt.Sprintf("%v", errObj["message"]),
			Code:    fmt.Sprintf("%v", errObj["code"]),
		}
	}

	if result, ok := decoded["result"].(map[string]interface{}); ok {
		if d, ok := result["data"].(map[string]interface{}); ok {
			resp.Result = f.postprocessAfterDecode(d, method).(map[string]interface{})
		} else {
			resp.Result = result
		}
	}

	return resp, nil
}

// DecodeEvent decodes an event message.
func (f *MessageFactory) DecodeEvent(data []byte) (*EventContainer, error) {
	decoded, err := f.DecodeMessage(data)
	if err != nil {
		return nil, err
	}

	eventType, _ := decoded["type"].(string)
	topic, _ := decoded["topic"].(string)

	var eventData map[string]interface{}
	if d, ok := decoded["data"].(map[string]interface{}); ok {
		eventData = f.postprocessAfterDecode(d, eventType).(map[string]interface{})
	}

	return &EventContainer{
		Type:  eventType,
		Data:  eventData,
		Topic: topic,
	}, nil
}
