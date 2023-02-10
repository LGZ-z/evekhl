package khl

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// ErrWSAlreadyOpen is the error when connecting with connected websocket.
var ErrWSAlreadyOpen = errors.New("websocket is already opened")

// Run starts a websocket connection. It does not block the function.
func (s *Session) Run() (err error) {

	s.Lock()
	defer s.Unlock()

	if s.wsConn != nil {
		s.Logger.Error().Caller(3).Msg("websocket is already open")
		return ErrWSAlreadyOpen
	}

	if s.gateway == "" {
		s.gateway, err = s.Gateway()
		if err != nil {
			s.Logger.Error().Caller(3).Err("err", err).Msg("unable to get ws gateway")
			return
		}
	}

	//s.log(LogInfo, "connecting to gateway %s", s.gateway)
	s.Logger.Info().Caller(3).Str("gateway_url", s.gateway).Msg("connecting to gateway")
	s.wsConn, _, err = websocket.DefaultDialer.Dial(s.gateway, http.Header{})
	if err != nil {
		s.Logger.Error().Caller(3).
			Str("gateway_url", s.gateway).
			Err("err", err).
			Msg("error connecting to gateway")
		//s.log(LogError, "error connecting to gateway %s, %s", s.gateway, err.Error())
		s.gateway = ""
		s.wsConn = nil
		return
	}
	s.wsConn.SetCloseHandler(func(code int, text string) error {
		return nil
	})

	defer func() {
		if err != nil {
			s.wsConn.Close()
			s.wsConn = nil
		}
	}()
	mt, m, err := s.wsConn.ReadMessage()
	if err != nil {
		s.Logger.Error().Caller(3).Err("err", err).Msg("error reading message from websocket")
		return
	}
	e, err := s.onEvent(mt, m)
	if err != nil {
		s.Logger.Error().Caller(3).Err("err", err).Msg("error parsing event")
		return
	}
	if e.Signal != EventSignalHello {
		s.gateway = ""
		err = fmt.Errorf("expecting signal hello, got singal %d", e.Signal)
		return err
	}
	s.LastHeartbeatAck = time.Now().UTC()

	var h EventDataHello
	if err = json.Unmarshal(e.Data, &h); err != nil {
		s.Logger.Error().Caller(3).Err("err", err).Msg("error unmarshalling hello")
		err = fmt.Errorf("error unmarshalling hello, %s", err.Error())
		return
	}
	if h.Code != EventStatusOk {
		s.gateway = ""
		s.Logger.Error().Caller(3).Int("code", int(h.Code)).Msg("error status is not ok")
		err = fmt.Errorf("expecting status ok, received %d", h.Code)
		return
	}

	s.listening = make(chan interface{})
	go s.heartbeat(s.wsConn, s.listening)
	go s.listen(s.wsConn, s.listening)

	return
}

func (s *Session) onEvent(messageType int, message []byte) (e *Event, err error) {
	var reader io.Reader
	reader = bytes.NewBuffer(message)

	if messageType == websocket.BinaryMessage {
		z, err2 := zlib.NewReader(reader)
		if err2 != nil {
			s.Logger.Error().Caller(3).Err("err", err).Msg("error decompressing websocket message")
			//s.log(LogError, "error uncompressing websocket message, %s", err)
			return nil, err2
		}
		defer func() {
			err3 := z.Close()
			if err3 != nil {
				s.Logger.Warn().Caller(3).Err("err", err3).Msg("error closing zlib")
				//s.log(LogWarning, "error closing zlib, %s", err)
			}
		}()

		reader = z
	}

	jsonDecoder := json.NewDecoder(reader)
	if err = jsonDecoder.Decode(&e); err != nil {
		s.Logger.Error().Caller(3).Err("err", err).Msg("error decoding websocket message")
		return
	}

	s.Logger.Debug().Caller(3).Int("signal", int(e.Signal)).Int64("seq", e.SequenceNumber).Bytes("data", e.Data).Msg("received event")

	if e.Signal == EventSignalHello {
		return
	}

	if e.Signal == EventSignalPong {
		s.Lock()
		s.LastHeartbeatAck = time.Now().UTC()
		s.Unlock()
		return
	}

	if e.Signal == EventSignalReconnect {
		s.Logger.Info().Caller(3).Msg("closing current ws and reconnecting in response to Reconnect signal")
		//s.log(LogInfo, "closing current ws and reconnecting in response to Reconnect signal")
		s.CloseWithCode(websocket.CloseServiceRestart)
		s.Lock()
		s.gateway = ""
		atomic.StoreInt64(s.sequence, 0)
		s.snStore.Clear()
		s.Unlock()
		s.reconnect()
		return
	}

	if e.Signal == EventSignalResumeAck {
		s.Logger.Info().Caller(3).Msg("all missing message are sent, received Resume Ack signal")
		//s.log(LogInfo, "all missing message are sent, received Resume Ack signal")
		return
	}

	if e.Signal != EventSignalEvent {
		s.Logger.Error().Caller(3).Int("signal", int(e.Signal)).Int64("seq", e.SequenceNumber).Bytes("data", e.Data).Msg("unknown signal")
		//s.log(LogError, "unknown signal: %d, seq: %d, data: %s", e.Signal, e.SequenceNumber, string(e.Data))
		return
	}

	atomic.AddInt64(s.sequence, 1)
	var exist bool
	func() {
		s.snStore.Lock()
		defer s.snStore.Unlock()
		exist = s.snStore.TestAndInsert(e.SequenceNumber)
	}()
	if exist && e.SequenceNumber != 0 {
		return nil, nil
	}
	data := EventData{}

	if err = json.Unmarshal(e.Data, &data); err != nil {
		s.Logger.Error().Caller(3).Int("signal", int(e.Signal)).Int64("seq", e.SequenceNumber).Bytes("data", e.Data).Err("err", err).Msg("unmarshal event data error")

		//s.log(LogError, "unmarshal event data error: %s\nsignal: %d, seq: %d, data: %s", err, e.Signal, e.SequenceNumber, string(e.Data))
		return
	}
	if data.Type == MessageTypeSystem {
		if data.ChannelType == "WEBHOOK_CHALLENGE" {
			return e, errWebhookVerify
		}
		sys := EventDataSystem{}
		if err = json.Unmarshal(data.Extra, &sys); err != nil {
			s.Logger.Error().Caller(3).Int("signal", int(e.Signal)).Int64("seq", e.SequenceNumber).Bytes("data", e.Data).Err("err", err).Msg("unmarshal system event extra.body error")
			//s.log(LogError, "unmarshal system event extra.body error: %s\nsignal: %d, seq: %d, data: %s", err, e.Signal, e.SequenceNumber, string(e.Data))
			return
		}
		if eh, ok := registeredEventHandler[sys.Type]; ok {
			t := eh.New()
			ex := t.GetExtra()
			if err = json.Unmarshal(sys.Body, ex); err != nil {
				s.Logger.Error().Caller(3).Int("signal", int(e.Signal)).Int64("seq", e.SequenceNumber).Bytes("data", e.Data).Err("err", err).Msg("unmarshal extra error")
				//s.log(LogError, "unmarshal extra error: %s\nsignal: %d, seq: %d, data: %s", err, e.Signal, e.SequenceNumber, string(e.Data))
			}
			s.handleEvent(eh.Type(), data.EventDataGeneral, t)
		} else {
			s.Logger.Warn().Caller(3).Int("signal", int(e.Signal)).Int64("seq", e.SequenceNumber).Bytes("data", e.Data).Msg("unknown system message event")
			//s.log(LogWarning, "unknown system message event: signal: %d, seq: %d, data: %s", e.Signal, e.SequenceNumber, string(e.Data))
		}
	} else {
		if eh, ok := registeredEventHandler[strconv.Itoa(int(data.Type))]; ok {
			t := eh.New()
			ex := t.GetExtra()
			if err = json.Unmarshal(data.Extra, ex); err != nil {
				s.Logger.Error().Caller(3).Int("signal", int(e.Signal)).Int64("seq", e.SequenceNumber).Bytes("data", e.Data).Err("err", err).Msg("unmarshal extra error")

				//s.log(LogError, "unmarshal extra error: %s\nsignal: %d, seq: %d, data: %s", err, e.Signal, e.SequenceNumber, string(e.Data))
			}
			s.handleEvent(eh.Type(), data.EventDataGeneral, t)
		} else {
			s.Logger.Warn().Caller(3).Int("signal", int(e.Signal)).Int64("seq", e.SequenceNumber).Bytes("data", e.Data).Msg("unknown message event")
			//s.log(LogWarning, "unknown system message event: signal: %d, seq: %d, data: %s", e.Signal, e.SequenceNumber, string(e.Data))
		}
	}

	return nil, err
}

func (s *Session) listen(wsConn *websocket.Conn, listening <-chan interface{}) {

	for {
		messageType, message, err := wsConn.ReadMessage()
		if err != nil {
			s.RLock()
			sameConnection := s.wsConn == wsConn
			s.RUnlock()

			if sameConnection {
				s.Logger.Warn().Caller(3).Str("gateway_url", s.gateway).Err("err", err).Msg("error reading from gateway")
				//s.log(LogWarning, "error reading from gateway %s websocket, %s", s.gateway, err)
				err := s.Close()
				if err != nil {
					s.Logger.Warn().Caller(3).Err("err", err).Msg("error closing session connection")
					//s.log(LogWarning, "error closing session connection, %s", err)
				}
				s.Logger.Info().Caller(3).Msg("calling reconnect")
				//s.log(LogInfo, "calling reconnect now.")
				s.reconnect()
			}
			return
		}
		select {
		case <-listening:
			return
		default:
			s.onEvent(messageType, message)
		}
	}
}

type pingSignal struct {
	Signal         EventSignal `json:"s"`
	SequenceNumber int64       `json:"sn"`
}

func (s *Session) heartbeat(wsConn *websocket.Conn, listening <-chan interface{}) {
	if listening == nil || wsConn == nil {
		return
	}
	var err error
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		s.RLock()
		last := s.LastHeartbeatAck
		s.RUnlock()
		sequence := atomic.LoadInt64(s.sequence)
		//s.log(LogDebug, "sending gateway websocket heartbeat seq %d", sequence)
		s.Logger.Debug().Caller(3).Int64("seq", sequence).Msg("sending gateway websocket heartbeat")

		s.wsMutex.Lock()
		s.LastHeartbeatSent = time.Now().UTC()
		err = wsConn.WriteJSON(pingSignal{
			Signal:         EventSignalPing,
			SequenceNumber: sequence,
		})
		s.wsMutex.Unlock()
		if err != nil || time.Now().UTC().Sub(last) > 36*time.Second {
			if err != nil {
				s.Logger.Error().Caller(3).Str("gateway_url", s.gateway).Err("err", err).Msg("error sending heartbeat to gateway")
				//s.log(LogError, "error sending heartbeat to gateway %s, %s", s.gateway, err)
			} else {
				s.Logger.Error().Caller(3).Dur("latency", time.Now().UTC().Sub(last)).Msg("ACK not received, reconnect")
				//s.log(LogError, "haven't gotten a heartbeat ACK in %v, triggering a reconnection", time.Now().UTC().Sub(last))
			}
			s.Close()
			s.reconnect()
		}
		select {
		case <-ticker.C:
		case <-listening:
			return
		}

	}
}

func (s *Session) reconnect() {
	s.Logger.Info().Caller(3).Msg("called")
	//s.log(LogInfo, "called")
	var err error
	wait := time.Duration(1)
	for {
		//s.log(LogInfo, "trying to reconnect to gateway")
		s.Logger.Info().Caller(3).Msg("trying to reconnect to gateway")
		err = s.Run()
		if err == nil {
			s.Logger.Info().Caller(3).Msg("successfully reconnected to gateway")
			//s.log(LogInfo, "successfully reconnected to gateway")
			return
		}

		if err == ErrWSAlreadyOpen {
			s.Logger.Info().Caller(3).Msg("websocket already exists, no need to reconnect")
			//s.log(LogInfo, "websocket already exists, no need to reconnect")
			return
		}

		s.Logger.Error().Caller(3).Err("err", err).Msg("error reconnecting to gateway")
		//s.log(LogError, "error reconnecting to gateway, %s", err)
		<-time.After(wait * time.Second)
		wait *= 2
		if wait > 600 {
			wait = 600
		}
	}
}

// Close closes a websocket connection.
func (s *Session) Close() (err error) {
	return s.CloseWithCode(websocket.CloseNormalClosure)
}

// CloseWithCode closes a websocket connection with custom websocket closing code.
func (s *Session) CloseWithCode(code int) (err error) {
	s.Lock()
	if s.listening != nil {
		//s.log(LogInfo, "closing listening channel")
		s.Logger.Info().Caller(3).Msg("closing listening channel")
		close(s.listening)
		s.listening = nil
	}
	if s.wsConn != nil {
		s.Logger.Info().Caller(3).Msg("sending close frame")
		s.wsMutex.Lock()
		err := s.wsConn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(code, ""))
		s.wsMutex.Unlock()
		if err != nil {
			//s.log(LogInfo, "error closing websocket, %s", err)
			s.Logger.Info().Caller(3).Err("err", err).Msg("error closing websocket")
		}

		time.Sleep(1 * time.Second)
		s.Logger.Info().Caller(3).Msg("closing gateway websocket")
		//s.log(LogInfo, "closing gateway websocket")
		err = s.wsConn.Close()
		if err != nil {
			//s.log(LogInfo, "error closing websocket, %s", err)
			s.Logger.Info().Caller(3).Err("err", err).Msg("error closing websocket")
		}
		s.wsConn = nil
	}
	s.Unlock()
	return
}
