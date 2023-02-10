package khl

import (
	"bytes"
	"compress/zlib"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
)

var errWebhookVerify = errors.New("web")

// WebhookHandler provides a http.HandlerFunc for webhook.
func (s *Session) WebhookHandler() http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		defer request.Body.Close()
		var err error
		s.Logger.Trace().Caller(3).Msg("new request")
		if request.Method != "POST" {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		r := request.Body
		defer r.Close()
		buf := &bytes.Buffer{}
		if !strings.Contains(request.RequestURI, "compress=0") {
			r, err = zlib.NewReader(r)
			if err != nil {
				s.Logger.Error().Err("error", err).Caller(3).Msg("error in init zlib")
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}
			defer r.Close()
		}
		_, err = buf.ReadFrom(r)
		if err != nil {
			s.Logger.Error().Err("error", err).Caller(3).Msg("error in reading body")
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		if s.Identify.WebsocketKey != nil {
			e := &struct {
				Encrypt string `json:"encrypt"`
			}{}
			err = json.NewDecoder(buf).Decode(e)
			if err != nil {
				s.Logger.Error().Err("error", err).Caller(3).Msg("error in parsing encrypted request")
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}
			base64Reader := base64.NewDecoder(base64.StdEncoding, strings.NewReader(e.Encrypt))
			newBuf := &bytes.Buffer{}
			_, err = newBuf.ReadFrom(base64Reader)
			if err != nil {
				s.Logger.Error().Err("error", err).Caller(3).Msg("error in decoding base64")
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}
			c, err := aes.NewCipher(s.Identify.WebsocketKey)
			if err != nil {
				s.Logger.Error().Err("error", err).Caller(3).Msg("error in creating cipher")
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}
			dec := cipher.NewCBCDecrypter(c, newBuf.Bytes()[:16])
			payloadReader := base64.NewDecoder(base64.StdEncoding, bytes.NewReader(newBuf.Bytes()[16:]))
			buf.Reset()
			buf.ReadFrom(payloadReader)
			dec.CryptBlocks(buf.Bytes(), buf.Bytes())
		}
		e, err := s.onEvent(websocket.TextMessage, buf.Bytes())
		if err == errWebhookVerify {
			i := &struct {
				Type        int    `json:"type"`
				ChannelType string `json:"channel_type"`
				Challenge   string `json:"challenge"`
				VerifyToken string `json:"verify_token"`
			}{}
			err = json.Unmarshal(e.Data, i)
			if err != nil {
				s.Logger.Error().Err("error", err).Caller(3).Msg("error in unmarshalling data")
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}
			if s.Identify.VerifyToken != "" && i.VerifyToken != s.Identify.VerifyToken {
				s.Logger.Warn().Err("error", err).Caller(3).Msg("received wrong data")
				writer.WriteHeader(http.StatusUnauthorized)
				return
			}
			_, err = writer.Write([]byte(`{"challenge":"` + i.Challenge + `"}`))
			if err != nil {
				s.Logger.Error().Err("error", err).Msg("error in writing to response")
				return
			}
			s.Logger.Info().Caller(3).Msg("webhook challenge done")
			return
		} else if err != nil {
			s.Logger.Error().Err("error", err).Caller(3).Msg("error in parsing event")
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}
