package EVE

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lgzzzz/evekhl/pkg/khl"
	slog "github.com/lgzzzz/evekhl/pkg/log"
	"github.com/phuslu/log"
)

type eveBot struct {
	logger     log.Logger
	botSession *khl.Session

	guild *khl.Guild
}

type Option struct {
	WelcomeMessage string
}

func NewEveBot(token string, o *Option) *eveBot {
	logger := log.Logger{
		Level:  log.TraceLevel,
		Writer: &log.ConsoleWriter{},
	}
	e := &eveBot{
		logger:     logger,
		botSession: khl.New(token, slog.NewLogger(&logger)),
	}

	if o != nil {
		WelcomeMessage = o.WelcomeMessage
	}

	return e

}

func (e *eveBot) Run() {

	e.botSession.AddHandler(e.welcomeMember)

	err := e.botSession.Run()
	if err != nil {
		panic(err)
	}

	e.initGuildInfo()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc, os.Interrupt, syscall.SIGTERM)
	<-sc

	_ = e.botSession.Close()

	_ = e.botSession.UserOffline()
}

func (e *eveBot) initGuildInfo() {
	list, _, err := e.botSession.GuildList(nil)
	if err != nil {
		panic("init GuildInfo fail")
	}
	if len(list) > 1 {
		panic("got joined more than one guild")
	}
	e.guild = list[0]
}

func (e *eveBot) welcomeMember(ctx *khl.GuildMemberAddContext) {

	time.Sleep(5 * time.Second)

	name := fmt.Sprintf("(met)%s(met)", ctx.Extra.UserID)
	content := fmt.Sprintf(WelcomeMessage, name)
	_, _ = ctx.Session.MessageCreate(&khl.MessageCreate{
		MessageCreateBase: khl.MessageCreateBase{
			TargetID: e.guild.DefaultChannelID,
			Content:  content,
			Type:     khl.MessageTypeCard,
		},
	})
}
