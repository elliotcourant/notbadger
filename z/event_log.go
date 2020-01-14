package z

import "golang.org/x/net/trace"

var (
	NoEventLog trace.EventLog = nilEventLog{}
)

type nilEventLog struct{}

func (nel nilEventLog) Printf(format string, a ...interface{}) {}

func (nel nilEventLog) Errorf(format string, a ...interface{}) {}

func (nel nilEventLog) Finish() {}
