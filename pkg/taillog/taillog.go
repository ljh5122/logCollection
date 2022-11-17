package taillog

import (
	"fmt"
	"github.com/hpcloud/tail"
)

type TailLog struct {
	client *tail.Tail
}

func NewClient(logFilePath string) *TailLog {
	client, err := tail.TailFile(logFilePath, tail.Config{
		MustExist: false,
		ReOpen: true,
		Location: &tail.SeekInfo{Offset: 0,Whence: 2},
		Poll: true,
		Follow: true,
	})

	if err != nil {
		fmt.Println("TailLog NewClient fail, ", err)
	}

	return &TailLog{client: client}
}

func (receiver *TailLog) GetLogChan() <-chan *tail.Line {
	return receiver.client.Lines
}