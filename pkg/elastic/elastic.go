package elastic

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
)

type Elastic struct {
	client *elastic.Client
}

func NewElastic(addr, username, password string) *Elastic {
	client, err := elastic.NewClient(elastic.SetURL(addr), elastic.SetBasicAuth(username, password))
	if err != nil {
		fmt.Println("elastic NewClient fail, ", err)
	}

	return &Elastic{client: client}
}

func (receiver *Elastic) Put(index, bodyStr string)  {
	if _, err := receiver.client.IndexExists(index).Do(context.Background()); err != nil {
		fmt.Println("elastic IndexExists fail, ", err)
	}

	_, err := receiver.client.Index().Index(index).BodyString(bodyStr).Do(context.Background())
	if err != nil {
		fmt.Println("elastic put body fail, ", err)
		return
	}
}