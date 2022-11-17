package etcd

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/client/v3"
	"time"
)

type Client struct {
	client *clientv3.Client
}

const (
	EventTypeDelete = clientv3.EventTypePut
	EventTypePut    = clientv3.EventTypePut
	closeSendErrTimeout = 250 * time.Millisecond
)

func NewClient(addr ...string) *Client {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   addr,
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		fmt.Println("etcd client connect fail, error : ", err)
	}

	return &Client{client: client}
}

func (receiver *Client) Put(key string, value string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := receiver.client.Put(ctx, key, value)
	if err != nil {
		fmt.Printf("etcd put %v fail, %v \n", key, err)
	}

	defer cancel()
	fmt.Printf("etcd put %v success, %v \n", key, resp.Header.GetRevision())
}

func (receiver *Client) Get(key string) *clientv3.GetResponse {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := receiver.client.Get(ctx, key)
	if err != nil {
		fmt.Printf("etcd get %v fail, %v \n", key, err)
	}

	defer cancel()
	return resp
}

func (receiver *Client) Delete(key string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := receiver.client.Delete(ctx, key)
	if err != nil {
		fmt.Printf("etcd get %v fail, %v \n", key, err)
	}

	defer cancel()
	fmt.Printf("etcd delete %v success, %v \n", key, resp.Deleted)
}

func (receiver *Client) Watch(key string) clientv3.WatchChan {
	return receiver.client.Watch(context.Background(), key)
}