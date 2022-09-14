package bus_nats

import (
	"errors"
	"sync"
	"time"

	"github.com/chefsgo/bus"
	"github.com/nats-io/nats.go"
)

//------------------------- 默认队列驱动 begin --------------------------

type (
	natsBusDriver struct{}
	natsBusSub    struct {
		Name  string
		Group string
	}
	natsBusConnect struct {
		mutex   sync.RWMutex
		running bool
		actives int64

		name    string
		config  bus.Config
		setting natsBusSetting

		delegate bus.Delegate

		client *nats.Conn

		subs map[string]natsBusSub
	}

	natsBusSetting struct {
		Url string
	}
)

// 连接
func (driver *natsBusDriver) Connect(name string, config bus.Config) (bus.Connect, error) {
	//获取配置信息
	setting := natsBusSetting{
		Url: nats.DefaultURL,
	}

	if vv, ok := config.Setting["url"].(string); ok {
		setting.Url = vv
	}

	return &natsBusConnect{
		name: name, config: config, setting: setting,
		subs: make(map[string]natsBusSub, 0),
	}, nil
}

// 打开连接
func (connect *natsBusConnect) Open() error {
	client, err := nats.Connect(connect.setting.Url)
	if err != nil {
		return err
	}
	connect.client = client

	return nil
}
func (connect *natsBusConnect) Health() (bus.Health, error) {
	connect.mutex.RLock()
	defer connect.mutex.RUnlock()
	return bus.Health{Workload: connect.actives}, nil
}

// 关闭连接
func (connect *natsBusConnect) Close() error {
	if connect.client != nil {
		connect.client.Close()
	}
	return nil
}

func (connect *natsBusConnect) Accept(delegate bus.Delegate) error {
	connect.mutex.Lock()
	defer connect.mutex.Unlock()

	//绑定
	connect.delegate = delegate

	return nil
}

// 注册
func (connect *natsBusConnect) Register(name string) error {
	connect.mutex.Lock()
	defer connect.mutex.Unlock()
	connect.subs[name] = natsBusSub{name, name}
	return nil
}

// 开始
func (connect *natsBusConnect) Start() error {
	connect.mutex.Lock()
	defer connect.mutex.Unlock()

	nc := connect.client

	//监听队列
	for subKey, subVal := range connect.subs {
		key := subKey
		nc.QueueSubscribe(key, subVal.Group, func(msg *nats.Msg) {
			connect.delegate.Serve(key, msg.Data, func(data []byte, err error) {
				if msg.Reply != "" {
					if err == nil && data != nil {
						msg.Respond(data)
					} else {
						//返回空数据
						msg.Respond(make([]byte, 0))
					}
				}
			})
		})

	}
	connect.running = true
	return nil
}
func (connect *natsBusConnect) Request(name string, data []byte, timeout time.Duration) ([]byte, error) {
	if connect.client == nil {
		return nil, errors.New("无效连接")
	}
	nc := connect.client

	reply, err := nc.Request(name, data, timeout)
	if err != nil {
		return nil, err
	}

	return reply.Data, nil
}
