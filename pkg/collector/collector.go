package collector

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/fengxsong/aliyun-exporter/pkg/client"
	"github.com/fengxsong/aliyun-exporter/pkg/config"
)

// cloudMonitor ..
type cloudMonitor struct {
	namespace string
	cfg       *config.Config
	logger    log.Logger
	// sdk client
	client *client.MetricClient
	// collector metrics
	scrapeDurationDesc *prometheus.Desc

	lock sync.Mutex

	reqCount int
	maxCount int
	interval time.Duration
	reqLock sync.Mutex
	reqChan chan bool
}

// NewCloudMonitorCollector create a new collector for cloud monitor
func NewCloudMonitorCollector(appName string, cfg *config.Config, rt http.RoundTripper, logger log.Logger) (prometheus.Collector, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	cli, err := client.NewMetricClient(cfg.AccessKey, cfg.AccessKeySecret, cfg.Region, rt, logger)
	if err != nil {
		return nil, err
	}
	return &cloudMonitor{
		namespace: appName,
		cfg:       cfg,
		logger:    logger,
		client:    cli,
		scrapeDurationDesc: prometheus.NewDesc(
			prometheus.BuildFQName(appName, "scrape", "collector_duration_seconds"),
			fmt.Sprintf("%s_exporter: Duration of a collector scrape.", appName),
			[]string{"namespace", "collector"},
			nil,
		),
		maxCount: 50,
		interval: 1*time.Second,
		reqChan: make(chan bool, 1),
		reqCount: 0,
	}, nil
}

func (m *cloudMonitor) Describe(ch chan<- *prometheus.Desc) {
	ch <- m.scrapeDurationDesc
}

func (m *cloudMonitor) Collect(ch chan<- prometheus.Metric) {
	m.lock.Lock()
	defer m.lock.Unlock()

	subCtx, cancel := context.WithCancel(context.Background())
	// 启动定时器，每秒重置reqCount
	go m.resetReqCount(subCtx)

	wg := &sync.WaitGroup{}
	// do collect
	for sub, metrics := range m.cfg.Metrics {
		for i := range metrics {
			wg.Add(1)
			m.reqIsAvailable()
			select {
			case <- m.reqChan:
				// reqCount + 1
				m.reqIncrease()

				go func(namespace string, metric *config.Metric) {
					defer wg.Done()
					start := time.Now()
					m.client.Collect(m.namespace, namespace, metric, ch)
					ch <- prometheus.MustNewConstMetric(
						m.scrapeDurationDesc,
						prometheus.GaugeValue,
						time.Now().Sub(start).Seconds(),
						namespace, metric.String())
				}(sub, metrics[i])
			}
		}
	}
	// 每一次请求完成后都要退出计时器
	cancel()
	wg.Wait()
}

// 请求计数增加
func (m *cloudMonitor) reqIncrease() {
	m.reqLock.Lock()
	defer m.reqLock.Unlock()

	m.reqCount += 1
}

// 是否可以继续请求
func (m *cloudMonitor) reqIsAvailable()  {
	m.reqLock.Lock()
	defer m.reqLock.Unlock()

	if m.reqCount < m.maxCount {
		// 因为计时器也会往channel中写入数据，所以需要判断下, 以免阻塞在此
		if len(m.reqChan) == 0 {
			m.reqChan <- true
		}
	}
}

func (m *cloudMonitor) resetReqCount(ctx context.Context) {

	ticker := time.NewTicker(m.interval)
	for {
		select {
		case <- ticker.C:
			m.reqLock.Lock()
			m.reqCount = 0
			m.reqLock.Unlock()

			// 定时器写入是因为reqCount >= maxCount后，reqIsAvailable不再写入true到channel，避免select阻塞
			m.reqChan <- true
		case <- ctx.Done():
			return
		}
	}
}
