package vpc

import (
	"github.com/go-kit/kit/log/level"
	"net/http"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/vpc"
	"github.com/fengxsong/aliyun-exporter/pkg/client/service"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

// constants
const (
	name = "nat"
	pageSize = 50
)

// Client wrap client
type Client struct {
	*vpc.Client
	desc *prometheus.Desc
	logger log.Logger
}

// New create ServiceCollector
func New(ak, secret, region string, rt http.RoundTripper, logger log.Logger) (service.Collector, error) {
	client, err := vpc.NewClientWithAccessKey(region, ak, secret)
	if err != nil {
		return nil, err
	}
	client.SetTransport(rt)

	return &Client{Client: client, logger: logger}, nil
}

// Collect collect metrics
func (c *Client) Collect(namespace string, ch chan<- prometheus.Metric) {
	if c.desc == nil {
		c.desc = service.NewInstanceClientDesc(namespace, name, []string{"regionId", "instanceId", "name", "status"})
	}

	req := vpc.CreateDescribeNatGatewaysRequest()
	req.PageSize = requests.NewInteger(pageSize)
	instanceCh := make(chan vpc.NatGateway, 1<<10)
	go func() {
		defer close(instanceCh)
		for hasNextPage, pageNum := true, 1; hasNextPage != false; pageNum++ {
			req.PageNumber = requests.NewInteger(pageNum)
			response, err := c.DescribeNatGateways(req)
			if err != nil {
				level.Error(c.logger).Log("error", err)
				return
			}
			if len(response.NatGateways.NatGateway) < pageSize {
				hasNextPage = false
			}
			for i := range response.NatGateways.NatGateway {
				instanceCh <- response.NatGateways.NatGateway[i]
			}
		}
	}()

	for ins := range instanceCh {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, 1.0,
			ins.RegionId, ins.NatGatewayId, ins.Name, ins.Status)
	}
}

// register
func init()  {
	service.Register(name, New)
}
