package client

import (
	"context"
	"fmt"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-lib-utils/connection"
	"github.com/kubernetes-csi/csi-lib-utils/metrics"
	"github.com/kubernetes-csi/csi-lib-utils/rpc"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"

	modifyrpc "github.com/awslabs/volume-modifier-for-k8s/pkg/rpc"
)

type Client interface {
	GetDriverName(context.Context) (string, error)

	SupportsVolumeModification(context.Context) error

	Modify(ctx context.Context, volumeID string, params, reqContext map[string]string) error

	CloseConnection()
}

func New(addr string, timeout time.Duration, metricsmanager metrics.CSIMetricsManager) (Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := connection.Connect(ctx, addr, metricsmanager, connection.OnConnectionLoss(connection.ExitOnConnectionLoss()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to CSI driver: %w", err)
	}

	err = rpc.ProbeForever(ctx, conn, timeout)
	if err != nil {
		return nil, fmt.Errorf("failed probing CSI driver: %w", err)
	}

	caps, err := rpc.GetControllerCapabilities(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("failed getting controller capabilities: %v", err)
	}

	return &client{
		conn:                           conn,
		supportsControllerModifyVolume: caps[csi.ControllerServiceCapability_RPC_MODIFY_VOLUME],
	}, nil
}

type client struct {
	conn                           *grpc.ClientConn
	supportsControllerModifyVolume bool
}

func (c *client) GetDriverName(ctx context.Context) (string, error) {
	return rpc.GetDriverName(ctx, c.conn)
}

func (c *client) SupportsVolumeModification(ctx context.Context) error {
	if c.supportsControllerModifyVolume {
		return nil
	}

	cc := modifyrpc.NewModifyClient(c.conn)
	req := &modifyrpc.GetCSIDriverModificationCapabilityRequest{}
	_, err := cc.GetCSIDriverModificationCapability(ctx, req)
	return err
}

func (c *client) Modify(ctx context.Context, volumeID string, params, reqContext map[string]string) error {
	var err error
	if c.supportsControllerModifyVolume {
		cc := csi.NewControllerClient(c.conn)
		req := &csi.ControllerModifyVolumeRequest{
			VolumeId:          volumeID,
			MutableParameters: params,
			Secrets:           reqContext,
		}
		_, err = cc.ControllerModifyVolume(ctx, req)
	} else {
		cc := modifyrpc.NewModifyClient(c.conn)
		req := &modifyrpc.ModifyVolumePropertiesRequest{
			Name:       volumeID,
			Parameters: params,
			Context:    reqContext,
		}
		_, err = cc.ModifyVolumeProperties(ctx, req)
	}
	if err == nil {
		klog.V(4).InfoS("Volume modification completed", "volumeID", volumeID)
	}
	return err
}

func (c *client) CloseConnection() {
	c.conn.Close()
}
