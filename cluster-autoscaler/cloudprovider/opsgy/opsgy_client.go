package opsgy

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"golang.org/x/oauth2/clientcredentials"
)

// OpsgyClient to interface with the Opsgy api.
type OpsgyClient struct {
	baseURL   string
	projectID string

	client *http.Client
}

// KubernetesNodePoolPage page of pools
type KubernetesNodePoolPage struct {
	Items    []*KubernetesNodePool `json:"items,omitempty"`
	PageSize int                   `json:"pageSize,omitempty"`
	NextPage string                `json:"nextPage,omitempty"`
}

// KubernetesNodePool represents a node pool in a Kubernetes cluster.
type KubernetesNodePool struct {
	ClusterID  string `json:"clusterId,omitempty"`
	ProjectID  string `json:"projectId,omitempty"`
	NodePoolID string `json:"nodePoolId,omitempty"`
	Name       string `json:"name,omitempty"`
	Replicas   int    `json:"replicas,omitempty"`
	AutoScale  bool   `json:"autoScale,omitempty"`
	MinNodes   int    `json:"minNodes,omitempty"`
	MaxNodes   int    `json:"maxNodes,omitempty"`

	Status *KubernetesNodePoolStatus `json:"status,omitempty"`
}

type KubernetesNodePoolStatus struct {
	Status      string                              `json:"status,omitempty"`
	Allocatable KubernetesNodePoolStatusAllocatable `json:"allocatable,omitempty"`
	Vms         []*KubernetesNodePoolVMStatus       `json:"vms,omitempty"`
}

type KubernetesNodePoolStatusAllocatable struct {
	CPU     int64 `json:"cpu,omitempty"`
	Memory  int64 `json:"memory,omitempty"`
	Storage int64 `json:"storage,omitempty"`
}

type KubernetesNodePoolVMStatus struct {
	VMID   string `json:"vmId,omitempty"`
	Name   string `json:"name,omitempty"`
	Status string `json:"status,omitempty"`
}

type KubernetesNodePoolScaleRequest struct {
	Replicas int `json:"replicas"`
}

func newOpsgyClient(user string, token string, baseURL string, projectID string) *OpsgyClient {
	if baseURL == "" {
		baseURL = "https://api.opsgy.com"
	}

	config := clientcredentials.Config{
		ClientID:     user,
		ClientSecret: token,
		TokenURL:     baseURL + "/oauth/token",
	}

	oauthClient := config.Client(context.Background())

	oc := &OpsgyClient{
		baseURL:   baseURL,
		projectID: projectID,
		client:    oauthClient,
	}
	return oc
}

func (oc *OpsgyClient) ListNodePools(context context.Context, clusterID string) ([]*KubernetesNodePool, error) {
	nodePools := make([]*KubernetesNodePool, 0)

	var lastItem *string
	for {
		var startFrom = ""
		if lastItem != nil {
			startFrom = "&startFrom=" + *lastItem
		}
		url := oc.baseURL + "/v1/projects/" + oc.projectID + "/clusters/" + clusterID + "/node-pools?pageSize=50" + startFrom

		resp, err := oc.client.Get(url)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		var nodePoolPage KubernetesNodePoolPage
		err2 := json.NewDecoder(resp.Body).Decode(&nodePoolPage)
		if err2 != nil {
			return nil, err2
		}

		for _, nodePool := range nodePoolPage.Items {
			nodePools = append(nodePools, nodePool)
		}
		lastItem = &nodePoolPage.NextPage
		if len(nodePoolPage.Items) < 50 {
			break
		}
	}

	return nodePools, nil
}

func (oc *OpsgyClient) ScaleNodePool(context context.Context, clusterID string, nodePoolID string, scaleReq *KubernetesNodePoolScaleRequest) error {
	buf := new(bytes.Buffer)
	json.NewEncoder(buf).Encode(scaleReq)
	req, err := http.NewRequest("POST", oc.baseURL+"/v1/projects/"+oc.projectID+"/clusters/"+clusterID+"/node-pools/"+nodePoolID+"/scale", buf)
	if err != nil {
		return err
	}
	req.Header.Add("content-type", "application/json")

	resp, err2 := oc.client.Do(req)

	if err2 != nil {
		return err2
	}
	defer resp.Body.Close()

	return nil
}

func (oc *OpsgyClient) DeleteNode(context context.Context, clusterID string, nodePoolID string, vmID string) error {
	req, err := http.NewRequest("DELETE", oc.baseURL+"/v1/projects/"+oc.projectID+"/clusters/"+clusterID+"/node-pools/"+nodePoolID+"/vms/"+vmID, nil)
	if err != nil {
		return err
	}
	resp, err2 := oc.client.Do(req)
	if err2 != nil {
		return err2
	}
	defer resp.Body.Close()

	return nil
}
