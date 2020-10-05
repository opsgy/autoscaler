
package opsgy

import (
	"encoding/json"
	"context"
	"golang.org/x/oauth2/clientcredentials"
	"net/http"
)

// OpsgyClient to interface with the Opsgy api.
type OpsgyClient struct {

	baseURL string
	projectID string

	client *http.Client

}

// KubernetesNodePool represents a node pool in a Kubernetes cluster.
type KubernetesNodePool struct {
	ClusterID        string   `json:"clusterId,omitempty"`
	ProjectID        string   `json:"projectId,omitempty"`
	NodePoolID       string   `json:"nodePoolId,omitempty"`
	Name             string   `json:"name,omitempty"`
	Replicas         int      `json:"replicas,omitempty"`
	AutoScale        bool     `json:"autoScale,omitempty"`
	MinNodes         int      `json:"minNodes,omitempty"`
	MaxNodes         int      `json:"maxNodes,omitempty"`

	Status           *KubernetesNodePoolStatus `json:"status,omitempty"`
}

type KubernetesNodePoolStatus struct {
  Status string `json:"status,omitempty"`
	Vms []*KubernetesNodePoolVMStatus `json:"vms,omitempty"`
}

type KubernetesNodePoolVMStatus struct {
  VMID string `json:"vmId,omitempty"`
  Name string `json:"name,omitempty"`
  Status string `json:"status,omitempty"`
}

type KubernetesNodePoolScaleRequest struct {
  Replicas int
}

func newOpsgyClient(user string, token string, baseURL string, projectID string) *OpsgyClient {
	if baseURL == "" {
		baseURL = "https://api.opsgy.com";
	}

	config := clientcredentials.Config{
    ClientID: user,
    ClientSecret: token,
    TokenURL: baseURL + "/oauth/token",
	}

	oauthClient := config.Client(context.Background());

  oc := &OpsgyClient{
		baseURL: baseURL,
		projectID: projectID,
		client: oauthClient,
	}
	return oc
}

func (oc *OpsgyClient) ListNodePools(context context.Context, clusterID string) ([]*KubernetesNodePool, error) {
	resp, err := oc.client.Get(oc.baseURL + "/v1/projects/" + oc.projectID + "/clusters/" + clusterID + "/node-pools");
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	nodePools := make([]*KubernetesNodePool, 0)
	err2 := json.NewDecoder(resp.Body).Decode(&nodePools)
	if err2 != nil {
		return nil, err2
	}
	return nodePools, nil
}

func (oc *OpsgyClient) ScaleNodePool(context context.Context, clusterID string, nodePoolID string, req *KubernetesNodePoolScaleRequest) error {
	resp, err := oc.client.Get(oc.baseURL + "/v1/projects/" + oc.projectID + "/clusters/" + clusterID + "/node-pools/" + nodePoolID + "/scale");
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	nodePools := make([]*KubernetesNodePool, 0)
	err2 := json.NewDecoder(resp.Body).Decode(&nodePools)
	if err2 != nil {
		return err2
	}
	return nil
}

func (oc *OpsgyClient) DeleteNode(context context.Context, clusterID string, nodePoolID string, vmID string) error {
	req, err := http.NewRequest("DELETE", oc.baseURL + "/v1/projects/" + oc.projectID + "/clusters/" + clusterID + "/node-pools/" + nodePoolID + "/nodes/" + vmID, nil)
	if err != nil {
		return err
	}
	resp, err2 := oc.client.Do(req);
	if err2 != nil {
		return err2
	}
	defer resp.Body.Close()

	return nil
}

