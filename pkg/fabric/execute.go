package fabric

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	// run this if no reference is found. go get github.com/go-resty/resty/v2
	"github.com/go-resty/resty/v2"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

type InstallAction struct {
	Steps []InstallStep `yaml:"install"`
}

type InstallStep struct {
	InstallArguments `yaml:"fabric"`
}

type InstallArguments struct {
	Name string `yaml:"arguments"`
}

type FabricArtifacts struct {
	Workspace_id string                 `json:"workspace_id"`
	Access_token string                 `json:"access_token"`
	Lakehouse    map[string]interface{} `json:"lakehouse"`
}

// We get the entire YAML in the payload. But we only process the section that is needed for this mixin
func (m *Mixin) getPayloadData() ([]byte, error) {
	reader := bufio.NewReader(m.In)
	data, err := io.ReadAll(reader)
	return data, errors.Wrap(err, "could not read payload STDIN")

}

func (m *Mixin) processJSON(filePath string) FabricArtifacts {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatal("Error reading JSON")
	}

	var fa FabricArtifacts

	errs := json.Unmarshal(content, &fa)
	if errs != nil {
		fmt.Println("There seems to be an error")

	}

	return fa
}

func (m *Mixin) Execute(ctx context.Context) error {
	fmt.Println("Executing Fabric Mixin")
	payload, err := m.getPayloadData()
	if err != nil {
		return err

	}
	fmt.Print(payload)
	var action InstallAction
	err = yaml.Unmarshal(payload, &action)
	if err != nil {
		return err
	}
	fmt.Println(action)
	fmt.Println(action.Steps[0].Name)

	fabricArt := m.processJSON(action.Steps[0].Name)

	m.post(fabricArt.Workspace_id, fabricArt.Access_token, fabricArt.Lakehouse)
	return nil
}

func (m *Mixin) post(workspace_id string, token string, jsonRequest map[string]interface{}) {
	uri := "https://api.fabric.microsoft.com/v1/workspaces/" + workspace_id + "/items"
	fmt.Println("Creating Fabric artifacts in Workspace :" + workspace_id)
	fmt.Println((uri))
	fmt.Print("Artifact Definition: ")
	fmt.Println(jsonRequest)

	client := resty.New()
	response, err := client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(jsonRequest).
		SetAuthToken(token).
		Post(uri)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(response.Header())
}
