package configuration

import (
	"encoding/json"
	"io/ioutil"
)

type PeerList []string

func PeerListFromJson(jsonText []byte) (*PeerList, error) {
	var pl PeerList
	return &pl, json.Unmarshal(jsonText, &pl)
}

func PeerListFromJsonFile(path string) (*PeerList, error) {
	text, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return PeerListFromJson(text)
}

func (pl *PeerList) Count() int {
	return len(*pl)
}
