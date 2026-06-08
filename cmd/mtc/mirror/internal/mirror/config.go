//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mirror

import (
	"encoding/json"
	"errors"
	"os"

	"golang.org/x/mod/sumdb/note"
	f_note "github.com/transparency-dev/formats/note"
)

// Config is a placeholder structure for configuring mirrored logs.
//
// There will likely be a standard format at some point which we should try to adopt, but for now this is it.
type Config struct {
	Logs []LogConfig `json:"logs"`
}

// LogConfig represents a log.
type LogConfig struct {
	jsonLogConfig

	Verifier note.Verifier `json:"-"`
}

type jsonLogConfig struct {
	Vkey string `json:"vkey"`
}

// UnmarshalJSON handles the unmarshalling of LogConfig.
// This is needed to parse the VKEY into a verifier.
func (lc *LogConfig) UnmarshalJSON(data []byte) error {
	r := &LogConfig{}
	if err := json.Unmarshal(data, &r.jsonLogConfig); err != nil {
		return err
	}
	if r.Vkey == "" {
		return errors.New("vkey is required")
	}
	vk, err := f_note.NewVerifier(r.Vkey)
	if err != nil {
		return err
	}
	r.Verifier = vk
	*lc = *r
	return nil
}

func Load(path string) (Config, error) {
	if path == "" {
		return Config{}, errors.New("path is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
