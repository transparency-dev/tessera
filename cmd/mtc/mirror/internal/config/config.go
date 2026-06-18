// Copyright 2026 The Tessera authors. All Rights Reserved.
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

// Package config contains code and structs for configuring log sources for mirrors.
package config

// Config represents a collection of logs which are eligible for being mirrored.
type Config struct {
	Logs []LogConfig `json:"logs"`
}

// LogConfig represents the configuration for a single log.
type LogConfig struct {
	// VKey is the verification key of the log.
	// The origin is the name of the key.
	VKey string `json:"vkey"`
}
