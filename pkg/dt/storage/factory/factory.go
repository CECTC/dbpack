/*
 * Copyright 2022 CECTC, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package factory

import (
	"fmt"

	"github.com/cectc/dbpack/pkg/dt/storage"
)

// driverFactories stores an internal mapping between storage driver names and their respective
// factories
var driverFactories = make(map[string]StorageDriverFactory)

// StorageDriverFactory is a factory interface for creating storage.Driver interfaces
// Storage drivers should call Register() with a factory to make the driver available by name.
// Individual Driver implementations generally register with the factory via the Register
// func (below) in their init() funcs, and as such they should be imported anonymously before use.
type StorageDriverFactory interface {
	// Create returns a new storage.Driver with the given parameters
	// Parameters will vary by driver and may be ignored
	// Each parameter key must only consist of lowercase letters and numbers
	Create(parameters map[string]interface{}) (storage.Driver, error)
}

// Register makes a storage driver available by the provided name.
// If Register is called twice with the same name or if driver factory is nil, it panics.
// Additionally, it is not concurrency safe. Most Storage Drivers call this function
// in their init() functions. See the documentation for StorageDriverFactory for more.
func Register(name string, factory StorageDriverFactory) {
	if factory == nil {
		panic("Must not provide nil StorageDriverFactory")
	}
	_, registered := driverFactories[name]
	if registered {
		panic(fmt.Sprintf("StorageDriverFactory named %s already registered", name))
	}

	driverFactories[name] = factory
}

// Create a new storage.Driver with the given name and
// parameters. To use a driver, the StorageDriverFactory must first be
// registered with the given name. If no drivers are found, an
// InvalidStorageDriverError is returned
func Create(name string, parameters map[string]interface{}) (storage.Driver, error) {
	driverFactory, ok := driverFactories[name]
	if !ok {
		return nil, InvalidStorageDriverError{name}
	}
	return driverFactory.Create(parameters)
}

// InvalidStorageDriverError records an attempt to construct an unregistered storage driver
type InvalidStorageDriverError struct {
	Name string
}

func (err InvalidStorageDriverError) Error() string {
	return fmt.Sprintf("Driver not registered: %s", err.Name)
}
