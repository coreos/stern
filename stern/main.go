//   Copyright 2016 Wercker Holding BV
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package stern

import (
	"context"

	"github.com/pkg/errors"
	"github.com/wercker/stern/kubernetes"
)

// Run starts the main run loop
func Run(ctx context.Context, config *Config) error {
	clientConfig := kubernetes.NewClientConfig(config.KubeConfig, config.ContextName)
	clientset, err := kubernetes.NewClientSet(clientConfig)
	if err != nil {
		return err
	}

	var namespace string
	// A specific namespace is ignored if all-namespaces is provided
	if config.AllNamespaces {
		namespace = ""
	} else {
		namespace = config.Namespace
		if namespace == "" {
			namespace, _, err = clientConfig.Namespace()
			if err != nil {
				return errors.Wrap(err, "unable to get default namespace")
			}
		}
	}

	added, removed, terminated, err := Watch(ctx, clientset.Core().Pods(namespace), config.PodQuery, config.ContainerQuery, config.LabelSelector)
	if err != nil {
		return errors.Wrap(err, "failed to set up watch")
	}

	tails := make(map[string]*Tail)

	go func() {
		for {
			select {
			case p, ok := <-added:
				if !ok {
					return
				}

				id := p.GetID()
				if _, exists := tails[id]; exists {
					continue
				}

				tail := NewTail(p.Namespace, p.Pod, p.Container, &TailOptions{
					Timestamps:   config.Timestamps,
					SinceSeconds: int64(config.Since.Seconds()),
					Exclude:      config.Exclude,
					Namespace:    config.AllNamespaces,
					TailLines:    config.TailLines,
				})
				tails[id] = tail
				tail.Start(ctx, clientset.Core().Pods(p.Namespace))
			case p, ok := <-terminated:
				if !ok {
					return
				}
				id := p.GetID()
				if _, exists := tails[id]; exists {
					continue
				}

				tail := NewTail(p.Namespace, p.Pod, p.Container, &TailOptions{
					NoFollow:     true,
					Previous:     true,
					Timestamps:   config.Timestamps,
					SinceSeconds: int64(config.Since.Seconds()),
					Exclude:      config.Exclude,
					Namespace:    config.AllNamespaces,
					TailLines:    config.TailLines,
				})
				tails[id] = tail
				tail.Start(ctx, clientset.Core().Pods(p.Namespace))
			case p, ok := <-removed:
				if !ok {
					return
				}
				id := p.GetID()
				if tail, ok := tails[id]; ok {
					tail.Close()
					delete(tails, id)
				}
			}
		}
	}()

	<-ctx.Done()

	return nil
}
