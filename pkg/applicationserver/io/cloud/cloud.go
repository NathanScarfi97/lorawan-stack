// Copyright © 2019 The Things Network Foundation, The Things Industries B.V.
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

// Package cloud implements the go-cloud frontend.
package cloud

import (
	"context"
	"fmt"

	"go.thethings.network/lorawan-stack/pkg/applicationserver/io"
	"go.thethings.network/lorawan-stack/pkg/applicationserver/io/formatters"
	"go.thethings.network/lorawan-stack/pkg/log"
	"go.thethings.network/lorawan-stack/pkg/ttnpb"
	"go.thethings.network/lorawan-stack/pkg/unique"
	"gocloud.dev/pubsub"
)

type srv struct {
	ctx          context.Context
	server       io.Server
	formatter    formatters.Formatter
	topic        *pubsub.Topic
	subscription *pubsub.Subscription
}

// Start starts the cloud frontend.
func Start(ctx context.Context, server io.Server, formatter formatters.Formatter, pubURL, subURL string) (sub *io.Subscription, err error) {
	ctx = log.NewContextWithField(ctx, "namespace", "applicationserver/io/cloud")
	if err != nil {
		return nil, err
	}
	s := &srv{
		ctx:       ctx,
		server:    server,
		formatter: formatter,
	}

	s.topic, err = pubsub.OpenTopic(ctx, pubURL)
	if err != nil {
		return nil, err
	}
	s.subscription, err = pubsub.OpenSubscription(ctx, subURL)
	if err != nil {
		return nil, err
	}

	go func() {
		<-ctx.Done()
		s.topic.Shutdown(ctx)
		s.subscription.Shutdown(ctx)
	}()

	sub = io.NewSubscription(s.ctx, "cloud", nil)
	// Publish upstream
	go func() {
		logger := log.FromContext(s.ctx)
		for {
			select {
			case <-sub.Context().Done():
				logger.WithError(sub.Context().Err()).Debug("Done sending upstream messages")
				return
			case up := <-sub.Up():
				buf, err := s.formatter.FromUp(up)
				if err != nil {
					log.WithError(err).Warn("Failed to marshal upstream message")
					continue
				}
				logger.Infof("Publish upstream message")
				s.topic.Send(ctx, &pubsub.Message{
					Body: buf,
				})
			}
		}
	}()

	// Subscribe downstream
	go func() {
		logger := log.FromContext(s.ctx)
		for ctx.Err() == nil {
			msg, err := s.subscription.Receive(ctx)
			if err != nil {
				logger.WithError(err).Warn("Failed to receive downlink queue operation")
				continue
			}
			operation, err := s.formatter.ToDownlinksOperation(msg.Body)
			if err != nil {
				logger.WithError(err).Warn("Failed to decode downlink queue operation")
				continue
			}
			var op func(io.Server, context.Context, ttnpb.EndDeviceIdentifiers, []*ttnpb.ApplicationDownlink) error
			switch operation.Operation {
			case ttnpb.DownlinkQueueOperation_PUSH:
				op = io.Server.DownlinkQueuePush
			case ttnpb.DownlinkQueueOperation_REPLACE:
				op = io.Server.DownlinkQueueReplace
			default:
				panic(fmt.Errorf("invalid operation: %v", operation.Operation))
			}
			logger.WithFields(log.Fields(
				"device_uid", unique.ID(s.ctx, operation.EndDeviceIdentifiers),
				"count", len(operation.Downlinks),
			)).Debug("Handle downlink messages")
			if err := op(s.server, s.ctx, operation.EndDeviceIdentifiers, operation.Downlinks); err != nil {
				logger.WithError(err).Warn("Failed to handle downlink messages")
			}
		}
	}()

	return sub, nil
}
