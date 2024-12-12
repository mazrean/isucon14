package main

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type RideEvent struct {
	status     string
	evaluation int
	chair      *Chair
	ride       *Ride
	updatedAt  time.Time
}

var (
	chairEventBus     = map[string][]chan<- *RideEvent{}
	chairEventBusLock = sync.RWMutex{}
	userEventBus      = map[string][]chan<- *RideEvent{}
	userEventBusLock  = sync.RWMutex{}
)

func initEventBus() {
	chairEventBusLock.Lock()
	defer chairEventBusLock.Unlock()

	chairEventBus = make(map[string][]chan<- *RideEvent)

	userEventBusLock.Lock()
	defer userEventBusLock.Unlock()

	userEventBus = make(map[string][]chan<- *RideEvent)
}

func ChairSubscribe(event string, ch chan<- *RideEvent) {
	chairEventBusLock.Lock()
	defer chairEventBusLock.Unlock()

	chairEventBus[event] = append(chairEventBus[event], ch)
}

func ChairPublish(event string, message *RideEvent) {
	chairEventBusLock.RLock()
	defer chairEventBusLock.RUnlock()

	chairStatusGauge.WithLabelValues(message.status).Inc()
	switch message.status {
	case "MATCHED":
		chairStatusGauge.WithLabelValues("COMPLETED").Dec()
	case "ENROUTE":
		chairStatusGauge.WithLabelValues("MATCHED").Dec()
	case "PICKUP":
		chairStatusGauge.WithLabelValues("ENROUTE").Dec()
	case "CARRYING":
		chairStatusGauge.WithLabelValues("PICKUP").Dec()
	case "ARRIVED":
		chairStatusGauge.WithLabelValues("CARRYING").Dec()
	case "COMPLETED":
		chairStatusGauge.WithLabelValues("ARRIVED").Dec()
	}

	for _, ch := range chairEventBus[event] {
		ch <- message
	}
}

var chairStatusGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "chair_status",
	Help: "chair status",
}, []string{"status"})

func UserSubscribe(event string, ch chan<- *RideEvent) {
	userEventBusLock.Lock()
	defer userEventBusLock.Unlock()

	userEventBus[event] = append(userEventBus[event], ch)
}

func UserPublish(event string, message *RideEvent) {
	userEventBusLock.RLock()
	defer userEventBusLock.RUnlock()

	userStatusGauge.WithLabelValues(message.status).Inc()
	switch message.status {
	case "MATCHING":
		userStatusGauge.WithLabelValues("COMPLETED").Dec()
	case "MATCHED":
		userStatusGauge.WithLabelValues("MATCHING").Dec()
	case "ENROUTE":
		userStatusGauge.WithLabelValues("MATCHED").Dec()
	case "PICKUP":
		userStatusGauge.WithLabelValues("ENROUTE").Dec()
	case "CARRYING":
		userStatusGauge.WithLabelValues("PICKUP").Dec()
	case "ARRIVED":
		userStatusGauge.WithLabelValues("CARRYING").Dec()
	case "COMPLETED":
		userStatusGauge.WithLabelValues("ARRIVED").Dec()
	}

	for _, ch := range userEventBus[event] {
		ch <- message
	}
}

var userStatusGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "user_status",
	Help: "user status",
}, []string{"status"})
