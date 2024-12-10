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
	chairID    string
	updatedAt  time.Time
}

var (
	eventBus     = map[string][]chan<- *RideEvent{}
	eventBusLock = sync.RWMutex{}
)

func initEventBus() {
	eventBusLock.Lock()
	defer eventBusLock.Unlock()

	eventBus = make(map[string][]chan<- *RideEvent)
}

func Subscribe(event string, ch chan<- *RideEvent) {
	eventBusLock.Lock()
	defer eventBusLock.Unlock()

	eventBus[event] = append(eventBus[event], ch)
}

func Publish(event string, message *RideEvent) {
	eventBusLock.RLock()
	defer eventBusLock.RUnlock()

	statusGauge.WithLabelValues(message.status).Inc()
	switch message.status {
	case "MATCHED":
		statusGauge.WithLabelValues("MATCHING").Dec()
	case "ENROUTE":
		statusGauge.WithLabelValues("MATCHED").Dec()
	case "PICKUP":
		statusGauge.WithLabelValues("ENROUTE").Dec()
	case "CARRYING":
		statusGauge.WithLabelValues("PICKUP").Dec()
	case "ARRIVED":
		statusGauge.WithLabelValues("CARRYING").Dec()
	case "COMPLETED":
		statusGauge.WithLabelValues("ARRIVED").Dec()
	}

	for _, ch := range eventBus[event] {
		ch <- message
	}
}

var statusGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "chair_status",
	Help: "chair status",
}, []string{"status"})
