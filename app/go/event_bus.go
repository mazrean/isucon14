package main

import (
	"sync"
	"time"
)

type RideEvent struct {
	status     string
	evaluation int
	chair      *Chair
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

	for _, ch := range eventBus[event] {
		ch <- message
	}
}
