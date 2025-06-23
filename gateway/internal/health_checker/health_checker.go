package health_checker

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	container2 "github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/op/go-logging"
)

const (
	HeartbeatInterval    = 4 * time.Second
	CheckInterval        = 10 * time.Second
	MaxMissingHeartbeats = 3
)

type HealthChecker struct {
	NodeName     string
	Port         int
	PreviousNode string
	NextNode     string
	NextPort     int
	logger       *logging.Logger
}

func NewHealthChecker(logger *logging.Logger) *HealthChecker {
	node := getEnv("HOSTNAME")
	port, _ := strconv.Atoi(getEnv("PUERTO"))
	previousNode := getEnv("NODO_ANTERIOR")
	nextNode := getEnv("NODO_SIGUIENTE")
	nextPort, _ := strconv.Atoi(getEnv("PUERTO_SIGUIENTE"))
	return &HealthChecker{
		NodeName:     node,
		Port:         port,
		PreviousNode: previousNode,
		NextNode:     nextNode,
		NextPort:     nextPort,
		logger:       logger,
	}
}

func (h *HealthChecker) Start() {
	addrLocal, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", h.NodeName, h.Port))
	if err != nil {
		h.logger.Fatalf("Error resolving local node address: %v", err)
	}
	h.logger.Infof("[MONITOR] Listening for heartbeats at address: %s", addrLocal.String())

	receiver, err := net.ListenUDP("udp", addrLocal)
	if err != nil {
		h.logger.Fatalf("Error opening UDP for receiving: %v", err)
	}

	nextAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", h.NextNode, h.NextPort))
	if err != nil {
		h.logger.Fatalf("Error resolving next node address: %v", err)
	}
	h.logger.Infof("[MONITOR] Sending heartbeats to next node at address: %s", nextAddr.String())

	sender, err := net.DialUDP("udp", nil, nextAddr)
	if err != nil {
		h.logger.Fatalf("Error opening UDP for sending: %v", err)
	}

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		<-sigs
		h.logger.Infof("[MONITOR] Received exit signal, closing sockets")
		receiver.Close()
		sender.Close()
		os.Exit(0)
	}()

	lastReceived := time.Now()
	missed := 0

	go func() {
		buf := make([]byte, 64)
		for {
			receiver.SetReadDeadline(time.Now().Add(HeartbeatInterval))
			n, _, err := receiver.ReadFromUDP(buf)
			if err == nil && string(buf[:n]) == "HB" {
				lastReceived = time.Now()
				missed = 0
			}
		}
	}()

	go func() {
		for {
			_, err := sender.Write([]byte("HB"))
			if err != nil {
				h.logger.Infof("[MONITOR-%s] Error sending HB: %v", h.NodeName, err)
			}

			if time.Since(lastReceived) > CheckInterval {
				missed++
				h.logger.Infof("[MONITOR] No heartbeat received")
				lastReceived = time.Now()
				if missed >= MaxMissingHeartbeats {
					h.logger.Infof("[MONITOR] Maximum missing heartbeats reached, node may be down")
					missed = 0
					h.startContainer(h.PreviousNode)
				}
			}

			time.Sleep(HeartbeatInterval)
		}
	}()

	select {}
}

func (h *HealthChecker) startContainer(containerName string) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		h.logger.Errorf("[MONITOR] Unable to create docker-cli: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	h.logger.Infof("[MONITOR] Attempting to restart container: %s", containerName)

	container, err := cli.ContainerInspect(ctx, containerName)
	if err == nil && container.State.Running {
		h.logger.Infof("[MONITOR] Container %s is already running, no action taken", containerName)
		return
	}

	err = cli.ContainerStart(ctx, containerName, container2.StartOptions{})
	if err != nil {
		h.logger.Errorf("[MONITOR] Error starting container %s: %v", containerName, err)
		return
	}

	h.logger.Infof("[MONITOR] Successfully restarted container %s", containerName)
}

func getEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("undefined env var: %s", key)
	}

	return val
}
