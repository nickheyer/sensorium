package agent

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"sensorium/internal/config"
	sensorpb "sensorium/internal/proto"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/net"
	"github.com/shirou/gopsutil/v4/process"
	"github.com/shirou/gopsutil/v4/sensors"
	"google.golang.org/protobuf/proto"
)

// Process CPU tracker
type procCPUCache struct {
	sync.RWMutex
	lastCPU  map[int32]*cpu.TimesStat
	lastTime time.Time
}

func (c *procCPUCache) calculateCPU(p *process.Process) float32 {
	c.Lock()
	defer c.Unlock()

	times, err := p.Times()
	if err != nil {
		return 0
	}

	now := time.Now()

	if lastCPU, ok := c.lastCPU[p.Pid]; ok && !c.lastTime.IsZero() {
		duration := now.Sub(c.lastTime).Seconds()
		if duration > 0 {
			// Calculate CPU usage as percentage
			cpuDelta := (times.User - lastCPU.User) + (times.System - lastCPU.System)
			// Don't multiply by NumCPU - we want per-process percentage
			cpuPercent := (cpuDelta / duration) * 100.0
			c.lastCPU[p.Pid] = times
			return float32(cpuPercent)
		}
	}

	// First time seeing this process
	c.lastCPU[p.Pid] = times
	return 0
}

func (c *procCPUCache) cleanup(activePIDs map[int32]bool) {
	c.Lock()
	defer c.Unlock()

	// Remove dead processes
	for pid := range c.lastCPU {
		if !activePIDs[pid] {
			delete(c.lastCPU, pid)
		}
	}
	c.lastTime = time.Now()
}

func Run(ctx context.Context, client pulsar.Client, cfg config.AgentConfig) error {
	log.Printf("Agent starting with NodeID: %s, Topic: %s", cfg.NodeID, cfg.Topic)

	prod, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:           cfg.Topic,
		DisableBatching: false,
		CompressionType: pulsar.LZ4,
	})
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer prod.Close()

	log.Printf("Agent producer created successfully")

	t := time.NewTicker(cfg.SamplePeriod)
	defer t.Stop()

	// Cache stats
	var prevCPUTimes []cpu.TimesStat
	var prevNetStats map[string]net.IOCountersStat
	var prevTime time.Time

	// Process CPU cache
	procCPU := &procCPUCache{
		lastCPU: make(map[int32]*cpu.TimesStat),
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			// METRICS START
			m := &sensorpb.MetricFrame{
				NodeId: cfg.NodeID,
				TsMs:   time.Now().UnixMilli(),
			}

			// CPU % - YOUR EXISTING OPTIMIZED CODE
			currentCPUTimes, err := cpu.Times(true)
			if err == nil && prevCPUTimes != nil {
				// Overall %
				var totalDelta, idleDelta float64

				// Per core % slice
				m.CpuCorePcts = make([]float32, len(currentCPUTimes))

				for i, curr := range currentCPUTimes {
					if i < len(prevCPUTimes) {
						prev := prevCPUTimes[i]
						total := (curr.User - prev.User) + (curr.System - prev.System) +
							(curr.Idle - prev.Idle) + (curr.Nice - prev.Nice) +
							(curr.Iowait - prev.Iowait) + (curr.Irq - prev.Irq) +
							(curr.Softirq - prev.Softirq) + (curr.Steal - prev.Steal)
						idle := curr.Idle - prev.Idle

						totalDelta += total
						idleDelta += idle

						// Per-core %
						if total > 0 {
							usage := 100.0 * (1.0 - idle/total)
							if i < len(m.CpuCorePcts) {
								m.CpuCorePcts[i] = float32(usage)
							}
						}
					}
				}

				// Overall %
				if totalDelta > 0 {
					m.CpuUsagePct = float32(100.0 * (1.0 - idleDelta/totalDelta))
				}
			} else if err == nil {
				// First pass: just times - no calc
				m.CpuCorePcts = make([]float32, len(currentCPUTimes))
			}

			// Update cache for next pass
			prevCPUTimes = currentCPUTimes

			// EFFICIENT PROCESS MONITORING
			if procs, err := process.Processes(); err == nil {
				m.ProcessCount = uint32(len(procs))

				type procStat struct {
					proc    *process.Process
					cpuPct  float32
					memPct  float32
					memRss  uint64
					memVms  uint64
					name    string
					user    string
					created int64
				}

				// First pass: get memory for ALL processes (single syscall each)
				procMemory := make([]struct {
					proc *process.Process
					rss  uint64
				}, 0, len(procs))

				threadCount := 0
				activePIDs := make(map[int32]bool)

				for _, p := range procs {
					activePIDs[p.Pid] = true

					// Get memory info
					if memInfo, err := p.MemoryInfo(); err == nil && memInfo.RSS > 0 {
						procMemory = append(procMemory, struct {
							proc *process.Process
							rss  uint64
						}{p, memInfo.RSS})
					}

					// Count threads
					if threads, err := p.NumThreads(); err == nil {
						threadCount += int(threads)
					}
				}

				m.ThreadCount = uint32(threadCount)

				// Sort by memory to find top processes
				sort.Slice(procMemory, func(i, j int) bool {
					return procMemory[i].rss > procMemory[j].rss
				})

				// Get detailed stats for top 20 processes only
				topCount := min(len(procMemory), 20)
				procStats := make([]procStat, 0, topCount)

				for i := range topCount {
					p := procMemory[i].proc
					stat := procStat{
						proc:   p,
						memRss: procMemory[i].rss,
					}

					// Get CPU % using our cache
					stat.cpuPct = procCPU.calculateCPU(p)

					// Get other details
					if memPct, err := p.MemoryPercent(); err == nil {
						stat.memPct = memPct
					}
					if memInfo, err := p.MemoryInfo(); err == nil {
						stat.memVms = memInfo.VMS
					}
					if name, err := p.Name(); err == nil {
						stat.name = name
					}
					if user, err := p.Username(); err == nil {
						stat.user = user
					}
					if created, err := p.CreateTime(); err == nil {
						stat.created = created
					}

					procStats = append(procStats, stat)
				}

				// Clean up dead processes from cache
				if len(procCPU.lastCPU) > 100 {
					procCPU.cleanup(activePIDs)
				}

				// Sort by CPU for top CPU procs
				sort.Slice(procStats, func(i, j int) bool {
					return procStats[i].cpuPct > procStats[j].cpuPct
				})

				// Add top 5 by CPU
				for i := 0; i < 5 && i < len(procStats); i++ {
					p := &procStats[i]
					m.TopCpuProcs = append(m.TopCpuProcs, &sensorpb.MetricFrame_ProcessInfo{
						Pid:        p.proc.Pid,
						Name:       p.name,
						CpuPct:     p.cpuPct,
						MemPct:     p.memPct,
						MemRss:     p.memRss,
						MemVms:     p.memVms,
						CreateTime: p.created,
						Username:   p.user,
					})
				}

				// Re-sort by memory for top memory list
				sort.Slice(procStats, func(i, j int) bool {
					return procStats[i].memRss > procStats[j].memRss
				})

				// Add top 5 by memory
				for i := 0; i < 5 && i < len(procStats); i++ {
					p := &procStats[i]
					m.TopMemProcs = append(m.TopMemProcs, &sensorpb.MetricFrame_ProcessInfo{
						Pid:        p.proc.Pid,
						Name:       p.name,
						CpuPct:     p.cpuPct,
						MemPct:     p.memPct,
						MemRss:     p.memRss,
						MemVms:     p.memVms,
						CreateTime: p.created,
						Username:   p.user,
					})
				}
			}

			// CPU hz
			if cpuInfos, err := cpu.Info(); err == nil {
				m.CpuFreqsMhz = make([]uint64, len(cpuInfos))
				for i, info := range cpuInfos {
					m.CpuFreqsMhz[i] = uint64(info.Mhz)
				}
				if len(cpuInfos) > 0 {
					m.CpuModel = cpuInfos[0].ModelName
				}
			}

			// Memory
			if vm, err := mem.VirtualMemory(); err == nil {
				m.MemUsed = vm.Used
				m.MemTotal = vm.Total
				m.MemFree = vm.Free
				m.MemAvailable = vm.Available
				m.MemCached = vm.Cached
				m.MemBuffers = vm.Buffers
			}

			// Swap
			if swap, err := mem.SwapMemory(); err == nil {
				m.SwapUsed = swap.Used
				m.SwapTotal = swap.Total
				m.SwapFree = swap.Free
			}

			// Disk %
			if parts, err := disk.Partitions(false); err == nil {
				for _, part := range parts {
					// Skip fake-ish linux fs's
					if strings.HasPrefix(part.Mountpoint, "/sys") ||
						strings.HasPrefix(part.Mountpoint, "/proc") ||
						strings.HasPrefix(part.Mountpoint, "/dev") ||
						strings.HasPrefix(part.Mountpoint, "/run") ||
						part.Fstype == "tmpfs" ||
						part.Fstype == "devtmpfs" {
						continue
					}

					if usage, err := disk.Usage(part.Mountpoint); err == nil {
						m.DiskUsages = append(m.DiskUsages, &sensorpb.MetricFrame_DiskUsage{
							MountPoint: part.Mountpoint,
							Device:     part.Device,
							FsType:     part.Fstype,
							Total:      usage.Total,
							Used:       usage.Used,
							Free:       usage.Free,
							UsagePct:   float32(usage.UsedPercent),
						})
					}
				}
			}

			// Disk IO
			if ioCounters, err := disk.IOCounters(); err == nil {
				for device, io := range ioCounters {
					// Skip loops and other linux bs
					if strings.HasPrefix(device, "loop") ||
						strings.HasPrefix(device, "ram") {
						continue
					}

					m.DiskIos = append(m.DiskIos, &sensorpb.MetricFrame_DiskIO{
						Device:      device,
						ReadCount:   io.ReadCount,
						WriteCount:  io.WriteCount,
						ReadBytes:   io.ReadBytes,
						WriteBytes:  io.WriteBytes,
						ReadTimeMs:  io.ReadTime,
						WriteTimeMs: io.WriteTime,
						IoTimeMs:    io.IoTime,
					})
				}
			}

			// Net iface data
			if netCounters, err := net.IOCounters(true); err == nil {
				currentNetStats := make(map[string]net.IOCountersStat)
				currentTime := time.Now()

				for _, nc := range netCounters {
					// Skip loopback and vm/virt dev
					if nc.Name == "lo" || strings.HasPrefix(nc.Name, "veth") {
						continue
					}

					currentNetStats[nc.Name] = nc

					netIface := &sensorpb.MetricFrame_NetworkInterface{
						Name:        nc.Name,
						BytesSent:   nc.BytesSent,
						BytesRecv:   nc.BytesRecv,
						PacketsSent: nc.PacketsSent,
						PacketsRecv: nc.PacketsRecv,
						ErrIn:       nc.Errin,
						ErrOut:      nc.Errout,
						DropIn:      nc.Dropin,
						DropOut:     nc.Dropout,
					}

					// Calc net traffic and speeds if we have data
					if prevNetStats != nil && !prevTime.IsZero() {
						if prev, ok := prevNetStats[nc.Name]; ok {
							duration := currentTime.Sub(prevTime).Seconds()
							if duration > 0 {
								// bytes/sec
								bytesSentRate := float64(nc.BytesSent-prev.BytesSent) / duration
								bytesRecvRate := float64(nc.BytesRecv-prev.BytesRecv) / duration
								// Convert bytes/sec to Mbps for display
								netIface.SpeedMbps = uint64((bytesSentRate + bytesRecvRate) * 8 / 1000000)
							}
						}
					}

					// TODO: Tempted to set bad ifaces speeds to -1 but might trigger recalc infinitely

					m.NetInterfaces = append(m.NetInterfaces, netIface)
				}

				prevNetStats = currentNetStats
				prevTime = currentTime
			}

			// TEMPS
			if temps, err := sensors.TemperaturesWithContext(ctx); err == nil {
				sensorMap := make(map[string]*sensorpb.MetricFrame_TemperatureSensor)

				for _, temp := range temps {
					// Group by sensor name, keep highest temp
					key := temp.SensorKey
					if existing, ok := sensorMap[key]; !ok || float32(temp.Temperature) > existing.TemperatureC {
						sensorMap[key] = &sensorpb.MetricFrame_TemperatureSensor{
							Name:         key,
							TemperatureC: float32(temp.Temperature),
							HighC:        float32(temp.High),
							CriticalC:    float32(temp.Critical),
						}
					}
				}

				// Add all sensors
				// NOTE: Need to think of a way to better manage this. The amount of arbitrary sensors on some
				// server hardware is INSANE.
				for _, sensor := range sensorMap {
					m.TempSensors = append(m.TempSensors, sensor)
				}

				// Set primary/display temp for the CPU if we can, should be an average across all cores
				if cpuTemp, ok := sensorMap["coretemp_core_0"]; ok {
					m.CpuTempC = cpuTemp.TemperatureC
				} else if cpuTemp, ok := sensorMap["cpu_thermal"]; ok {
					m.CpuTempC = cpuTemp.TemperatureC
				} else if len(sensorMap) > 0 {
					// Use first available as fallback
					// NOTE: We should test for this because we shouldn't be hitting this. We need a better agg method.
					for _, sensor := range sensorMap {
						m.CpuTempC = sensor.TemperatureC
						break
					}
				}
			}

			// Connection counts
			if conns, err := net.Connections("all"); err == nil {
				for _, conn := range conns {
					switch conn.Status {
					case "ESTABLISHED":
						m.ConnEstablished++
					case "LISTEN":
						m.ConnListen++
					case "TIME_WAIT":
						m.ConnTimeWait++
					case "CLOSE_WAIT":
						m.ConnCloseWait++
					}
				}
			}

			// Sys info
			if hostInfo, err := host.Info(); err == nil {
				m.UptimeSeconds = hostInfo.Uptime
				m.BootTime = hostInfo.BootTime
				m.Hostname = hostInfo.Hostname
				m.OsPlatform = hostInfo.Platform
				m.OsFamily = hostInfo.PlatformFamily
				m.OsVersion = hostInfo.PlatformVersion
				m.KernelVersion = hostInfo.KernelVersion
				m.KernelArch = hostInfo.KernelArch
				if m.KernelArch == "" {
					m.KernelArch = runtime.GOARCH
				}
			}

			b, _ := proto.Marshal(m)
			msgID, _ := prod.Send(ctx, &pulsar.ProducerMessage{
				Key:     cfg.NodeID,
				Payload: b,
			})

			log.Printf("Agent sent metrics for host (%v) - Message ID: %v\n", m.Hostname, msgID)
		}
	}
}
