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
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/net"
	"github.com/shirou/gopsutil/v4/process"
	"github.com/shirou/gopsutil/v4/sensors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

type cpuSample struct {
	total       float64
	procs       map[int32]float64
	threadCount int32
	at          time.Time
}

type procCPU struct {
	pid   int32
	name  string
	cpu   float64
	mem   float32
	rss   uint64
	vms   uint64
	user  string
	start int64
}

var lastSample *cpuSample

// Helpers

func sampleCPU(ctx context.Context) (*cpuSample, error) {
	times, err := cpu.Times(false)
	if err != nil || len(times) == 0 {
		return nil, err
	}

	sampler := times[0]
	total := sampler.User + sampler.System + sampler.Idle + sampler.Nice + sampler.Iowait + sampler.Irq +
		sampler.Softirq + sampler.Steal + sampler.Guest + sampler.GuestNice

	procs, err := process.Processes()
	if err != nil {
		return nil, err
	}

	ps := make(map[int32]float64, len(procs))
	var mu sync.Mutex
	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(runtime.NumCPU() / 2) // Using half cores

	for _, p := range procs {
		g.Go(func() error {
			t, err := p.TimesWithContext(ctx)
			if err != nil {
				return nil // Skip on error
			}
			mu.Lock()
			ps[p.Pid] = t.User + t.System
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Fetch total threads
	misc, err := load.MiscWithContext(ctx)
	threadCt := int32(0)
	if err == nil {
		threadCt = int32(misc.ProcsTotal)
	}

	return &cpuSample{total: total, procs: ps, threadCount: threadCt, at: time.Now()}, nil
}

func diffAndRank(ctx context.Context, prev, curr *cpuSample, topN int) ([]procCPU, error) {
	totalDelta := curr.total - prev.total
	if totalDelta <= 0 {
		return nil, fmt.Errorf("invalid CPU delta")
	}

	numCPU, _ := cpu.Counts(true)
	var mu sync.Mutex
	results := make([]procCPU, 0, len(curr.procs))
	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(runtime.NumCPU() / 2) // Using half cores

	for pid, curTime := range curr.procs {
		prevTime, ok := prev.procs[pid]
		if !ok {
			continue
		}
		delta := curTime - prevTime
		if delta <= 0 {
			continue
		}

		usage := (delta / totalDelta) * 100 * float64(numCPU)
		if usage <= 0 {
			continue
		}

		g.Go(func() error {
			p, err := process.NewProcess(pid)
			if err != nil {
				return nil
			}
			name, _ := p.Name()
			memPct, _ := p.MemoryPercent()
			memInfo, _ := p.MemoryInfo()
			user, _ := p.Username()
			start, _ := p.CreateTime()

			mu.Lock()
			results = append(results, procCPU{
				pid: pid, name: name, cpu: usage,
				mem: memPct, rss: memInfo.RSS, vms: memInfo.VMS,
				user: user, start: start,
			})
			mu.Unlock()
			return nil
		})
	}

	_ = g.Wait()

	sort.Slice(results, func(i, j int) bool { return results[i].cpu > results[j].cpu })
	if len(results) > topN {
		results = results[:topN]
	}
	return results, nil
}

func GetTopProcesses(ctx context.Context, topN int) (cpuTop, memTop []procCPU, sample *cpuSample, err error) {
	curr, err := sampleCPU(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	if lastSample == nil {
		lastSample = curr
		return nil, nil, nil, nil // no baseline yet
	}

	procs, err := diffAndRank(ctx, lastSample, curr, topN)
	lastSample = curr
	if err != nil {
		return nil, nil, nil, err
	}

	// Also top mem
	memSorted := make([]procCPU, len(procs))
	copy(memSorted, procs)
	sort.Slice(memSorted, func(i, j int) bool {
		return memSorted[i].mem > memSorted[j].mem
	})
	if len(memSorted) > topN {
		memSorted = memSorted[:topN]
	}

	return procs, memSorted, curr, nil
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

	t := time.NewTicker(cfg.SamplePeriod)
	defer t.Stop()

	process.EnableBootTimeCache(true)
	lastSample, _ = sampleCPU(ctx) // seed

	var prevNetStats map[string]net.IOCountersStat
	var prevTime time.Time

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

			// CPU %
			if cpuPcts, err := cpu.Percent(0, false); err == nil && len(cpuPcts) > 0 {
				m.CpuUsagePct = float32(cpuPcts[0])
			}

			// Per-core usage
			if coresPcts, err := cpu.Percent(0, true); err == nil {
				m.CpuCorePcts = make([]float32, len(coresPcts))
				for i, pct := range coresPcts {
					m.CpuCorePcts[i] = float32(pct)
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

			// CPU counts
			if logical, err := cpu.Counts(true); err == nil {
				m.CpuCountLogical = uint32(logical)
			}
			if physical, err := cpu.Counts(false); err == nil {
				m.CpuCountPhysical = uint32(physical)
			}

			// Load avg
			if loadAvg, err := load.Avg(); err == nil {
				m.LoadAvg_1M = float32(loadAvg.Load1)
				m.LoadAvg_5M = float32(loadAvg.Load5)
				m.LoadAvg_15M = float32(loadAvg.Load15)
			}

			// Proc/thread ct
			topCPU, topMem, sample, _ := GetTopProcesses(ctx, 5)
			m.ThreadCount = sample.threadCount
			m.ProcessCount = int32(len(sample.procs))

			for _, p := range topCPU {
				// Top 5 procs by CPU %
				m.TopCpuProcs = append(m.TopCpuProcs, &sensorpb.MetricFrame_ProcessInfo{
					Pid:        p.pid,
					Name:       p.name,
					CpuPct:     p.cpu,
					MemPct:     p.mem,
					MemRss:     p.rss,
					MemVms:     p.vms,
					CreateTime: p.start,
					Username:   p.user,
				})
			}

			// Top 5 procs by mem
			for _, p := range topMem {
				m.TopMemProcs = append(m.TopMemProcs, &sensorpb.MetricFrame_ProcessInfo{
					Pid:        p.pid,
					Name:       p.name,
					CpuPct:     p.cpu,
					MemPct:     p.mem,
					MemRss:     p.rss,
					MemVms:     p.vms,
					CreateTime: p.start,
					Username:   p.user,
				})
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
