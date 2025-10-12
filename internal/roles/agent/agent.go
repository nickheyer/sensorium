package agent

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sort"
	"strings"
	"time"

	"sensorium/internal/config"
	sensorpb "sensorium/internal/proto"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/prometheus/procfs"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/net"
	"github.com/shirou/gopsutil/v4/sensors"
	"google.golang.org/protobuf/proto"
)

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

	// Initialize procfs
	fs, err := procfs.NewDefaultFS()
	if err != nil {
		return fmt.Errorf("failed to initialize procfs: %w", err)
	}

	log.Printf("Agent producer created successfully")

	t := time.NewTicker(cfg.SamplePeriod)
	defer t.Stop()

	// Cache for calculating deltas
	var prevNetStats map[string]net.IOCountersStat
	var prevStat procfs.Stat
	var prevProcStats map[int]procfs.ProcStat
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

			// CPU stats using procfs
			stat, err := fs.Stat()
			if err == nil {
				// Calculate CPU usage from stat
				if prevTime.IsZero() {
					prevTime = time.Now()
					prevStat = stat
				} else {
					currentTime := time.Now()

					// Overall CPU calculation
					totalDelta := float64(stat.CPUTotal.User - prevStat.CPUTotal.User +
						stat.CPUTotal.Nice - prevStat.CPUTotal.Nice +
						stat.CPUTotal.System - prevStat.CPUTotal.System +
						stat.CPUTotal.Idle - prevStat.CPUTotal.Idle +
						stat.CPUTotal.Iowait - prevStat.CPUTotal.Iowait +
						stat.CPUTotal.IRQ - prevStat.CPUTotal.IRQ +
						stat.CPUTotal.SoftIRQ - prevStat.CPUTotal.SoftIRQ +
						stat.CPUTotal.Steal - prevStat.CPUTotal.Steal)

					idleDelta := float64(stat.CPUTotal.Idle - prevStat.CPUTotal.Idle)

					if totalDelta > 0 {
						m.CpuUsagePct = float32((1.0 - idleDelta/totalDelta) * 100.0)
					}

					// Per-core CPU
					m.CpuCorePcts = make([]float32, len(stat.CPU))
					for i, cpu := range stat.CPU {
						if int(i) < len(prevStat.CPU) {
							prevCPU := prevStat.CPU[i]
							coreTotal := float64(cpu.User - prevCPU.User +
								cpu.Nice - prevCPU.Nice +
								cpu.System - prevCPU.System +
								cpu.Idle - prevCPU.Idle +
								cpu.Iowait - prevCPU.Iowait +
								cpu.IRQ - prevCPU.IRQ +
								cpu.SoftIRQ - prevCPU.SoftIRQ +
								cpu.Steal - prevCPU.Steal)
							coreIdle := float64(cpu.Idle - prevCPU.Idle)
							if coreTotal > 0 {
								m.CpuCorePcts[i] = float32((1.0 - coreIdle/coreTotal) * 100.0)
							}
						}
					}

					prevStat = stat
					prevTime = currentTime
				}

				m.CpuCountLogical = uint32(len(stat.CPU))
				m.CpuCountPhysical = uint32(runtime.NumCPU())
			}

			// Load average from procfs
			if loadavg, err := fs.LoadAvg(); err == nil {
				m.LoadAvg_1M = float32(loadavg.Load1)
				m.LoadAvg_5M = float32(loadavg.Load5)
				m.LoadAvg_15M = float32(loadavg.Load15)
			}

			// Process stats using procfs
			if procs, err := fs.AllProcs(); err == nil {
				m.ProcessCount = uint32(len(procs))

				type procInfo struct {
					proc   procfs.Proc
					stat   procfs.ProcStat
					cpuPct float32
					memRss int64
				}

				procInfos := make([]procInfo, 0)
				currentProcStats := make(map[int]procfs.ProcStat)
				threadCount := 0

				// First pass: get basic stats for all processes
				for _, p := range procs {
					stat, err := p.Stat()
					if err != nil {
						continue
					}

					currentProcStats[p.PID] = stat
					threadCount += stat.NumThreads

					// Only track processes with meaningful memory
					if stat.ResidentMemory() > 0 {
						info := procInfo{
							proc:   p,
							stat:   stat,
							memRss: int64(stat.ResidentMemory()),
						}

						// Calculate CPU% if we have previous data
						if prevProcStats != nil {
							if prevStat, ok := prevProcStats[p.PID]; ok && prevStat.Starttime == stat.Starttime {
								// Same process (not restarted)
								cpuDelta := (stat.UTime - prevStat.UTime) + (stat.STime - prevStat.STime)
								timeDelta := time.Since(prevTime).Seconds()
								if timeDelta > 0 {
									info.cpuPct = float32(float64(cpuDelta) / timeDelta / float64(runtime.NumCPU()))
								}
							}
						}

						procInfos = append(procInfos, info)
					}
				}

				m.ThreadCount = uint32(threadCount)
				prevProcStats = currentProcStats

				// Sort by memory to find top processes
				sort.Slice(procInfos, func(i, j int) bool {
					return procInfos[i].memRss > procInfos[j].memRss
				})

				// Get top 5 by memory
				for i := 0; i < 5 && i < len(procInfos); i++ {
					p := &procInfos[i]

					// Get username (this still requires a syscall, but only for top 5)
					username := ""
					if status, err := p.proc.NewStatus(); err == nil && len(status.UIDs) > 0 {
						// You'd need to map UID to username here
						username = fmt.Sprintf("uid:%d", status.UIDs[0])
					}

					// Get memory info
					memInfo, _ := fs.Meminfo()
					memPct := float32(0)
					if memInfo.MemTotal != nil && *memInfo.MemTotal > 0 {
						memPct = float32(p.memRss) / float32(*memInfo.MemTotal) * 100
					}

					m.TopMemProcs = append(m.TopMemProcs, &sensorpb.MetricFrame_ProcessInfo{
						Pid:        int32(p.proc.PID),
						Name:       p.stat.Comm,
						CpuPct:     p.cpuPct,
						MemPct:     memPct,
						MemRss:     uint64(p.memRss),
						MemVms:     uint64(p.stat.VirtualMemory()),
						CreateTime: int64(p.stat.Starttime),
						Username:   username,
					})
				}

				// Sort by CPU for top CPU procs
				sort.Slice(procInfos, func(i, j int) bool {
					return procInfos[i].cpuPct > procInfos[j].cpuPct
				})

				// Get top 5 by CPU
				for i := 0; i < 5 && i < len(procInfos); i++ {
					p := &procInfos[i]

					username := ""
					if status, err := p.proc.NewStatus(); err == nil && len(status.UIDs) > 0 {
						username = fmt.Sprintf("uid:%d", status.UIDs[0])
					}

					memInfo, _ := fs.Meminfo()
					memPct := float32(0)
					if memInfo.MemTotal != nil && *memInfo.MemTotal > 0 {
						memPct = float32(p.memRss) / float32(*memInfo.MemTotal) * 100
					}

					m.TopCpuProcs = append(m.TopCpuProcs, &sensorpb.MetricFrame_ProcessInfo{
						Pid:        int32(p.proc.PID),
						Name:       p.stat.Comm,
						CpuPct:     p.cpuPct,
						MemPct:     memPct,
						MemRss:     uint64(p.memRss),
						MemVms:     uint64(p.stat.VirtualMemory()),
						CreateTime: int64(p.stat.Starttime),
						Username:   username,
					})
				}
			}

			// Memory (use procfs meminfo)
			if memInfo, err := fs.Meminfo(); err == nil {
				if memInfo.MemTotal != nil {
					m.MemTotal = *memInfo.MemTotal
				}
				if memInfo.MemFree != nil {
					m.MemFree = *memInfo.MemFree
				}
				if memInfo.MemAvailable != nil {
					m.MemAvailable = *memInfo.MemAvailable
				}
				if memInfo.Cached != nil {
					m.MemCached = *memInfo.Cached
				}
				if memInfo.Buffers != nil {
					m.MemBuffers = *memInfo.Buffers
				}
				if memInfo.SwapTotal != nil {
					m.SwapTotal = *memInfo.SwapTotal
				}
				if memInfo.SwapFree != nil {
					m.SwapFree = *memInfo.SwapFree
				}

				// Calculate used
				m.MemUsed = m.MemTotal - m.MemAvailable
				m.SwapUsed = m.SwapTotal - m.SwapFree
			}

			// Keep using gopsutil for disk, network, temps, etc since procfs doesn't have good support
			// [Rest of your existing code for disk, network, temps, etc...]

			// CPU info (for model name)
			if cpuinfo, err := fs.CPUInfo(); err == nil && len(cpuinfo) > 0 {
				m.CpuModel = cpuinfo[0].ModelName
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
