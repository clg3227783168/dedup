核心原因

  1. 长期运行的守护进程

  dedup-snapshotter
  是一个需要持续运行的守护进程,而不是一次性执行的命令。从
  cmd/main.go:155-172
  可以看到,主进程会一直监听信号和请求,直到收到停止信号。

  2. 作为 Containerd 的 Proxy Plugin

  - dedup-snapshotter 通过 Unix Socket
  (/run/containerd/dedup-snapshotter.sock) 与 containerd 通信
  - containerd 需要在运行时随时调用这个 snapshotter
  来处理镜像拉取、快照创建等操作
  - 如果 snapshotter 没有运行,containerd 将无法使用 dedup 功能

  3. 多个后台任务需要持续执行

  从 cmd/main.go:125-133 可以看到,程序启动后会运行多个后台任务:

  go startMetricsReporter()      // 每5分钟报告一次指标
  go startAuditCleanup(auditLogger)  // 每24小时清理一次审计日志
  go apiServer.Start()           // HTTP API 服务器(端口8080)

  4. 按需加载机制的必要条件

  根据 CLAUDE.md 中的技术细节:
  - 当容器访问文件时,fscache 会通知用户态的 dedupd 进程处理访问请求
  - dedupd 需要持续运行才能响应内核的按需加载请求
  - 如果进程不在,按需加载功能将失效

  5. 自动重启和故障恢复

  systemd 服务配置中包含了关键的保活设置(从 install.sh:60-64):

  Type=simple
  Restart=always          # 崩溃后自动重启
  RestartSec=5           # 5秒后重启

  这确保了服务意外退出后能自动恢复。

  6. 系统启动时自动启动

  通过 systemctl enable 
  dedup-snapshotter,可以让服务在系统启动时自动运行,确保 containerd
  启动时 snapshotter 已经就绪(参见 install.sh:58 中的
  After=network.target)。

  systemd 服务的具体配置

  从 dedup-snapshotter/scripts/install.sh:54-71 可以看到服务配置:

  [Unit]
  Description=Dedup Snapshotter for Containerd
  After=network.target          # 网络启动后再启动

  [Service]
  Type=simple
  ExecStart=/usr/local/bin/dedup-snapshotter
  Restart=always                # 崩溃自动重启
  RestartSec=5
  Environment="ADDRESS=/run/containerd/dedup-snapshotter.sock"
  Environment="ROOT=/var/lib/containerd/io.containerd.snapshotter.v1.de
  dup"
  Environment="CONFIG=/etc/dedup-snapshotter/config.json"

  [Install]
  WantedBy=multi-user.target    # 开机自启

  总结

  如果不使用 systemd 服务而是手动启动:
  - ❌ 系统重启后需要手动启动
  - ❌ 进程崩溃后不会自动恢复
  - ❌ 难以统一管理和监控
  - ❌ 无法保证在 containerd 之后、其他服务之前启动

  使用 systemd 服务后:
  - ✅ 开机自动启动
  - ✅ 崩溃自动恢复
  - ✅ 统一的日志管理(journalctl -u dedup-snapshotter)
  - ✅ 标准化的服务管理(systemctl start/stop/restart/status)
  - ✅ 符合 Linux 系统服务的最佳实践