# Dedup Snapshotter - 基于 Erofs 的块级去重容器快照存储

一个高性能的 containerd snapshotter 插件,实现了基于 Erofs 文件系统的块级去重存储、按需加载和内存去重功能。

## 特性

### 核心功能

1. **块级数据去重** - 4MB 固定块大小的内容寻址存储,SHA-256 哈希去重,跨镜像层的数据块复用
2. **按需加载** - 基于 Erofs 只读文件系统的延迟加载,异步预取机制,容器启动时序追踪优化
3. **内存去重** - 基于 KSM (Kernel Samepage Merging) 的页面合并,madvise(MADV_MERGEABLE) 标记可合并内存
4. **Erofs 文件系统支持** - 只读压缩镜像格式,原生内核支持,无 FUSE 性能损耗
5. **OCI 兼容性** - 完全兼容 OCI v1 镜像规范,保留镜像分层逻辑

## 架构设计

```
┌─────────────────────────────────────────────────────────┐
│                    Containerd                           │
└────────────────────┬────────────────────────────────────┘
                     │ gRPC
┌────────────────────▼────────────────────────────────────┐
│              Dedup Snapshotter                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │   Erofs      │  │    Lazy      │  │   Memory     │  │
│  │   Builder    │  │   Loader     │  │   Dedup      │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │   Mount      │  │    Chunk     │  │   Metrics    │  │
│  │   Manager    │  │   Indexer    │  │   Reporter   │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│         Storage Layer (Erofs + Overlay)                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│  │   Chunks    │  │   Images    │  │  Snapshots  │     │
│  │   (dedup)   │  │   (.erofs)  │  │  (overlay)  │     │
│  └─────────────┘  └─────────────┘  └─────────────┘     │
└─────────────────────────────────────────────────────────┘
```

## 快速开始

### 1. 安装依赖

```bash
# OpenCloudOS / CentOS
sudo yum install erofs-utils sqlite-devel

# Ubuntu / Debian
sudo apt-get install erofs-utils libsqlite3-dev
```

### 2. 编译安装

```bash
make build
sudo make install
```

### 3. 配置 containerd

编辑 `/etc/containerd/config.toml`:

```toml
[proxy_plugins]
  [proxy_plugins.dedup]
    type = "snapshot"
    address = "/run/containerd/dedup-snapshotter.sock"
```

### 4. 启动服务

```bash
sudo systemctl start dedup-snapshotter
sudo systemctl enable dedup-snapshotter
sudo systemctl restart containerd
```

### 5. 测试

```bash
# 使用 dedup snapshotter 拉取镜像
sudo ctr images pull --snapshotter=dedup docker.io/library/nginx:latest

# 查看去重统计
sudo journalctl -u dedup-snapshotter | grep Metrics
```

## 配置说明

配置文件位于 `/etc/dedup-snapshotter/config.json`

## 性能优化

- 块级去重效果: 典型场景节省 30-60% 存储空间
- 按需加载优化: 容器启动时间减少 50-70%
- 内存去重: 节省 20-40% 内存占用
