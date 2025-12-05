#!/usr/bin/env python3
import argparse
import os
import sys
import time
from time import sleep
import grpc

# 将 utils 路径添加到系统路径
sys.path.append(
    os.path.join(os.path.dirname(os.path.abspath(__file__)),
                 '../../utils/'))
import p4runtime_lib.bmv2
import p4runtime_lib.helper
from p4runtime_lib.switch import ShutdownAllSwitchConnections

# ---- 端口定义 (根据 topology.json) ----
SWITCH_TO_HOST_PORT = 1
# S1 连接关系
S1_PORT_TO_S2 = 2
S1_PORT_TO_S3 = 3
# S2 连接关系
S2_PORT_TO_S1 = 2
S2_PORT_TO_S3 = 3
# S3 连接关系
S3_PORT_TO_S2 = 3
S3_PORT_TO_S1 = 2

# ---- 隧道 ID 定义 ----
# 格式: 源ID_目的ID (例如 T100 代表 S1->S2)
TUNNEL_ID_S1_S2 = 100
TUNNEL_ID_S2_S1 = 200
TUNNEL_ID_S1_S3 = 101
TUNNEL_ID_S3_S1 = 300
TUNNEL_ID_S2_S3 = 201
TUNNEL_ID_S3_S2 = 301

def writeTunnelRules(p4info_helper, ingress_sw, egress_sw, tunnel_id,
                     dst_eth_addr, dst_ip_addr, transit_port):
    """
    安装隧道规则:
    1. 入口交换机 (Ingress): 匹配目的IP -> 封装隧道头 (Action: myTunnel_ingress)
    2. 出口交换机 (Egress): 匹配隧道ID -> 解封装并转发给主机 (Action: myTunnel_egress)
    注意：为了避免冲突，这里不再安装中间传输规则，直接由 ingress/egress 处理。
    """
    # 1. Ingress Rule: 遇到目的IP，封装隧道ID，并发送到级联端口
    # 注意：这里会占用该 IP 在 ipv4_lpm 表中的匹配项
    table_entry = p4info_helper.buildTableEntry(
        table_name="MyIngress.ipv4_lpm",
        match_fields={"hdr.ipv4.dstAddr": (dst_ip_addr, 32)},
        action_name="MyIngress.myTunnel_ingress",
        action_params={"dst_id": tunnel_id})
    ingress_sw.WriteTableEntry(table_entry)
    print(f"Installed Ingress Rule on {ingress_sw.name}: IP {dst_ip_addr} -> Tunnel {tunnel_id}")

    # 2. Transit Rule: 在入口交换机上也需要配置隧道转发规则，将包发出去
    # (P4逻辑中 myTunnel_ingress 只是加头，之后需要查 myTunnel_exact 表决定出端口)
    table_entry = p4info_helper.buildTableEntry(
        table_name="MyIngress.myTunnel_exact",
        match_fields={"hdr.myTunnel.dst_id": tunnel_id},
        action_name="MyIngress.myTunnel_forward",
        action_params={"port": transit_port})
    ingress_sw.WriteTableEntry(table_entry)

    # 3. Egress Rule: 在出口交换机，匹配隧道ID，解封装，发给主机
    table_entry = p4info_helper.buildTableEntry(
        table_name="MyIngress.myTunnel_exact",
        match_fields={"hdr.myTunnel.dst_id": tunnel_id},
        action_name="MyIngress.myTunnel_egress",
        action_params={"dstAddr": dst_eth_addr, "port": SWITCH_TO_HOST_PORT})
    egress_sw.WriteTableEntry(table_entry)
    print(f"Installed Egress Rule on {egress_sw.name}: Tunnel {tunnel_id} -> Host")

def getCounterValue(p4info_helper, sw, counter_name, index):
    """
    读取计数器值的辅助函数
    """
    for response in sw.ReadCounters(p4info_helper.get_counters_id(counter_name), index):
        for entity in response.entities:
            return entity.counter_entry.data.packet_count, entity.counter_entry.data.byte_count
    return 0, 0

def log_link_stats(p4info_helper, src_sw, dst_sw, t_id_src_dst, t_id_dst_src, log_file):
    """
    读取双向链路计数器并写入文件
    模拟截图中的格式输出
    """
    # 获取方向 1: Src -> Dst (例如 S1 -> S2)
    # 在 Src 处读取 Ingress 计数 (进入隧道的数据)
    s_tx_pkts, s_tx_bytes = getCounterValue(p4info_helper, src_sw, "MyIngress.ingressTunnelCounter", t_id_src_dst)
    # 在 Dst 处读取 Egress 计数 (离开隧道的数据)
    d_rx_pkts, d_rx_bytes = getCounterValue(p4info_helper, dst_sw, "MyIngress.egressTunnelCounter", t_id_src_dst)

    # 获取方向 2: Dst -> Src (例如 S2 -> S1)
    d_tx_pkts, d_tx_bytes = getCounterValue(p4info_helper, dst_sw, "MyIngress.ingressTunnelCounter", t_id_dst_src)
    s_rx_pkts, s_rx_bytes = getCounterValue(p4info_helper, src_sw, "MyIngress.egressTunnelCounter", t_id_dst_src)

    # 构建日志内容 (参考截图格式)
    lines = []
    lines.append(f"----- {src_sw.name} -> {dst_sw.name} -----")
    lines.append(f"{src_sw.name} MyIngress.ingressTunnelCounter {t_id_src_dst}: {s_tx_pkts} packets ({s_tx_bytes} bytes)")
    lines.append(f"{dst_sw.name} MyIngress.egressTunnelCounter {t_id_src_dst}: {d_rx_pkts} packets ({d_rx_bytes} bytes)")
    lines.append("")
    lines.append(f"----- {dst_sw.name} -> {src_sw.name} -----")
    lines.append(f"{dst_sw.name} MyIngress.ingressTunnelCounter {t_id_dst_src}: {d_tx_pkts} packets ({d_tx_bytes} bytes)")
    lines.append(f"{src_sw.name} MyIngress.egressTunnelCounter {t_id_dst_src}: {s_rx_pkts} packets ({s_rx_bytes} bytes)")
    lines.append("\n" + "-"*30 + " Finished " + "-"*30 + "\n")

    log_content = "\n".join(lines)

    # 写入文件 (覆盖写，或者追加写，这里用追加以保留历史)
    # 为了防止文件无限增长，演示中可以使用 'w' 覆盖模式，或者在此处追加
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(time.strftime("[%Y-%m-%d %H:%M:%S]\n"))
        f.write(log_content)

def main(p4info_file_path, bmv2_file_path):
    p4info_helper = p4runtime_lib.helper.P4InfoHelper(p4info_file_path)

    # 创建日志目录
    if not os.path.exists("logs"):
        os.makedirs("logs")

    # 清空旧日志
    for f in ["logs/S1S2.txt", "logs/S1S3.txt", "logs/S2S3.txt"]:
        with open(f, "w") as file:
            file.write("=== Statistic Log Started ===\n")

    try:
        # 1. 建立连接
        s1 = p4runtime_lib.bmv2.Bmv2SwitchConnection(
            name='s1', address='127.0.0.1:50051', device_id=0)
        s2 = p4runtime_lib.bmv2.Bmv2SwitchConnection(
            name='s2', address='127.0.0.1:50052', device_id=1)
        s3 = p4runtime_lib.bmv2.Bmv2SwitchConnection(
            name='s3', address='127.0.0.1:50053', device_id=2)

        # 2. 设置主控并下发 Pipeline
        for sw in [s1, s2, s3]:
            sw.MasterArbitrationUpdate()
            sw.SetForwardingPipelineConfig(
                p4info=p4info_helper.p4info, bmv2_json_file_path=bmv2_file_path)
            print(f"Installed P4 Program on {sw.name}")

        # 3. 配置隧道规则 (全互联)
        # 注意：这里配置了隧道后，就不需要配置普通的 IP Forward 规则给这些 IP 了

        # --- Link S1 <-> S2 ---
        # h1(S1) -> h2(S2)
        writeTunnelRules(p4info_helper, s1, s2, TUNNEL_ID_S1_S2,
                         "08:00:00:00:02:22", "10.0.2.2", S1_PORT_TO_S2)
        # h2(S2) -> h1(S1)
        writeTunnelRules(p4info_helper, s2, s1, TUNNEL_ID_S2_S1,
                         "08:00:00:00:01:11", "10.0.1.1", S2_PORT_TO_S1)

        # --- Link S1 <-> S3 ---
        # h1(S1) -> h3(S3)
        writeTunnelRules(p4info_helper, s1, s3, TUNNEL_ID_S1_S3,
                         "08:00:00:00:03:33", "10.0.3.3", S1_PORT_TO_S3)
        # h3(S3) -> h1(S1)
        writeTunnelRules(p4info_helper, s3, s1, TUNNEL_ID_S3_S1,
                         "08:00:00:00:01:11", "10.0.1.1", S3_PORT_TO_S1)

        # --- Link S2 <-> S3 ---
        # h2(S2) -> h3(S3)
        writeTunnelRules(p4info_helper, s2, s3, TUNNEL_ID_S2_S3,
                         "08:00:00:00:03:33", "10.0.3.3", S2_PORT_TO_S3)
        # h3(S3) -> h2(S2)
        writeTunnelRules(p4info_helper, s3, s2, TUNNEL_ID_S3_S2,
                         "08:00:00:00:02:22", "10.0.2.2", S3_PORT_TO_S2)

        print("\nAll tunnel rules installed. Hosts should be reachable.")
        print("Starting statistics monitoring (Press Ctrl+C to stop)...\n")

        # 4. 循环读取计数器并写入文件
        while True:
            sleep(2)
            print("Refreshing statistics...")

            # S1 <-> S2
            log_link_stats(p4info_helper, s1, s2, TUNNEL_ID_S1_S2, TUNNEL_ID_S2_S1, "logs/S1S2.txt")

            # S1 <-> S3
            log_link_stats(p4info_helper, s1, s3, TUNNEL_ID_S1_S3, TUNNEL_ID_S3_S1, "logs/S1S3.txt")

            # S2 <-> S3
            log_link_stats(p4info_helper, s2, s3, TUNNEL_ID_S2_S3, TUNNEL_ID_S3_S2, "logs/S2S3.txt")

    except KeyboardInterrupt:
        print("\nShutting down.")
    except grpc.RpcError as e:
        print(f"gRPC Error: {e.details()}")
    finally:
        ShutdownAllSwitchConnections()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='P4Runtime Controller')
    parser.add_argument('--p4info', type=str, default='./build/advanced_tunnel.p4.p4info.txt',
                        help='P4info text file')
    parser.add_argument('--bmv2-json', type=str, default='./build/advanced_tunnel.json',
                        help='BMv2 JSON file')
    args = parser.parse_args()

    if not os.path.exists(args.p4info):
        print(f"File not found: {args.p4info}")
        sys.exit(1)

    main(args.p4info, args.bmv2_json)
