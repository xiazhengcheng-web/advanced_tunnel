#!/usr/bin/env python3
import argparse
import os
import sys
import time
from time import sleep
import grpc
sys.path.append(
    os.path.join(os.path.dirname(os.path.abspath(__file__)),
                 '../../utils/'))
import p4runtime_lib.bmv2
import p4runtime_lib.helper
from p4runtime_lib.switch import ShutdownAllSwitchConnections

# 端口定义：主机接入端口=1，交换机间端口按链路定义
SWITCH_TO_HOST_PORT = 1
S1_S2_PORT = 2    # s1↔s2用端口2
S1_S3_PORT = 3    # s1↔s3用端口3
S2_S3_PORT = 3    # s2↔s3用端口3
S3_S1_PORT = 2    # s3↔s1用端口2（反向）
S3_S2_PORT = 3    # s3↔s2用端口3（反向）

def writeTunnelRules(p4info_helper, ingress_sw, egress_sw, tunnel_id,
                     dst_eth_addr, dst_ip_addr, transit_port):
    """
    扩展：支持自定义交换机间传输端口（适配不同链路）
    """
    # 1. 隧道入口规则（ingress交换机：普通IP→隧道包）
    table_entry = p4info_helper.buildTableEntry(
        table_name="MyIngress.ipv4_lpm",
        match_fields={"hdr.ipv4.dstAddr": (dst_ip_addr, 32)},
        action_name="MyIngress.myTunnel_ingress",
        action_params={"dst_id": tunnel_id})
    ingress_sw.WriteTableEntry(table_entry)
    print(f"Installed ingress tunnel rule (tunnel {tunnel_id}) on {ingress_sw.name}")

    # 2. 隧道传输规则（ingress交换机：隧道包→下一跳交换机）
    table_entry = p4info_helper.buildTableEntry(
        table_name="MyIngress.myTunnel_exact",
        match_fields={"hdr.myTunnel.dst_id": tunnel_id},
        action_name="MyIngress.myTunnel_forward",
        action_params={"port": transit_port})
    ingress_sw.WriteTableEntry(table_entry)
    print(f"Installed transit tunnel rule (tunnel {tunnel_id}) on {ingress_sw.name}")

    # 3. 隧道出口规则（egress交换机：隧道包→普通IP→主机）
    table_entry = p4info_helper.buildTableEntry(
        table_name="MyIngress.myTunnel_exact",
        match_fields={"hdr.myTunnel.dst_id": tunnel_id},
        action_name="MyIngress.myTunnel_egress",
        action_params={"dstAddr": dst_eth_addr, "port": SWITCH_TO_HOST_PORT})
    egress_sw.WriteTableEntry(table_entry)
    print(f"Installed egress tunnel rule (tunnel {tunnel_id}) on {egress_sw.name}")

def writeIPForwardRules(p4info_helper, sw, dst_ip_addr, dst_mac_addr, egress_port):
    """
    新增：普通IP转发规则（无隧道头时使用）
    """
    table_entry = p4info_helper.buildTableEntry(
        table_name="MyIngress.ipv4_lpm",
        match_fields={"hdr.ipv4.dstAddr": (dst_ip_addr, 32)},
        action_name="MyIngress.ipv4_forward",
        action_params={"dstAddr": dst_mac_addr, "port": egress_port})
    sw.WriteTableEntry(table_entry)
    print(f"Installed IP forward rule (dst IP {dst_ip_addr}) on {sw.name}")

def readTableRules(p4info_helper, sw):
    """保持原功能，打印所有表规则"""
    print(f'\n----- Reading tables rules for {sw.name} -----')
    for response in sw.ReadTableEntries():
        for entity in response.entities:
            entry = entity.table_entry
            table_name = p4info_helper.get_tables_name(entry.table_id)
            print(f'{table_name}: ', end=' ')
            for m in entry.match:
                field_name = p4info_helper.get_match_field_name(table_name, m.field_id)
                field_val = p4info_helper.get_match_field_value(m)
                print(f'{field_name}={field_val}', end=' ')
            action = entry.action.action
            action_name = p4info_helper.get_actions_name(action.action_id)
            print(f'-> {action_name}', end=' ')
            for p in action.params:
                param_name = p4info_helper.get_action_param_name(action_name, p.param_id)
                print(f'{param_name}={p.value.hex()}', end=' ')
            print()

def printCounter(p4info_helper, sw, counter_name, index, log_file):
    """
    扩展：打印计数器并写入日志文件（带时间戳）
    """
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    for response in sw.ReadCounters(p4info_helper.get_counters_id(counter_name), index):
        for entity in response.entities:
            counter = entity.counter_entry
            log_msg = f"[{timestamp}] {sw.name} {counter_name} {index}: {counter.data.packet_count} packets ({counter.data.byte_count} bytes)\n"
            print(log_msg.strip())
            # 写入日志文件（追加模式）
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(log_msg)

def printGrpcError(e):
    print("gRPC Error:", e.details(), end=' ')
    status_code = e.code()
    print(f"({status_code.name})", end=' ')
    traceback = sys.exc_info()[2]
    print(f"[{traceback.tb_frame.f_code.co_filename}:{traceback.tb_lineno}]")

def main(p4info_file_path, bmv2_file_path):
    p4info_helper = p4runtime_lib.helper.P4InfoHelper(p4info_file_path)
    # 创建日志目录（若不存在）
    os.makedirs("logs", exist_ok=True)
    # 计数器日志文件路径
    counter_logs = {
        "s1": "logs/counter_s1.txt",
        "s2": "logs/counter_s2.txt",
        "s3": "logs/counter_s3.txt"
    }

    try:
        # 1. 建立所有交换机的gRPC连接（新增s3）
        s1 = p4runtime_lib.bmv2.Bmv2SwitchConnection(
            name='s1', address='127.0.0.1:50051', device_id=0,
            proto_dump_file='logs/s1-p4runtime-requests.txt')
        s2 = p4runtime_lib.bmv2.Bmv2SwitchConnection(
            name='s2', address='127.0.0.1:50052', device_id=1,
            proto_dump_file='logs/s2-p4runtime-requests.txt')
        s3 = p4runtime_lib.bmv2.Bmv2SwitchConnection(
            name='s3', address='127.0.0.1:50053', device_id=2,
            proto_dump_file='logs/s3-p4runtime-requests.txt')

        # 2. 建立主控制器仲裁（P4Runtime要求）
        s1.MasterArbitrationUpdate()
        s2.MasterArbitrationUpdate()
        s3.MasterArbitrationUpdate()

        # 3. 向所有交换机推送P4程序
        for sw in [s1, s2, s3]:
            sw.SetForwardingPipelineConfig(
                p4info=p4info_helper.p4info, bmv2_json_file_path=bmv2_file_path)
            print(f"Installed P4 Program on {sw.name}")

        # 4. 下发隧道规则：实现所有主机双向互通
        # 4.1 h1↔h2（隧道ID：100→h1→h2，200→h2→h1）
        writeTunnelRules(p4info_helper, ingress_sw=s1, egress_sw=s2, tunnel_id=100,
                         dst_eth_addr="08:00:00:00:02:22", dst_ip_addr="10.0.2.2",
                         transit_port=S1_S2_PORT)
        writeTunnelRules(p4info_helper, ingress_sw=s2, egress_sw=s1, tunnel_id=200,
                         dst_eth_addr="08:00:00:00:01:11", dst_ip_addr="10.0.1.1",
                         transit_port=S1_S2_PORT)

        # 4.2 h1↔h3（隧道ID：101→h1→h3，300→h3→h1）
        writeTunnelRules(p4info_helper, ingress_sw=s1, egress_sw=s3, tunnel_id=101,
                         dst_eth_addr="08:00:00:00:03:33", dst_ip_addr="10.0.3.3",
                         transit_port=S1_S3_PORT)
        writeTunnelRules(p4info_helper, ingress_sw=s3, egress_sw=s1, tunnel_id=300,
                         dst_eth_addr="08:00:00:00:01:11", dst_ip_addr="10.0.1.1",
                         transit_port=S3_S1_PORT)

        # 4.3 h2↔h3（隧道ID：201→h2→h3，301→h3→h2）
        writeTunnelRules(p4info_helper, ingress_sw=s2, egress_sw=s3, tunnel_id=201,
                         dst_eth_addr="08:00:00:00:03:33", dst_ip_addr="10.0.3.3",
                         transit_port=S2_S3_PORT)
        writeTunnelRules(p4info_helper, ingress_sw=s3, egress_sw=s2, tunnel_id=301,
                         dst_eth_addr="08:00:00:00:02:22", dst_ip_addr="10.0.2.2",
                         transit_port=S3_S2_PORT)

        # 5. 下发普通IP转发规则（无隧道头时兼容）
        # s1：h1→h2普通IP（转发到s2-2）
        writeIPForwardRules(p4info_helper, sw=s1, dst_ip_addr="10.0.2.2",
                            dst_mac_addr="08:00:00:00:02:22", egress_port=S1_S2_PORT)
        # s1：h1→h3普通IP（转发到s3-3）
        writeIPForwardRules(p4info_helper, sw=s1, dst_ip_addr="10.0.3.3",
                            dst_mac_addr="08:00:00:00:03:33", egress_port=S1_S3_PORT)
        # s2：h2→h1普通IP（转发到s1-2）
        writeIPForwardRules(p4info_helper, sw=s2, dst_ip_addr="10.0.1.1",
                            dst_mac_addr="08:00:00:00:01:11", egress_port=S1_S2_PORT)
        # s2：h2→h3普通IP（转发到s3-3）
        writeIPForwardRules(p4info_helper, sw=s2, dst_ip_addr="10.0.3.3",
                            dst_mac_addr="08:00:00:00:03:33", egress_port=S2_S3_PORT)
        # s3：h3→h1普通IP（转发到s1-2）
        writeIPForwardRules(p4info_helper, sw=s3, dst_ip_addr="10.0.1.1",
                            dst_mac_addr="08:00:00:00:01:11", egress_port=S3_S1_PORT)
        # s3：h3→h2普通IP（转发到s2-3）
        writeIPForwardRules(p4info_helper, sw=s3, dst_ip_addr="10.0.2.2",
                            dst_mac_addr="08:00:00:00:02:22", egress_port=S3_S2_PORT)

        # 6. 读取所有交换机的表规则（验证下发）
        readTableRules(p4info_helper, s1)
        readTableRules(p4info_helper, s2)
        readTableRules(p4info_helper, s3)

        # 7. 周期性读取计数器并写入文件（每2秒）
        while True:
            sleep(2)
            print('\n----- Reading tunnel counters (logged to logs/counter_*.txt) -----')
            # h1↔h2计数器
            printCounter(p4info_helper, s1, "MyIngress.ingressTunnelCounter", 100, counter_logs["s1"])
            printCounter(p4info_helper, s2, "MyIngress.egressTunnelCounter", 100, counter_logs["s2"])
            printCounter(p4info_helper, s2, "MyIngress.ingressTunnelCounter", 200, counter_logs["s2"])
            printCounter(p4info_helper, s1, "MyIngress.egressTunnelCounter", 200, counter_logs["s1"])
            # h1↔h3计数器
            printCounter(p4info_helper, s1, "MyIngress.ingressTunnelCounter", 101, counter_logs["s1"])
            printCounter(p4info_helper, s3, "MyIngress.egressTunnelCounter", 101, counter_logs["s3"])
            printCounter(p4info_helper, s3, "MyIngress.ingressTunnelCounter", 300, counter_logs["s3"])
            printCounter(p4info_helper, s1, "MyIngress.egressTunnelCounter", 300, counter_logs["s1"])
            # h2↔h3计数器
            printCounter(p4info_helper, s2, "MyIngress.ingressTunnelCounter", 201, counter_logs["s2"])
            printCounter(p4info_helper, s3, "MyIngress.egressTunnelCounter", 201, counter_logs["s3"])
            printCounter(p4info_helper, s3, "MyIngress.ingressTunnelCounter", 301, counter_logs["s3"])
            printCounter(p4info_helper, s2, "MyIngress.egressTunnelCounter", 301, counter_logs["s2"])

    except KeyboardInterrupt:
        print("\nShutting down controller.")
    except grpc.RpcError as e:
        printGrpcError(e)
    finally:
        ShutdownAllSwitchConnections()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='P4Runtime Controller (Full Interconnect)')
    parser.add_argument('--p4info', type=str, default='./build/advanced_tunnel.p4.p4info.txt',
                        help='P4info text file (default: ./build/advanced_tunnel.p4.p4info.txt)')
    parser.add_argument('--bmv2-json', type=str, default='./build/advanced_tunnel.json',
                        help='BMv2 JSON file (default: ./build/advanced_tunnel.json)')
    args = parser.parse_args()

    # 校验文件存在性
    for file_path in [args.p4info, args.bmv2_json]:
        if not os.path.exists(file_path):
            parser.print_help()
            print(f"\nFile not found: {file_path}\nHave you run 'make'?")
            parser.exit(1)

    main(args.p4info, args.bmv2_json)
