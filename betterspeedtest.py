import argparse
import asyncio
import logging
import shlex
from asyncio import subprocess
from math import ceil
from typing import Optional

import numpy
from icmplib import async_ping

NETPERF = 'netperf'


async def run_netperf(log: logging.Logger, proto: int, host: str, direction: str, length: int) -> subprocess.Process:
    cmd = f'-{proto} -H {host} -t {direction} -l {length} -v 0 -P 0'
    log.debug('%s %s', NETPERF, cmd)
    return await asyncio.create_subprocess_exec(
        NETPERF, *shlex.split(cmd),
        stdout=subprocess.PIPE)


async def get_netperf(proc):
    stdout, stderr = await proc.communicate()
    return float(stdout.strip())


def print_result(args: argparse.Namespace, result, speed: Optional[float] = None):
    sorted_rtts = list(sorted(result.rtts))

    if speed:
        dir_msg = 'Upload' if args.direction == 'up' else 'Download'
        print(f'{dir_msg:>9} {speed:.2f} Mbps')
    print(f'{"Latency:":>9} (in msec, {result.packets_sent} pings, {result.packet_loss * 100:.2f}% packet loss)')
    print(f'{"Min:":>9} {result.min_rtt:.3f}')
    print(f'{"10pct:":>9} {numpy.percentile(sorted_rtts, 10):.3f}')
    print(f'{"Median:":>9} {numpy.percentile(sorted_rtts, 50):.3f}')
    print(f'{"Avg:":>9} {result.avg_rtt:.3f}')
    print(f'{"90pct:":>9} {numpy.percentile(sorted_rtts, 90):.3f}')
    print(f'{"Max:":>9} {result.max_rtt:.3f}')
    print(f'{"Jitter:":>9} {result.jitter:.3f}')


async def main(log: logging.Logger, args: argparse.Namespace):
    direction = 'TCP_STREAM' if args.direction == 'up' else 'TCP_MAERTS'
    count = int(ceil(args.length / args.interval))
    length = int(ceil(count * args.interval))

    if args.idle:
        print(f'pinging {args.ping} for {count} * {args.interval}s = {length}s')
        ping_task = asyncio.create_task(async_ping(args.ping, count=count, interval=args.interval, privileged=False))
        await ping_task
        print_result(args, ping_task.result())
        return

    proto = 'ipv4' if args.proto == 4 else 'ipv6'
    print(f'{args.num} {proto} {args.direction} netperfs to {args.host} while pinging {args.ping} '
          f'with {1 / args.interval:.1f} pings/s for {length}s')

    procs = [await run_netperf(log, args.proto, args.host, direction, length + args.warmup + 2) for _ in
             range(args.num)]
    if args.warmup > 0:
        print(f'Warming up for {args.warmup}s...')
        await asyncio.sleep(args.warmup)

    print('Running test...')
    ping_task = asyncio.create_task(async_ping(args.ping, count=count, interval=args.interval, privileged=False))

    netperfs = [get_netperf(proc) for proc in procs]
    speeds = await asyncio.gather(*netperfs)
    speed = sum(speeds)

    await ping_task
    print_result(args, ping_task.result(), speed)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--proto', type=int, default=4)
    parser.add_argument('--host', default='netperf-west.bufferbloat.net')
    parser.add_argument('--ping', default='1.1.1.1')
    parser.add_argument('--direction', default='up')
    parser.add_argument('--length', type=int, default=30)
    parser.add_argument('--interval', type=float, default=.1)
    parser.add_argument('--warmup', type=int, default=10)
    parser.add_argument('--num', type=int, default=5)
    parser.add_argument('--idle', action='store_true')
    parser.add_argument('--log-level', default='info')
    pargs = parser.parse_args()

    logging.basicConfig()
    log = logging.getLogger('bst')
    log.setLevel(level=getattr(logging, pargs.log_level.upper()))

    asyncio.run(main(log, pargs))
