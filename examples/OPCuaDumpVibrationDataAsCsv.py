#!/usr/bin/env python3

import os
import re
import numpy
import asyncio
import argparse

from datetime import datetime, timedelta
from ipaddress import ip_address
from asyncua import ua, Client
from asyncua.common.node import Node
from typing import Optional

OUTPUT_DIR: str = './CSV'
# Settings for history read
DETAILS = ua.ReadRawModifiedDetails()
DETAILS.StartTime = datetime.utcnow()
DETAILS.EndTime = datetime.utcnow() - timedelta(days=100)
# DETAILS.EndTime = datetime.fromisoformat('2022-01-01T00:00:00')
DETAILS.NumValuesPerNode = 10


class VibrationToCSV(object):
    '''
    Dumps Root/Objects/ab:cd:12:34/vibration/x/accel/xAccelFreq to csv.
    '''
    TIMEOUT: int = 15  # [sec]
    PATTERN_MACID = re.compile(
        r'(([0-9a-f]{2}[:]){3}([0-9a-f]{2}))', re.I
    )

    @classmethod
    async def dump(cls, url: str) -> None:
        try:
            async with Client(url=url, timeout=cls.TIMEOUT) as client:
                await client.load_data_type_definitions()
                await cls._loop_devices(client=client)
        except (ConnectionError, OSError) as e:
            print(f'Connection to {url} failed: {str(e)}')

    @classmethod
    async def _loop_devices(cls, client: Client):
        childList = await client.nodes.objects.get_children()
        for device in childList:
            try:
                await cls._export_device(device=device)
            except ua.uaerrors.BadWaitingForInitialData:
                continue

    @classmethod
    async def _export_device(cls, device: Node) -> None:
        mac = await device.read_browse_name()
        m = re.match(cls.PATTERN_MACID, mac.Name)
        if not m:
            return  # not a sensor (ab:cd:12:34)

        nsIdx: int = mac.NamespaceIndex
        tagNode: Node = await device.get_child([
            ua.QualifiedName('deviceTag', nsIdx)
        ])
        tag: str = await tagNode.read_value()
        if tag == 'delete':
            return  # device was deleted

        try:
            vibrationNode: Node = await device.get_child([
                ua.QualifiedName('vibration', nsIdx),
                ua.QualifiedName('x', nsIdx),
                ua.QualifiedName('accel', nsIdx),
                ua.QualifiedName('xAccelFreq', nsIdx),
            ])
        except ua.uaerrors.BadNoMatch:
            print(f'REJECT: {mac.Name} ({tag})')
            return  # No vibration node
        else:
            print(f'ACCEPT: {mac.Name} ({tag})')

        await cls._loop_history(
            mac=mac,
            tag=tag,
            vibrationNode=vibrationNode,
        )

    @classmethod
    async def _loop_history(cls, vibrationNode: Node, **kwargs) -> None:
        cont: Optional[bytes] = None
        for i in range(1024):
            res = await vibrationNode.history_read(DETAILS, cont)
            res.StatusCode.check()
            for dv in res.HistoryData.DataValues:
                cls._export_vibration_to_csv(dv=dv, **kwargs)
            # No more data available
            cont = res.ContinuationPoint
            if cont is None:
                break

    @classmethod
    def _export_vibration_to_csv(
        cls,
        mac: ua.QualifiedName,
        tag: str,
        dv: ua.DataValue,
    ) -> None:
        vv = dv.Value.Value
        header = (
            f'device, {mac.Name}\n'
            f'tag, {tag}\n'
            f'rate, {vv.sampleRate}Hz\n'
            f'range, +/-{vv.formatRange}{vv.vUnits}\n'
            f'date, {dv.SourceTimestamp.isoformat()}\n'
            f'Frequency[Hz], Acceleration X-axis[{vv.vUnits}]'
        )

        def fmt(string: str) -> str:
            return string.replace(':', '-').replace(' ', '_')

        dt = dv.SourceTimestamp.replace(microsecond=0).isoformat()
        mc = mac.Name.replace(':', '').upper()
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        fileName = f'{OUTPUT_DIR}/M0x{mc}__{fmt(dt)}__{fmt(tag)}.csv'
        numpy.savetxt(
            fileName,
            numpy.array([vv.x_abscissa, vv.y_ordinate]).T,
            fmt='%10.5f',
            delimiter=',',
            header=header,
        )
        print(f'\t{fileName}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ip', help='e.g. 192.168.10.100')
    args = parser.parse_args()
    try:
        ip = ip_address(args.ip)
    except ValueError:
        parser.print_help()
        exit(0)
    url: str = f'opc.tcp://{ip}:4840/freeopcua/server'
    asyncio.run(VibrationToCSV.dump(url=url))
