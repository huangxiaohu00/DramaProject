from __future__ import annotations

from typing import Dict, List, Tuple

import importlib

config = importlib.import_module("drama_pipeline.2_config")

from .beidou import BeidouClient
from .duole import DuoleClient
from .feishu import FeishuClient
from .material import MaterialClient
from .mobo import MoboClient


def build_today_clients(logger=None, material_enabled: bool = True) -> Dict[str, object]:
    return {
        "mobo_client": MoboClient(
            authorization=config.MOBO_DRAMA_AUTH or (config.MOBO_AUTHS[0] if config.MOBO_AUTHS else ""),
            logger=logger,
        ),
        "beidou_client": BeidouClient(authorization=config.BEIDOU_DRAMA_AUTH, logger=logger),
        "feishu_client": FeishuClient(
            app_id=config.FEISHU_APP_ID,
            app_secret=config.FEISHU_APP_SECRET,
            app_token=config.FEISHU_APP_TOKEN,
            tables=config.FEISHU_TABLES,
            logger=logger,
        ),
        "duole_client": DuoleClient(cookie=config.DUOLE_COOKIE, logger=logger),
        "material_client": MaterialClient(cookie=config.MATERIAL_COOKIE, logger=logger) if material_enabled else None,
    }


def build_order_clients(logger=None) -> Tuple[List[object], List[object]]:
    mobo_clients = []
    for index, authorization in enumerate(config.MOBO_AUTHS, 1):
        client = MoboClient(authorization=authorization, logger=logger)
        client.account = f"mobo_{index}"
        mobo_clients.append(client)

    beidou_clients = []
    for index, authorization in enumerate(config.BEIDOU_AUTHS, 1):
        client = BeidouClient(authorization=authorization, logger=logger)
        client.account = f"beidou_{index}"
        beidou_clients.append(client)
    return mobo_clients, beidou_clients
