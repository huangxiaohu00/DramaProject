from __future__ import annotations

import importlib
import argparse
import sys
from concurrent.futures import ThreadPoolExecutor
from collections import OrderedDict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Tuple


if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


models = importlib.import_module("drama_pipeline.3_models")
excel_io = importlib.import_module("drama_pipeline.5_excel_io")
config = importlib.import_module("drama_pipeline.2_config")
client_factory = importlib.import_module("drama_pipeline.platform_clients.factory")
runtime = importlib.import_module("drama_pipeline.10_runtime")
runtime.configure_utf8_runtime()


OrderBundle = Dict[str, list]
ORDER_OUTPUT_HEADERS = ["语言", "剧场", "剧名", "订单数", "金额", "平台", "类型"]
ORDER_SUMMARY_SHEET = "yesterday_orders_summary"
AD_ORDER_TYPE = "广告金额"
AD_DETAIL_AMOUNT_THRESHOLD = 10.0

def collect_order_inputs(
    mobo_clients: Iterable[object],
    beidou_clients: Iterable[object],    begin_date: str,
    end_date: str,
    logger=None,
) -> OrderBundle:
    mobo_clients = list(mobo_clients)
    beidou_clients = list(beidou_clients)
    mobo_orders: List[models.OrderRecord] = []
    beidou_orders: List[models.OrderRecord] = []
    errors: List[Dict[str, str]] = []
    _log_info(logger, f"[订单] 开始采集: Mobo账号={len(mobo_clients)} 北斗账号={len(beidou_clients)} 日期={begin_date}~{end_date}")

    with ThreadPoolExecutor(max_workers=max(1, len(mobo_clients) + len(beidou_clients))) as executor:
        mobo_futures = [(client, executor.submit(_fetch_order_rows, client, begin_date, end_date)) for client in mobo_clients]
        beidou_futures = [(client, executor.submit(_fetch_order_rows, client, begin_date, end_date)) for client in beidou_clients]

        for client, future in mobo_futures:
            account = getattr(client, "account", "")
            try:
                rows = future.result()
                mobo_orders.extend(rows)
                _log_info(logger, _format_account_fetch_log("Mobo", account, rows, client))
            except Exception as exc:
                errors.append(_order_error("Mobo", account, exc))
                _log_warning(logger, f"[订单] Mobo账号 {account} 失败: {type(exc).__name__}: {exc}")

        for client, future in beidou_futures:
            account = getattr(client, "account", "")
            try:
                rows = future.result()
                beidou_orders.extend(rows)
                _log_info(logger, _format_account_fetch_log("北斗", account, rows, client))
            except Exception as exc:
                errors.append(_order_error("Beidou", account, exc))
                _log_warning(logger, f"[订单] 北斗账号 {account} 失败: {type(exc).__name__}: {exc}")

    summary = aggregate_orders([*mobo_orders, *beidou_orders])
    return {
        "mobo_orders": mobo_orders,
        "beidou_orders": beidou_orders,
        "summary": summary,
        "category_summary": build_order_category_summary(summary),
        "errors": errors,
    }


def _fetch_order_rows(client, begin_date: str, end_date: str) -> List[models.OrderRecord]:
    account = getattr(client, "account", "")
    return client.fetch_orders(begin_date, end_date, account=account)


def _format_account_fetch_log(platform: str, account: str, rows: List[models.OrderRecord], client) -> str:
    stats = getattr(client, "last_order_fetch_stats", {}) or {}
    raw_count = int(stats.get("raw_count", len(rows)) or 0)
    parsed_count = int(stats.get("parsed_count", len(rows)) or 0)
    total_count = int(stats.get("total_count", raw_count) or 0)
    amount_sum = round(float(stats.get("amount_sum", sum(float(row.amount or 0.0) for row in rows)) or 0.0), 2)
    return (
        f"[订单] {platform}账号 {account} 完成: rows={len(rows)} "
        f"raw_count={raw_count} parsed_count={parsed_count} "
        f"page_total={total_count} amount_sum={amount_sum}"
    )


def export_order_stage_files(bundle: Mapping[str, list], output_root: Path, date_text: str) -> Dict[str, Path]:
    path = Path(output_root) / f"yesterday_orders_summary_{date_text}.xlsx"
    rows = [row.to_dict() if hasattr(row, "to_dict") else dict(row) for row in bundle.get("summary", [])]
    excel_io.write_workbook(
        path,
        {ORDER_SUMMARY_SHEET: rows},
        sheet_headers={ORDER_SUMMARY_SHEET: ORDER_OUTPUT_HEADERS},
    )
    return {"summary": path}


def _order_error(platform: str, account: str, exc: Exception) -> Dict[str, str]:
    return {
        "平台": platform,
        "账号": account,
        "错误类型": type(exc).__name__,
        "错误信息": str(exc),
    }


def build_default_order_clients(logger=None) -> Tuple[List[object], List[object]]:
    return client_factory.build_order_clients(logger=logger)


def run_yesterday_orders(
    begin_date: str,
    end_date: str,
    output_root: Path | None = None,
    mobo_clients: Iterable[object] | None = None,
    beidou_clients: Iterable[object] | None = None,
    logger=None,
) -> Dict[str, Path]:
    if mobo_clients is None or beidou_clients is None:
        default_mobo, default_beidou = build_default_order_clients(logger=logger)
        if mobo_clients is None:
            mobo_clients = default_mobo
        if beidou_clients is None:
            beidou_clients = default_beidou
    root = output_root or config.ORDER_OUTPUT_ROOT
    if logger is not None:
        logger.set_run_date(begin_date)
        logger.step("开始采集昨日订单")
    bundle = collect_order_inputs(mobo_clients, beidou_clients, begin_date, end_date, logger=logger)
    if logger is not None:
        logger.info(
            f"订单明细: mobo={len(bundle.get('mobo_orders', []))}, "
            f"beidou={len(bundle.get('beidou_orders', []))}, "
            f"errors={len(bundle.get('errors', []))}"
        )
        if bundle.get("errors"):
            for message in summarize_order_errors(bundle.get("errors", [])):
                logger.warning(message)
            logger.warning(f"昨日订单存在 {len(bundle.get('errors', []))} 个账号错误")
        for row in bundle.get("category_summary", []):
            logger.info(f"订单汇总: 平台={row['平台']} 类型={row['类型']} 订单数={row['订单数']} 金额={row['金额']}")
        logger.step("写出昨日订单结果")
    return export_order_stage_files(bundle, Path(root), begin_date)


def parse_args(argv: List[str] | None = None):
    parser = argparse.ArgumentParser(description="昨日出单汇总")
    parser.add_argument("--begin", default="", help="开始日期，格式 YYYY-MM-DD；不填则弹窗选择")
    parser.add_argument("--end", default="", help="结束日期，格式 YYYY-MM-DD；不填则弹窗选择")
    args = parser.parse_args(argv)
    if bool(args.begin) != bool(args.end):
        parser.error("--begin 和 --end 必须同时提供；都不提供时会弹窗选择")
    return args


def resolve_order_date_range(begin_date: str = "", end_date: str = "") -> Tuple[str, str]:
    begin = str(begin_date or "").strip()
    end = str(end_date or "").strip()
    if begin and end:
        _validate_date_text(begin, "--begin")
        _validate_date_text(end, "--end")
        return begin, end
    default_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    return prompt_order_date_range(default_date, default_date)


def prompt_order_date_range(default_begin: str, default_end: str) -> Tuple[str, str]:
    try:
        import tkinter as tk
        from tkinter import messagebox
    except Exception:
        return default_begin, default_end

    result = {"begin": "", "end": ""}
    root = tk.Tk()
    root.title("选择昨日订单日期")
    root.resizable(False, False)

    tk.Label(root, text="开始日期 YYYY-MM-DD").grid(row=0, column=0, padx=12, pady=(12, 6), sticky="w")
    begin_var = tk.StringVar(value=default_begin)
    tk.Entry(root, textvariable=begin_var, width=18).grid(row=0, column=1, padx=12, pady=(12, 6))

    tk.Label(root, text="结束日期 YYYY-MM-DD").grid(row=1, column=0, padx=12, pady=6, sticky="w")
    end_var = tk.StringVar(value=default_end)
    tk.Entry(root, textvariable=end_var, width=18).grid(row=1, column=1, padx=12, pady=6)

    def submit() -> None:
        begin = begin_var.get().strip()
        end = end_var.get().strip()
        try:
            _validate_date_text(begin, "开始日期")
            _validate_date_text(end, "结束日期")
        except ValueError as exc:
            messagebox.showerror("日期格式错误", str(exc), parent=root)
            return
        result["begin"] = begin
        result["end"] = end
        root.destroy()

    def cancel() -> None:
        root.destroy()

    button_frame = tk.Frame(root)
    button_frame.grid(row=2, column=0, columnspan=2, pady=(8, 12))
    tk.Button(button_frame, text="确定", width=10, command=submit).pack(side="left", padx=6)
    tk.Button(button_frame, text="取消", width=10, command=cancel).pack(side="left", padx=6)
    root.bind("<Return>", lambda _event: submit())
    root.protocol("WM_DELETE_WINDOW", cancel)
    root.mainloop()

    if not result["begin"] or not result["end"]:
        raise SystemExit("已取消日期选择")
    return result["begin"], result["end"]


def _validate_date_text(value: str, field_name: str) -> None:
    try:
        datetime.strptime(str(value or "").strip(), "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{field_name} 必须是 YYYY-MM-DD 格式") from exc


def aggregate_orders(rows: Iterable[models.OrderRecord]) -> List[models.OrderRecord]:
    grouped: "OrderedDict[Tuple[str, str, str, str, str], models.OrderRecord]" = OrderedDict()
    account_lists: Dict[Tuple[str, str, str, str, str], List[str]] = {}

    for row in normalize_ad_order_rows(rows):
        key = row.match_key
        if key not in grouped:
            grouped[key] = models.OrderRecord(
                date=row.date,
                title=row.title,
                platform=row.platform,
                language=row.language,
                theater=row.theater,
                order_count=0,
                amount=0.0,
                account="",
                task_id=row.task_id,
                order_type=row.order_type,
                raw=dict(row.raw),
            )
            account_lists[key] = []

        target = grouped[key]
        target.order_count += int(row.order_count or 0)
        target.amount = round(target.amount + float(row.amount or 0), 2)
        if row.account and row.account not in account_lists[key]:
            account_lists[key].append(row.account)
        target.account = ",".join(account_lists[key])

    return list(grouped.values())


def normalize_ad_order_rows(rows: Iterable[models.OrderRecord]) -> List[models.OrderRecord]:
    output: List[models.OrderRecord] = []
    for row in rows:
        if row.order_type != AD_ORDER_TYPE:
            output.append(row)
            continue

        amount = round(float(row.amount or 0.0), 2)
        if amount >= AD_DETAIL_AMOUNT_THRESHOLD:
            output.append(_copy_order_record(row, order_count=1, amount=amount))
            continue

        output.append(
            _copy_order_record(
                row,
                title="其它",
                language="全部",
                theater="全部",
                order_count=1,
                amount=amount,
            )
        )
    return output


def _copy_order_record(row: models.OrderRecord, **overrides) -> models.OrderRecord:
    values = {
        "date": row.date,
        "title": row.title,
        "platform": row.platform,
        "language": row.language,
        "theater": row.theater,
        "order_count": row.order_count,
        "amount": row.amount,
        "account": row.account,
        "task_id": row.task_id,
        "order_type": row.order_type,
        "raw": dict(row.raw),
    }
    values.update(overrides)
    return models.OrderRecord(**values)


def build_order_category_summary(rows: Iterable[models.OrderRecord]) -> List[Dict[str, object]]:
    grouped: "OrderedDict[Tuple[str, str], Dict[str, object]]" = OrderedDict()
    total_orders = 0
    total_amount = 0.0
    for row in rows:
        key = (row.platform, row.order_type)
        if key not in grouped:
            grouped[key] = {"平台": row.platform, "类型": row.order_type, "订单数": 0, "金额": 0.0}
        grouped[key]["订单数"] = int(grouped[key]["订单数"]) + int(row.order_count or 0)
        grouped[key]["金额"] = round(float(grouped[key]["金额"]) + float(row.amount or 0.0), 2)
        total_orders += int(row.order_count or 0)
        total_amount = round(total_amount + float(row.amount or 0.0), 2)
    output = list(grouped.values())
    output.append({"平台": "全部", "类型": "全部", "订单数": total_orders, "金额": round(total_amount, 2)})
    return output


def summarize_order_errors(errors: Iterable[Mapping[str, str]]) -> List[str]:
    grouped: "OrderedDict[str, List[str]]" = OrderedDict()
    for error in errors:
        platform = str(error.get("平台") or "未知平台")
        account = str(error.get("账号") or "未知账号")
        grouped.setdefault(platform, []).append(account)
    return [
        f"账号错误汇总: 平台={platform} count={len(accounts)} accounts={','.join(accounts)}"
        for platform, accounts in grouped.items()
    ]


def _log_info(logger, message: str) -> None:
    if logger is not None and hasattr(logger, "info"):
        logger.info(message)


def _log_warning(logger, message: str) -> None:
    if logger is not None and hasattr(logger, "warning"):
        logger.warning(message)


def export_orders(rows: Iterable[models.OrderRecord], output_root: Path, date_text: str) -> Path:
    output_path = Path(output_root) / f"yesterday_orders_summary_{date_text}.xlsx"
    excel_io.write_workbook(
        output_path,
        {ORDER_SUMMARY_SHEET: [row.to_dict() for row in rows]},
        sheet_headers={ORDER_SUMMARY_SHEET: ORDER_OUTPUT_HEADERS},
    )
    return output_path


def main() -> None:
    args = parse_args()
    begin_date, end_date = resolve_order_date_range(args.begin, args.end)
    logger = runtime.PipelineLogger("yesterday_orders", run_date=begin_date)
    exit_code = 0
    try:
        runtime.bootstrap_runtime(config_module=config, logger=logger)
        paths = run_yesterday_orders(begin_date, end_date, logger=logger)
        print("昨日出单输出完成:", flush=True)
        for key, path in paths.items():
            print(f"  {key}: {path}", flush=True)
    except Exception as exc:
        logger.exception(exc, "昨日订单执行失败")
        log_path = logger.flush_if_needed()
        if log_path is not None:
            print(f"  debug_log: {log_path}", flush=True)
        if runtime.is_frozen_exe():
            import traceback

            traceback.print_exc()
            exit_code = 1
        else:
            raise
    else:
        log_path = logger.flush_if_needed()
        if log_path is not None:
            print(f"  debug_log: {log_path}", flush=True)
    finally:
        runtime.pause_before_exit()
    if exit_code:
        raise SystemExit(exit_code)



if __name__ == "__main__":
    main()
