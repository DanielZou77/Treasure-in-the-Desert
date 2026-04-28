from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from typing import Any

import duckdb

from Data_Center.cleaning import parse_datetime_input
from Data_Center.config import get_first_env, load_env, save_env_value
from Data_Center.database import connect_db
from Data_Center.display import print_all_tables
from Data_Center.features import calculate_and_store_features
from Data_Center.models import INTERVAL_CHOICES, INTERVAL_DELTAS, NewsProvider, PriceProvider, ProviderError
from Data_Center.news_data import ingest_news
from Data_Center.provider_registry import NEWS_PROVIDERS, PRICE_PROVIDERS
from Data_Center.self_test import run_self_test
from Data_Center.stock_data import ingest_stock_bars


def input_with_default(prompt: str, default: str | None = None) -> str:
    suffix = f" [{default}]" if default else ""
    value = input(f"{prompt}{suffix}: ").strip()
    return value or (default or "")


def prompt_int(prompt: str, default: int, minimum: int = 1, maximum: int = 1000) -> int:
    while True:
        value = input_with_default(prompt, str(default))
        try:
            parsed = int(value)
        except ValueError:
            print("请输入整数。")
            continue
        if minimum <= parsed <= maximum:
            return parsed
        print(f"请输入 {minimum} 到 {maximum} 之间的整数。")


def prompt_ticker(default: str = "AAPL") -> str:
    while True:
        ticker = input_with_default("股票代码", default).upper()
        if ticker:
            return ticker
        print("股票代码不能为空。")


def prompt_interval(provider: PriceProvider) -> str:
    print("\n可选K线级别：")
    for number, (interval, label) in INTERVAL_CHOICES.items():
        supported = "yes" if interval in provider.supported_intervals else "no"
        print(f"  {number}. {interval:<6} {label:<10} supported={supported}")
    while True:
        choice = input_with_default("请选择级别", "6")
        if choice in INTERVAL_CHOICES:
            interval = INTERVAL_CHOICES[choice][0]
            if interval in provider.supported_intervals:
                return interval
        print("该级别不可用，请重新选择。")


def prompt_time_range(interval: str, default_bars: int = 400) -> tuple[datetime, datetime]:
    end_default = datetime.now().replace(microsecond=0)
    start_default = end_default - INTERVAL_DELTAS[interval] * default_bars
    print("\n时间格式示例：2025-01-01 或 2025-01-01 09:30:00")
    print(f"直接回车默认取最近 {default_bars} 根 {interval} K线。")
    start_raw = input("开始时间: ").strip()
    end_raw = input("结束时间: ").strip()

    end = parse_datetime_input(end_raw, is_end=True) if end_raw else end_default
    start = parse_datetime_input(start_raw) if start_raw else end - INTERVAL_DELTAS[interval] * default_bars
    if start >= end:
        print("开始时间不能晚于结束时间，已自动改为默认最近400根。")
        end = end_default
        start = start_default
    return start, end


def prompt_news_time_range() -> tuple[datetime, datetime]:
    end_default = datetime.now().replace(microsecond=0)
    start_default = end_default - timedelta(days=7)
    print("\n新闻时间格式示例：2025-01-01 或 2025-01-01 09:30:00")
    print("直接回车默认最近7天。")
    start_raw = input("新闻开始时间: ").strip()
    end_raw = input("新闻结束时间: ").strip()
    end = parse_datetime_input(end_raw, is_end=True) if end_raw else end_default
    start = parse_datetime_input(start_raw) if start_raw else start_default
    if start >= end:
        print("开始时间不能晚于结束时间，已自动改为最近7天。")
        return start_default, end_default
    return start, end


def prompt_key(env: dict[str, str], env_name: str) -> str:
    value = input(f"请输入 {env_name}（输入会保存到当前进程）: ").strip()
    if not value:
        return ""
    env[env_name] = value
    os.environ[env_name] = value
    save_choice = input("是否写回根目录 .env？[y/N]: ").strip().lower()
    if save_choice == "y":
        save_env_value(env_name, value)
        print(f"{env_name} 已写入 .env。")
    return value


def choose_checked_provider(
    providers: dict[str, PriceProvider] | dict[str, NewsProvider],
    env: dict[str, str],
    default_name: str,
    title: str,
) -> tuple[Any, str] | tuple[None, None]:
    provider_names = list(providers)
    if default_name not in providers and provider_names:
        default_name = provider_names[0]

    while True:
        print(f"\n{title} API 提供商：")
        for idx, name in enumerate(provider_names, start=1):
            provider = providers[name]
            default_mark = " (default)" if name == default_name else ""
            print(f"  {idx}. {provider.display_name}{default_mark} - {provider.note}")
        print("  q. 返回主菜单")

        choice = input_with_default("请选择提供商", default_name).strip()
        if choice.lower() == "q":
            return None, None
        if choice.isdigit() and 1 <= int(choice) <= len(provider_names):
            provider = providers[provider_names[int(choice) - 1]]
        elif choice in providers:
            provider = providers[choice]
        else:
            print("没有这个提供商，请重新选择。")
            continue

        while True:
            env_name, key = get_first_env(env, provider.key_envs)
            if not key:
                print(f"未找到 {provider.display_name} 的 API key。候选环境变量：{', '.join(provider.key_envs)}")
                action = input("[1] 输入API key  [2] 换提供商  [3] 返回主菜单: ").strip()
                if action == "1":
                    key = prompt_key(env, env_name)
                    if not key:
                        continue
                elif action == "2":
                    break
                else:
                    return None, None

            print(f"正在检查 {provider.display_name} API 可用性...")
            result = provider.check(key)
            if result.ok:
                print(f"{provider.display_name} API 可用。")
                return provider, key

            print(f"{provider.display_name} API 不可用：{result.message}")
            action = input("[1] 换提供商  [2] 重新输入API key  [3] 返回主菜单: ").strip()
            if action == "1":
                break
            if action == "2":
                prompt_key(env, env_name)
                continue
            return None, None


def run_price_flow(con: duckdb.DuckDBPyConnection, env: dict[str, str]) -> None:
    default_provider = env.get("DEFAULT_PRICE_PROVIDER", "alpha_vantage")
    provider, key = choose_checked_provider(PRICE_PROVIDERS, env, default_provider, "行情")
    if provider is None:
        return

    ticker = prompt_ticker()
    interval = prompt_interval(provider)
    default_bars = 400
    if provider.name == "alpha_vantage" and interval == "1day":
        default_bars = 100
        print("\n提示：Alpha Vantage 免费 key 的日线接口只能返回最近约 100 条 compact 数据。")
        print("如需 400 根以上日线历史，可换用其他 provider 或升级 Alpha Vantage premium。")
    start, end = prompt_time_range(interval, default_bars=default_bars)

    print("\n先检查数据库中是否已有可复用数据...")
    try:
        derived, inserted = ingest_stock_bars(con, provider, key, ticker, interval, start, end)
    except ProviderError as exc:
        print(f"行情数据获取失败：{exc}")
        print("可以尝试缩短时间区间、换一家 API 提供商，或检查当前 API key 的套餐权限。")
        return

    if derived:
        print(f"已从更细粒度K线聚合生成 {derived} 条 {interval} 数据。")
    print(f"新增行情数据：{inserted} 条。")

    factor_count = calculate_and_store_features(con, ticker, interval)
    print(f"特征工程完成：写入/刷新 {factor_count} 条技术因子数值。")


def run_news_flow(con: duckdb.DuckDBPyConnection, env: dict[str, str]) -> None:
    default_provider = env.get("DEFAULT_NEWS_PROVIDER", "alpha_vantage")
    provider, key = choose_checked_provider(NEWS_PROVIDERS, env, default_provider, "新闻")
    if provider is None:
        return

    ticker = prompt_ticker()
    start, end = prompt_news_time_range()
    limit = prompt_int("最多获取新闻条数", 20, minimum=1, maximum=1000)
    print(f"从 {provider.display_name} 拉取 {ticker} 新闻: {start} -> {end}")
    try:
        inserted = ingest_news(con, provider, key, ticker, start, end, limit)
    except ProviderError as exc:
        print(f"新闻数据获取失败：{exc}")
        print("可以尝试缩短时间区间、换一家新闻 API 提供商，或检查当前 API key 的套餐权限。")
        return
    print(f"新增新闻：{inserted} 条。Summary 列保留为空，后续交给 AI 摘要模块填充。")


def run_cli() -> None:
    env = load_env()
    con, db_path = connect_db(env)
    print(f"Data Center 已连接：{db_path}")
    try:
        while True:
            print("\n" + "=" * 88)
            print("Data Center 测试台")
            print("1. 量价与因子表：获取K线并计算技术指标")
            print("2. 新闻主表：获取新闻")
            print("3. 打印五张核心表")
            print("4. 退出")
            choice = input_with_default("请选择功能", "3")
            if choice == "1":
                run_price_flow(con, env)
            elif choice == "2":
                run_news_flow(con, env)
            elif choice == "3":
                max_rows = prompt_int("每张表最多显示行数", 20, minimum=1, maximum=200)
                print_all_tables(con, max_rows=max_rows)
            elif choice == "4":
                break
            else:
                print("未知选项，请重新输入。")
    finally:
        con.close()


def main() -> None:
    if "--self-test" in sys.argv:
        run_self_test()
        return
    run_cli()
