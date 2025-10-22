from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import AsyncGenerator, Dict, List, Optional
import logging

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .alerting import AlertManager
from .backtest import BacktestOverrides, BacktestReport, BacktestRunner
from .config import Settings, get_settings
from .data_source import FileMetricsRepository, MetricsRepositoryError
from .governance import SignalGovernance, TelegramNotifier
from .indicator_models import CvdCurveResponse, DeltaOiCurveResponse, VolumeProfileStatsResponse
from .indicator_repository import (
    IndicatorRepository,
    IndicatorRepositoryError,
    IndicatorSeriesNotFoundError,
)
from .indicator_service import IndicatorCache, IndicatorService
from .market_data import MarketDataError, MarketDataRepository
from .market_models import MarketSnapshot, SignalFeed, SignalFeedItem, SignalDebugReport
from .metrics_service import MetricsService
from .models import AggregatedMetrics, AlertDispatchResult, GovernanceStatus, HealthResponse
from .signal_alerts import SignalAlertPipeline

app = FastAPI(title="Ingestion Monitoring Service", version="0.1.0")
logger = logging.getLogger(__name__)

_governance_instance: Optional[SignalGovernance] = None
_signal_alerts_instance: Optional[SignalAlertPipeline] = None
_market_data_repository: Optional[MarketDataRepository] = None
_indicator_cache_instance: Optional[IndicatorCache] = None

templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))
static_dir = Path(__file__).resolve().parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


def get_repository(settings: Settings = Depends(get_settings)) -> FileMetricsRepository:
    return FileMetricsRepository(settings.metrics_snapshot_path)


def get_metrics_service(
    repo: FileMetricsRepository = Depends(get_repository),
    settings: Settings = Depends(get_settings),
) -> MetricsService:
    return MetricsService(repository=repo, settings=settings)


def get_alert_manager(settings: Settings = Depends(get_settings)) -> AlertManager:
    return AlertManager(settings)


def get_backtest_runner(
    repository: FileMetricsRepository = Depends(get_repository),
    settings: Settings = Depends(get_settings),
) -> BacktestRunner:
    return BacktestRunner(repository=repository, settings=settings)


def get_signal_governance(
    settings: Settings = Depends(get_settings),
) -> SignalGovernance:
    global _governance_instance
    if _governance_instance is None:
        notifier = TelegramNotifier(
            settings.telegram_bot_token,
            settings.telegram_chat_id,
        )
        _governance_instance = SignalGovernance(
            rules=settings.governance_rules,
            notifier=notifier,
        )
    return _governance_instance


def get_signal_alerts(
    settings: Settings = Depends(get_settings),
) -> SignalAlertPipeline:
    global _signal_alerts_instance
    if _signal_alerts_instance is None:
        _signal_alerts_instance = SignalAlertPipeline(settings)
    return _signal_alerts_instance


def get_market_data_repository() -> MarketDataRepository:
    global _market_data_repository
    if _market_data_repository is None:
        _market_data_repository = MarketDataRepository()
    return _market_data_repository


def get_indicator_repository(settings: Settings = Depends(get_settings)) -> IndicatorRepository:
    return IndicatorRepository(settings.indicator_snapshot_path)


def get_indicator_cache(settings: Settings = Depends(get_settings)) -> IndicatorCache:
    global _indicator_cache_instance
    if _indicator_cache_instance is None:
        _indicator_cache_instance = IndicatorCache(
            redis_url=settings.redis_url,
            ttl_seconds=settings.indicator_cache_ttl_seconds,
        )
    return _indicator_cache_instance


def get_indicator_service(
    repository: IndicatorRepository = Depends(get_indicator_repository),
    cache: IndicatorCache = Depends(get_indicator_cache),
) -> IndicatorService:
    return IndicatorService(repository=repository, cache=cache)


@app.get("/", include_in_schema=False)
def index() -> Dict[str, str]:
    return {"message": "Ingestion monitoring service", "dashboard": "/dashboard"}


@app.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
def dashboard(
    request: Request,
    service: MetricsService = Depends(get_metrics_service),
) -> HTMLResponse:
    try:
        metrics = service.collect()
    except MetricsRepositoryError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "metrics": metrics,
            "summary": metrics.ingestion.status,
        },
    )


@app.get("/api/v1/metrics", response_model=AggregatedMetrics)
def metrics(service: MetricsService = Depends(get_metrics_service)) -> AggregatedMetrics:
    try:
        return service.collect()
    except MetricsRepositoryError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc


@app.get("/api/v1/health", response_model=HealthResponse)
def health(service: MetricsService = Depends(get_metrics_service)) -> HealthResponse:
    try:
        return service.health()
    except MetricsRepositoryError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc


@app.get("/api/v1/governance", response_model=GovernanceStatus)
def governance_status(
    repository: FileMetricsRepository = Depends(get_repository),
    governance: SignalGovernance = Depends(get_signal_governance),
) -> GovernanceStatus:
    try:
        snapshot = repository.fetch_snapshot()
    except MetricsRepositoryError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return governance.evaluate(snapshot)


@app.post("/api/v1/alerts/evaluate", response_model=AlertDispatchResult)
def trigger_alert(
    service: MetricsService = Depends(get_metrics_service),
    alert_manager: AlertManager = Depends(get_alert_manager),
) -> AlertDispatchResult:
    try:
        metrics = service.collect()
    except MetricsRepositoryError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return alert_manager.dispatch(metrics)


@app.post("/api/v1/alerts/signals")
def trigger_signal_alerts(
    repository: FileMetricsRepository = Depends(get_repository),
    pipeline: SignalAlertPipeline = Depends(get_signal_alerts),
) -> Dict[str, object]:
    try:
        snapshot = repository.fetch_snapshot()
    except MetricsRepositoryError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    delivered = pipeline.process(snapshot)
    return {"delivered_count": len(delivered), "delivered_ids": delivered}


@app.get(
    "/api/v1/indicators/cvd",
    response_model=CvdCurveResponse,
    tags=["Indicators"],
    summary="Retrieve cumulative volume delta curve",
    responses={
        404: {"description": "Requested CVD series not found"},
        503: {"description": "Indicator datastore unavailable"},
    },
)
def indicator_cvd(
    symbol: str = Query(
        ...,
        description="Instrument symbol to query (e.g. BTCUSDT).",
        examples={"perp": {"summary": "BTC perp", "value": "BTCUSDT"}},
    ),
    timeframe: str = Query(
        ...,
        description="Aggregation timeframe (e.g. 5m, 15m, 1h).",
        examples={"intraday": {"summary": "Five minute", "value": "5m"}},
    ),
    session: Optional[str] = Query(
        default=None,
        description="Optional trading session filter (asia, london, new_york).",
        examples={"ny": {"summary": "New York session", "value": "new_york"}},
    ),
    service: IndicatorService = Depends(get_indicator_service),
) -> CvdCurveResponse:
    """Expose the CVD curve used by the UI overlays and signal engine."""

    try:
        return service.cvd_curve(symbol=symbol, timeframe=timeframe, session=session)
    except IndicatorSeriesNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except IndicatorRepositoryError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc


@app.get(
    "/api/v1/indicators/delta-oi",
    response_model=DeltaOiCurveResponse,
    tags=["Indicators"],
    summary="Retrieve delta open interest percentage curve",
    responses={
        404: {"description": "Requested ΔOI% series not found"},
        503: {"description": "Indicator datastore unavailable"},
    },
)
def indicator_delta_oi(
    symbol: str = Query(
        ...,
        description="Instrument symbol to query (e.g. BTCUSDT).",
        examples={"perp": {"summary": "BTC perp", "value": "BTCUSDT"}},
    ),
    timeframe: str = Query(
        ...,
        description="Aggregation timeframe (e.g. 5m, 15m, 1h).",
        examples={"intraday": {"summary": "Five minute", "value": "5m"}},
    ),
    session: Optional[str] = Query(
        default=None,
        description="Optional trading session filter (asia, london, new_york).",
        examples={"asia": {"summary": "Asia session", "value": "asia"}},
    ),
    service: IndicatorService = Depends(get_indicator_service),
) -> DeltaOiCurveResponse:
    """Expose delta open interest percentage traces for downstream consumption."""

    try:
        return service.delta_oi_percent(symbol=symbol, timeframe=timeframe, session=session)
    except IndicatorSeriesNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except IndicatorRepositoryError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc


@app.get(
    "/api/v1/indicators/volume-profile",
    response_model=VolumeProfileStatsResponse,
    tags=["Indicators"],
    summary="Retrieve volume profile statistics",
    responses={
        404: {"description": "Requested volume profile not found"},
        503: {"description": "Indicator datastore unavailable"},
    },
)
def indicator_volume_profile(
    symbol: str = Query(
        ...,
        description="Instrument symbol to query (e.g. BTCUSDT).",
        examples={"perp": {"summary": "BTC perp", "value": "BTCUSDT"}},
    ),
    timeframe: str = Query(
        ...,
        description="Aggregation timeframe (e.g. 5m, 15m, 1h).",
        examples={"session": {"summary": "Session profile", "value": "5m"}},
    ),
    session: Optional[str] = Query(
        default=None,
        description="Optional trading session filter (asia, london, new_york).",
        examples={"london": {"summary": "London session", "value": "london"}},
    ),
    service: IndicatorService = Depends(get_indicator_service),
) -> VolumeProfileStatsResponse:
    """Expose value area, VWAP and distribution statistics for a slice."""

    try:
        return service.volume_profile(symbol=symbol, timeframe=timeframe, session=session)
    except IndicatorSeriesNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except IndicatorRepositoryError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc


@app.get("/api/v1/markets", response_model=MarketSnapshot)
def market_snapshot(
    symbols: Optional[List[str]] = Query(
        default=None,
        description="Symbols to include in the snapshot. Defaults to all tracked instruments.",
    ),
    repository: MarketDataRepository = Depends(get_market_data_repository),
) -> MarketSnapshot:
    try:
        snapshot = repository.market_snapshot(symbols=symbols)
    except MarketDataError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    return snapshot


@app.get("/api/v1/signals/feed", response_model=SignalFeed)
def signal_feed(
    symbol: Optional[str] = Query(
        default=None,
        description="Filter feed results to a single symbol (e.g. BTCUSDT).",
    ),
    confidence: Optional[str] = Query(
        default=None,
        description="Filter by confidence tag (low, medium, high).",
    ),
    session: Optional[str] = Query(
        default=None,
        description="Filter by trading session label (asia, london, new_york).",
    ),
    repository: MarketDataRepository = Depends(get_market_data_repository),
) -> SignalFeed:
    try:
        feed = repository.signal_feed(symbol=symbol, confidence=confidence, session=session)
    except MarketDataError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    return feed


@app.get("/api/v1/signals/{signal_id}", response_model=SignalFeedItem)
def signal_by_id(
    signal_id: int,
    repository: MarketDataRepository = Depends(get_market_data_repository),
) -> SignalFeedItem:
    try:
        return repository.signal_by_id(signal_id)
    except MarketDataError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@app.get("/api/v1/signals/{signal_id}/debug", response_model=SignalDebugReport)
def signal_debug(
    signal_id: int,
    repository: MarketDataRepository = Depends(get_market_data_repository),
) -> SignalDebugReport:
    try:
        return repository.debug_signal(signal_id)
    except MarketDataError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@app.get("/api/v1/signals/stream")
async def signal_stream(
    repository: MarketDataRepository = Depends(get_market_data_repository),
) -> StreamingResponse:
    try:
        items = repository.stream_items()
    except MarketDataError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    async def event_iterator() -> AsyncGenerator[str, None]:
        if not items:
            while True:
                await asyncio.sleep(15)
                yield "event: heartbeat\ndata: {}\n\n"

        for item in items:
            logger.info("Streaming signal event id=%s symbol=%s", item.id, item.symbol)
            payload = {"signal": item.model_dump(mode="json")}
            data = json.dumps(payload)
            yield f"event: signal\ndata: {data}\n\n"
            await asyncio.sleep(0.5)

        while True:
            await asyncio.sleep(15)
            yield "event: heartbeat\ndata: {}\n\n"

    return StreamingResponse(event_iterator(), media_type="text/event-stream")


@app.get("/api/v1/backtests/report", response_model=BacktestReport)
def backtest_report(
    windows: List[int] = Query(
        default=[30, 60, 90],
        description="Window lengths (in days) to evaluate. Provide multiple values to compute several windows.",
    ),
    win_return_threshold: Optional[float] = Query(
        default=None,
        description="Override minimum fractional return required to treat a trade as a win.",
    ),
    loss_return_threshold: Optional[float] = Query(
        default=None,
        description="Override maximum fractional return tolerated before classifying a trade as a loss.",
    ),
    min_trade_count: Optional[int] = Query(
        default=None,
        ge=0,
        description="Minimum trades required for a window to be considered a sufficient sample.",
    ),
    min_win_rate: Optional[float] = Query(
        default=None,
        ge=0.0,
        le=1.0,
        description="Target win rate expectation used when highlighting performance.",
    ),
    runner: BacktestRunner = Depends(get_backtest_runner),
) -> BacktestReport:
    overrides = BacktestOverrides(
        win_return_threshold=win_return_threshold,
        loss_return_threshold=loss_return_threshold,
        min_trade_count=min_trade_count,
        min_win_rate=min_win_rate,
    )
    try:
        return runner.run(windows=windows, overrides=overrides)
    except MetricsRepositoryError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@app.get("/api/v1/backtests/report/download", response_class=StreamingResponse)
def download_backtest_report(
    windows: List[int] = Query(
        default=[30, 60, 90],
        description="Window lengths (in days) to evaluate. Provide multiple values to compute several windows.",
    ),
    win_return_threshold: Optional[float] = Query(
        default=None,
        description="Override minimum fractional return required to treat a trade as a win.",
    ),
    loss_return_threshold: Optional[float] = Query(
        default=None,
        description="Override maximum fractional return tolerated before classifying a trade as a loss.",
    ),
    min_trade_count: Optional[int] = Query(
        default=None,
        ge=0,
        description="Minimum trades required for a window to be considered a sufficient sample.",
    ),
    min_win_rate: Optional[float] = Query(
        default=None,
        ge=0.0,
        le=1.0,
        description="Target win rate expectation used when highlighting performance.",
    ),
    runner: BacktestRunner = Depends(get_backtest_runner),
) -> StreamingResponse:
    overrides = BacktestOverrides(
        win_return_threshold=win_return_threshold,
        loss_return_threshold=loss_return_threshold,
        min_trade_count=min_trade_count,
        min_win_rate=min_win_rate,
    )
    try:
        report = runner.run(windows=windows, overrides=overrides)
    except MetricsRepositoryError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    csv_payload = runner.to_csv(report)
    filename = f"backtest_{report.generated_at.strftime('%Y%m%dT%H%M%SZ')}.csv"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(
        iter([csv_payload.encode("utf-8")]),
        media_type="text/csv",
        headers=headers,
    )


@app.get("/metrics/prometheus", response_class=PlainTextResponse, include_in_schema=False)
def prometheus_metrics(service: MetricsService = Depends(get_metrics_service)) -> PlainTextResponse:
    try:
        metrics = service.collect()
    except MetricsRepositoryError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    lines = [
        "# HELP ingestion_latency_seconds Last recorded ingestion latency seconds",
        "# TYPE ingestion_latency_seconds gauge",
        f"ingestion_latency_seconds {metrics.ingestion.current_latency_seconds or 0}",
        "# HELP ingestion_gap_seconds Time since last ingestion event in seconds",
        "# TYPE ingestion_gap_seconds gauge",
        f"ingestion_gap_seconds {metrics.ingestion.time_since_last_event_seconds or 0}",
        "# HELP ingestion_latency_seconds_max Maximum latency observed across sources",
        "# TYPE ingestion_latency_seconds_max gauge",
        f"ingestion_latency_seconds_max {metrics.ingestion.max_latency_seconds or 0}",
        "# HELP signals_generated_total Total signals tracked",
        "# TYPE signals_generated_total gauge",
        f"signals_generated_total {metrics.signals.total}",
        "# HELP signals_last_hour Count of signals generated in the last hour",
        "# TYPE signals_last_hour gauge",
        f"signals_last_hour {metrics.signals.last_60_minutes}",
        "# HELP strategy_win_rate Strategy win rate as a ratio",
        "# TYPE strategy_win_rate gauge",
        f"strategy_win_rate {metrics.performance.win_rate}",
        "# HELP strategy_average_return Strategy average return percentage",
        "# TYPE strategy_average_return gauge",
        f"strategy_average_return {metrics.performance.avg_return_pct}",
    ]

    for source_metric in metrics.ingestion.sources:
        lines.append(
            f"ingestion_source_latency_seconds{{source=\"{source_metric.source}\"}} {source_metric.latency_seconds}"
        )

    for status_name, count in metrics.signals.by_status.items():
        lines.append(
            f"signals_by_status_total{{status=\"{status_name}\"}} {count}"
        )

    for setup_name, count in metrics.signals.by_setup.items():
        lines.append(
            f"signals_by_setup_total{{setup=\"{setup_name}\"}} {count}"
        )

    for conf_name, count in metrics.signals.confidence_breakdown.items():
        lines.append(
            f"signals_confidence_total{{confidence=\"{conf_name}\"}} {count}"
        )

    return PlainTextResponse("\n".join(lines) + "\n")
