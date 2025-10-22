from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .alerting import AlertManager
from .backtest import BacktestOverrides, BacktestReport, BacktestRunner
from .config import Settings, get_settings
from .data_source import FileMetricsRepository, MetricsRepositoryError
from .metrics_service import MetricsService
from .models import AggregatedMetrics, AlertDispatchResult, HealthResponse

app = FastAPI(title="Ingestion Monitoring Service", version="0.1.0")

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

    return PlainTextResponse("\n".join(lines) + "\n")
