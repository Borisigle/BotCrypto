from __future__ import annotations

import json
import logging
import time
from threading import Lock
from typing import Any, Optional, Type

from pydantic import BaseModel

from .indicator_models import CvdCurveResponse, DeltaOiCurveResponse, VolumeProfileStatsResponse
from .indicator_repository import IndicatorRepository

logger = logging.getLogger(__name__)


class IndicatorCache:
    """Simple TTL cache that prefers Redis but falls back to an in-memory store."""

    def __init__(self, redis_url: Optional[str] = None, ttl_seconds: int = 30) -> None:
        self._ttl = max(int(ttl_seconds), 1)
        self._redis = self._init_redis(redis_url)
        self._store: dict[str, tuple[float, str]] = {}
        self._lock = Lock()

    def _init_redis(self, redis_url: Optional[str]) -> Optional[Any]:
        if not redis_url:
            return None

        try:  # pragma: no cover - optional dependency, exercised in production only
            import redis  # type: ignore
        except Exception as exc:
            logger.warning("Redis client unavailable (%s); using in-memory indicator cache", exc)
            return None

        try:
            client = redis.Redis.from_url(redis_url, decode_responses=True)
            client.ping()
        except Exception as exc:  # pragma: no cover - depends on runtime environment
            logger.warning("Redis ping failed (%s); using in-memory indicator cache", exc)
            return None

        logger.info("Using Redis-backed cache for indicator responses")
        return client

    def _now(self) -> float:
        return time.time()

    def get(self, key: str, model_cls: Type[BaseModel]) -> Optional[BaseModel]:
        payload: Optional[str]
        if self._redis is not None:
            payload = self._redis.get(key)
        else:
            with self._lock:
                record = self._store.get(key)
                if not record:
                    return None
                expires_at, payload = record
                if expires_at <= self._now():
                    del self._store[key]
                    return None

        if not payload:
            return None

        try:
            data = json.loads(payload)
            return model_cls.model_validate(data)
        except Exception:  # pragma: no cover - defensive clear path
            logger.exception("Failed to decode cached payload for key %s", key)
            if self._redis is not None:
                try:
                    self._redis.delete(key)
                except Exception:
                    logger.debug("Failed to delete invalid Redis entry for key %s", key)
            else:
                with self._lock:
                    self._store.pop(key, None)
            return None

    def set(self, key: str, value: BaseModel) -> None:
        payload = value.model_dump_json()
        if self._redis is not None:
            try:
                self._redis.setex(key, self._ttl, payload)
                return
            except Exception as exc:  # pragma: no cover - defensive fallback
                logger.warning("Failed to persist indicator payload to Redis (%s)", exc)

        with self._lock:
            self._store[key] = (self._now() + self._ttl, payload)

    def clear(self) -> None:
        if self._redis is not None:
            try:
                self._redis.flushdb()
                return
            except Exception:
                logger.debug("Failed to flush Redis cache", exc_info=True)
        with self._lock:
            self._store.clear()


class IndicatorService:
    """Facade that applies caching on top of the indicator repository."""

    def __init__(self, repository: IndicatorRepository, cache: IndicatorCache) -> None:
        self._repository = repository
        self._cache = cache

    @staticmethod
    def _cache_key(prefix: str, symbol: str, timeframe: str, session: Optional[str]) -> str:
        parts = [prefix, symbol.upper(), timeframe.lower(), (session or "*").lower()]
        return ":".join(parts)

    def cvd_curve(
        self, *, symbol: str, timeframe: str, session: Optional[str] = None
    ) -> CvdCurveResponse:
        cache_key = self._cache_key("cvd", symbol, timeframe, session)
        cached = self._cache.get(cache_key, CvdCurveResponse)
        if isinstance(cached, CvdCurveResponse):
            return cached

        series = self._repository.cvd_curve(symbol=symbol, timeframe=timeframe, session=session)
        self._cache.set(cache_key, series)
        return series

    def delta_oi_percent(
        self, *, symbol: str, timeframe: str, session: Optional[str] = None
    ) -> DeltaOiCurveResponse:
        cache_key = self._cache_key("delta", symbol, timeframe, session)
        cached = self._cache.get(cache_key, DeltaOiCurveResponse)
        if isinstance(cached, DeltaOiCurveResponse):
            return cached

        series = self._repository.delta_oi_percent(symbol=symbol, timeframe=timeframe, session=session)
        self._cache.set(cache_key, series)
        return series

    def volume_profile(
        self, *, symbol: str, timeframe: str, session: Optional[str] = None
    ) -> VolumeProfileStatsResponse:
        cache_key = self._cache_key("profile", symbol, timeframe, session)
        cached = self._cache.get(cache_key, VolumeProfileStatsResponse)
        if isinstance(cached, VolumeProfileStatsResponse):
            return cached

        profile = self._repository.volume_profile(symbol=symbol, timeframe=timeframe, session=session)
        self._cache.set(cache_key, profile)
        return profile
