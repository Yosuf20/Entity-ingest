"""
Loads config.yaml into typed Pydantic models.

API keys are read exclusively from environment variables (never stored in YAML).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from pydantic import BaseModel, Field

_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


# ---------------------------------------------------------------------------
# LLM provider config (Phase III)
# ---------------------------------------------------------------------------

class ProviderConfig(BaseModel):
    name: str
    model: str
    rpm: int
    max_input_tokens: int
    env_key: str  # name of the env-var that holds the API key

    @property
    def api_key(self) -> str | None:
        """Return the API key from the environment (never from config.yaml)."""
        return os.environ.get(self.env_key)


class RetryConfig(BaseModel):
    base: float = 1.0
    cap: float = 60.0
    max_attempts: int = 5


class BreakerConfig(BaseModel):
    threshold: int = 5
    cooldown: float = 60.0


# ---------------------------------------------------------------------------
# Crawler config (Phase I)
# ---------------------------------------------------------------------------

class DomainConfig(BaseModel):
    """Per-domain rate-limit and concurrency settings."""
    concurrency: int = 5       # max simultaneous requests to this domain
    min_interval: float = 0.0  # minimum seconds between consecutive requests


class CrawlerConfig(BaseModel):
    """Shared async HTTP client settings."""
    global_concurrency: int = 20
    user_agent: str = "AIDataPipeline/1.0"
    retry: RetryConfig = Field(default_factory=RetryConfig)
    domains: Dict[str, DomainConfig] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Papers pipeline config
# ---------------------------------------------------------------------------

class PapersPipelineConfig(BaseModel):
    target: int = 1000
    categories: List[str] = Field(default_factory=lambda: ["cs.AI", "cs.LG", "cs.CL", "cs.CV"])
    page_size: int = 100


# ---------------------------------------------------------------------------
# Top-level config
# ---------------------------------------------------------------------------

class AppConfig(BaseModel):
    providers: List[ProviderConfig]
    retry: RetryConfig
    breaker: BreakerConfig
    crawler: CrawlerConfig = Field(default_factory=CrawlerConfig)
    papers_pipeline: PapersPipelineConfig = Field(default_factory=PapersPipelineConfig)
    ai_keywords: List[str] = Field(default_factory=list)


def load_config(path: Path = _CONFIG_PATH) -> AppConfig:
    """Parse config.yaml and return a validated AppConfig."""
    with open(path) as fh:
        raw = yaml.safe_load(fh)
    # DomainConfig values come from YAML as plain dicts — coerce them
    if "crawler" in raw and "domains" in raw["crawler"]:
        raw["crawler"]["domains"] = {
            k: DomainConfig(**v)
            for k, v in raw["crawler"]["domains"].items()
        }
    return AppConfig(**raw)
