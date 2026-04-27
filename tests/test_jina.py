"""
Tests for Jina AI integration.

Covers:
- JINA_API_KEY loaded from environment
- fetch_via_jina() skips when no key is set
- fetch_via_jina() sends correct Authorization header
- fetch_via_jina() returns None on 401 / 429 / network error
- force_disabled=True skips the request
- env_loader.validate_env() shows key status
"""
from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from utils.env_loader import validate_env


# ---------------------------------------------------------------------------
# env_loader
# ---------------------------------------------------------------------------

def test_validate_env_returns_dict():
    result = validate_env()
    assert isinstance(result, dict)
    assert "JINA_API_KEY" in result
    assert result["JINA_API_KEY"] in ("set", "missing")


def test_jina_key_missing_by_default(monkeypatch):
    monkeypatch.delenv("JINA_API_KEY", raising=False)
    from utils import env_loader
    # Clear lru_cache
    env_loader.get_jina_api_key.cache_clear()
    env_loader.is_jina_enabled.cache_clear()
    key = env_loader.get_jina_api_key()
    assert key == ""


def test_jina_key_set_from_env(monkeypatch):
    monkeypatch.setenv("JINA_API_KEY", "test-key-abc")
    from utils import env_loader
    env_loader.get_jina_api_key.cache_clear()
    env_loader.is_jina_enabled.cache_clear()
    key = env_loader.get_jina_api_key()
    assert key == "test-key-abc"


# ---------------------------------------------------------------------------
# fetch_via_jina()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_via_jina_skips_when_no_key(monkeypatch):
    monkeypatch.delenv("JINA_API_KEY", raising=False)
    from utils import env_loader
    env_loader.get_jina_api_key.cache_clear()
    env_loader.is_jina_enabled.cache_clear()

    from utils.jina import fetch_via_jina
    result = await fetch_via_jina("https://example.com")
    assert result is None


@pytest.mark.asyncio
async def test_fetch_via_jina_force_disabled():
    from utils.jina import fetch_via_jina
    result = await fetch_via_jina("https://example.com", api_key="key", force_disabled=True)
    assert result is None


@pytest.mark.asyncio
async def test_fetch_via_jina_success():
    """Mock a successful Jina API response."""
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value={"data": {"content": "Extracted content"}})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.post = MagicMock(return_value=mock_resp)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with patch("aiohttp.ClientSession", return_value=mock_session):
        from utils.jina import fetch_via_jina
        result = await fetch_via_jina("https://example.com", api_key="test-key")

    assert result == "Extracted content"


@pytest.mark.asyncio
async def test_fetch_via_jina_returns_none_on_401():
    mock_resp = AsyncMock()
    mock_resp.status = 401
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.post = MagicMock(return_value=mock_resp)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with patch("aiohttp.ClientSession", return_value=mock_session):
        from utils.jina import fetch_via_jina
        result = await fetch_via_jina("https://example.com", api_key="bad-key")

    assert result is None


@pytest.mark.asyncio
async def test_fetch_via_jina_returns_none_on_429():
    mock_resp = AsyncMock()
    mock_resp.status = 429
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.post = MagicMock(return_value=mock_resp)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with patch("aiohttp.ClientSession", return_value=mock_session):
        from utils.jina import fetch_via_jina
        result = await fetch_via_jina("https://example.com", api_key="key")

    assert result is None


@pytest.mark.asyncio
async def test_fetch_via_jina_returns_none_on_network_error():
    import aiohttp
    with patch("aiohttp.ClientSession") as MockSession:
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_session.post.side_effect = aiohttp.ClientConnectorError(
            MagicMock(), OSError("network unreachable")
        )
        MockSession.return_value = mock_session

        from utils.jina import fetch_via_jina
        result = await fetch_via_jina("https://example.com", api_key="key")

    assert result is None


@pytest.mark.asyncio
async def test_fetch_via_jina_uses_authorization_header():
    """Verify the Authorization: Bearer header is sent correctly."""
    captured_kwargs = {}

    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value={"data": {"content": "ok"}})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)

    def _capture_post(*args, **kwargs):
        captured_kwargs.update(kwargs)
        return mock_resp

    mock_session = MagicMock()
    mock_session.post = _capture_post
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with patch("aiohttp.ClientSession", return_value=mock_session):
        from utils.jina import fetch_via_jina
        await fetch_via_jina("https://example.com", api_key="my-secret-key")

    headers = captured_kwargs.get("headers", {})
    assert headers.get("Authorization") == "Bearer my-secret-key"
    assert headers.get("Accept") == "application/json"
