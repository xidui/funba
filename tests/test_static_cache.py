"""HTTP caching should reduce repeat asset requests without hiding updates."""

import pytest

from tests.test_auth import _make_app


@pytest.fixture
def client():
    app, _, _ = _make_app()
    app.config["TESTING"] = True
    return app.test_client()


@pytest.mark.parametrize(
    "path",
    [
        "/static/team_logos/historical/1610612738/current.svg",
        "/static/visual-refresh.css?v=20260906",
    ],
)
def test_static_assets_can_be_reused_without_revalidation(client, path):
    response = client.get(path)

    assert response.status_code == 200
    assert response.cache_control.public
    assert response.cache_control.max_age == 3600
    assert not response.cache_control.no_cache
    assert not response.cache_control.immutable
    assert response.headers["ETag"]
    assert response.headers["Last-Modified"]


def test_expired_static_asset_can_still_revalidate(client):
    path = "/static/favicon.svg"
    first_response = client.get(path)

    response = client.get(path, headers={"If-None-Match": first_response.headers["ETag"]})

    assert response.status_code == 304
    assert response.cache_control.public
    assert response.cache_control.max_age == 3600
    assert response.data == b""


def test_missing_asset_does_not_get_successful_asset_cache_policy(client):
    response = client.get("/static/does-not-exist.svg")

    assert response.status_code == 404
    assert not response.cache_control.public
    assert response.cache_control.max_age is None


def test_public_api_does_not_get_static_asset_cache_policy(client):
    response = client.get("/api/health")

    assert response.status_code == 200
    assert not response.cache_control.public
    assert response.cache_control.max_age is None
