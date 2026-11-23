"""Per-page plugin modules for the Next UI (see web/next_ui.py).

A module named after a template stem (``player.py`` for ``player.html``) may
define ``enrich(ctx, api) -> dict`` (merged into the template's ``nx``) and/or
``register(app, api)`` to add routes or JSON APIs at startup.
"""
