from __future__ import annotations

from datetime import datetime
import inspect
from pathlib import Path

from sqlalchemy.orm import Session, sessionmaker

from db.models import MetricDefinition as MetricDefinitionModel, engine
from metrics.framework import registry
from metrics.framework.family import (
    FAMILY_VARIANT_CAREER,
    FAMILY_VARIANT_SEASON,
    build_career_code_variant,
    derive_career_description,
    derive_career_min_sample,
    derive_career_name,
    family_career_key,
)
from metrics.framework.runtime import load_code_metric

SessionLocal = sessionmaker(bind=engine)


def _source_owner(metric_def):
    metric_cls = type(metric_def)
    module = inspect.getmodule(metric_cls)
    if module is not None and module.__name__ == "metrics.framework.registry" and len(metric_cls.__mro__) > 1:
        return metric_cls.__mro__[1]
    return metric_cls


def _clean_module_source(metric_def) -> tuple[str, str]:
    metric_cls = _source_owner(metric_def)
    module = inspect.getmodule(metric_cls)
    if module is None or not getattr(module, "__file__", None):
        raise ValueError(f"Cannot resolve source file for builtin metric {metric_def.key!r}")

    path = Path(module.__file__).resolve()
    code = path.read_text()
    cleaned_lines = [line for line in code.rstrip().splitlines() if not line.strip().startswith("register(")]
    cleaned_code = "\n".join(cleaned_lines).rstrip() + "\n"
    return cleaned_code, path.as_posix()


def sync_builtin_metrics_to_db(session: Session, *, overwrite: bool = False) -> dict[str, int]:
    inserted = 0
    updated = 0
    skipped = 0
    now = datetime.utcnow()

    builtin_metrics = sorted(
        [metric for metric in registry.get_all() if not getattr(metric, "career", False)],
        key=lambda metric: metric.key,
    )

    for metric in builtin_metrics:
        code_python, source_path = _clean_module_source(metric)
        loaded = load_code_metric(code_python)
        supports_career = bool(getattr(loaded, "supports_career", False)) and loaded.scope != "game"

        def _upsert(
            key: str,
            *,
            name: str,
            description: str,
            min_sample: int,
            code: str,
            variant: str,
            family_key: str,
            base_metric_key: str | None,
            managed_family: bool,
        ) -> None:
            nonlocal inserted, updated, skipped
            row = (
                session.query(MetricDefinitionModel)
                .filter(MetricDefinitionModel.key == key)
                .first()
            )

            if row is not None and not overwrite:
                skipped += 1
                return

            if row is None:
                row = MetricDefinitionModel(
                    key=key,
                    created_at=now,
                )
                session.add(row)
                inserted += 1
            else:
                updated += 1

            row.family_key = family_key
            row.variant = variant
            row.base_metric_key = base_metric_key
            row.managed_family = managed_family
            row.name = name
            row.description = description
            row.scope = loaded.scope
            row.category = getattr(loaded, "category", "") or ""
            row.group_key = getattr(metric, "group_key", None)
            row.source_type = "code"
            row.status = "published"
            row.definition_json = None
            row.code_python = code
            row.expression = f"[seed_builtin] {source_path}"
            row.min_sample = int(min_sample or 1)
            row.updated_at = now

        _upsert(
            loaded.key,
            name=loaded.name,
            description=loaded.description,
            min_sample=int(getattr(loaded, "min_sample", 1) or 1),
            code=code_python,
            variant=FAMILY_VARIANT_SEASON,
            family_key=loaded.key,
            base_metric_key=None,
            managed_family=supports_career,
        )

        if supports_career:
            career_name = derive_career_name(loaded.name, getattr(loaded, "career_name_suffix", " (Career)"))
            career_description = derive_career_description(loaded.description)
            career_min_sample = derive_career_min_sample(
                int(getattr(loaded, "min_sample", 1) or 1),
                getattr(loaded, "career_min_sample", None),
            )
            _upsert(
                family_career_key(loaded.key),
                name=career_name,
                description=career_description,
                min_sample=career_min_sample,
                code=build_career_code_variant(
                    code_python,
                    base_key=loaded.key,
                    name=career_name,
                    description=career_description,
                    min_sample=career_min_sample,
                ),
                variant=FAMILY_VARIANT_CAREER,
                family_key=loaded.key,
                base_metric_key=loaded.key,
                managed_family=True,
            )

    session.commit()
    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def main() -> None:
    with SessionLocal() as session:
        result = sync_builtin_metrics_to_db(session, overwrite=False)
    print(result)


if __name__ == "__main__":
    main()
