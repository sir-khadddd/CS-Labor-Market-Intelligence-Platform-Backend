"""Train role trajectory classifier from dev CSVs or Postgres."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ml.constants import DEFAULT_ARTIFACTS_DIR
from ml.data import load_trajectory_dataset
from ml.model import persist_eval_split, save_model, train_trajectory_classifier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        choices=("dev", "postgres"),
        default="dev",
        help="Data source for features and labels",
    )
    parser.add_argument(
        "--entity-type",
        default="role",
        help="Entity type to train on (default: role)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_ARTIFACTS_DIR,
        help="Directory for model artifact and metrics",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger.info(
        "Loading trajectory dataset source=%s entity_type=%s",
        args.source,
        args.entity_type,
    )
    dataset = load_trajectory_dataset(
        source=args.source,
        entity_type=args.entity_type,
    )
    logger.info("Loaded %s rows for training", len(dataset))

    pipeline, metrics = train_trajectory_classifier(dataset)
    model_path = save_model(pipeline, metrics, output_dir=args.output_dir)

    logger.info("Saved model to %s", model_path)
    logger.info("Validation metrics:\n%s", json.dumps(metrics, indent=2))

    split_id = persist_eval_split(metrics, entity_type=args.entity_type)
    if split_id:
        logger.info("Recorded eval split split_id=%s", split_id)

    print(f"Model saved: {model_path}")
    print(f"Validation accuracy: {metrics['accuracy']:.3f}")
    print(f"Validation macro F1: {metrics['macro_f1']:.3f}")
    if split_id:
        print(f"Eval split recorded: {split_id}")
    else:
        print("Eval split not recorded (Postgres unavailable)")


if __name__ == "__main__":
    main()
