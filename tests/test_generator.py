import csv
import subprocess
import sys


def test_generator_creates_expected_files(tmp_path):
    for batch in (1, 2):
        subprocess.run([sys.executable, "data_generator/generate_data.py", "--out", str(tmp_path),
                        "--batch", str(batch), "--orders", "100"], check=True)
    assert (tmp_path / "products" / "products_batch1.csv").exists()
    assert not (tmp_path / "products" / "products_batch2.csv").exists()
    with open(tmp_path / "orders" / "orders_batch2.csv") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) >= 100 and rows[0]["order_id"].startswith("O2")
