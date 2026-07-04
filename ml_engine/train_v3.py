"""
Sentinel v3 training script.

Only real change vs. the original notebook: stratify=y on the train/test split.
Same 4 features as v2 (price, volume, action_SELL, order_type_MARKET) -- a
status_FILLED/CANCELLED feature was tested and measured NO improvement over
this split fix alone, so it was deliberately left out to avoid adding a live
schema change (ingestion API + Kafka payload + Spark consumer schema) that
doesn't earn its keep.

Run from the project root:
    python ml_engine/train_v3.py
"""
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
import joblib
import os

LABEL_MAP = {0: "Normal", 1: "Wash Trade", 2: "Volume Spike",
             3: "Spoofing", 4: "Front-Running", 5: "Flash Crash"}

def train():
    df = pd.read_parquet("data/silver_trades/")

    df_encoded = pd.get_dummies(df, columns=["action", "order_type"], drop_first=True)
    feat_cols = ["price", "volume", "action_SELL", "order_type_MARKET"]

    X = df_encoded[feat_cols]
    y = df_encoded["label"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight="balanced")
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    print("--- Sentinel v3 Model Performance (stratified split) ---")
    print(classification_report(
        y_test, y_pred,
        target_names=[LABEL_MAP[c] for c in sorted(y.unique())]
    ))

    for feat, imp in sorted(zip(feat_cols, model.feature_importances_), key=lambda x: -x[1]):
        print(f"{feat}: {imp:.4f}")

    save_dir = "ml_engine/saved_models"
    os.makedirs(save_dir, exist_ok=True)
    model_path = f"{save_dir}/anomaly_detector_v3.pkl"
    joblib.dump(model, model_path)
    print(f"Saved to: {model_path}")

if __name__ == "__main__":
    train()