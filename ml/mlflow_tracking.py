import logging
import os

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_EXPERIMENT = os.getenv("MLFLOW_EXPERIMENT_NAME", "device_readings_experiment")


def train_and_log_model(features_df=None):
    """Train a Linear Regression model and log to MLflow."""
    import mlflow
    import mlflow.sklearn

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    # Use provided features or generate synthetic data for demo
    if features_df is None or features_df.empty:
        logger.info("No features provided. Using synthetic data for demo.")
        np.random.seed(42)
        n_samples = 200
        features_df = pd.DataFrame(
            {
                "avg_reading": np.random.uniform(30, 70, n_samples),
                "max_reading": np.random.uniform(50, 100, n_samples),
            }
        )

    y = 2.5 * features_df["avg_reading"] + 1.8 * features_df["max_reading"] + np.random.randn(len(features_df)) * 10

    X_train, X_test, y_train, y_test = train_test_split(features_df, y, test_size=0.2, random_state=42)

    with mlflow.start_run():
        model = LinearRegression()
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        mlflow.log_param("model_type", "LinearRegression")
        mlflow.log_param("n_features", len(features_df.columns))
        mlflow.log_param("n_samples", len(features_df))
        mlflow.log_metric("mse", mse)
        mlflow.log_metric("r2_score", r2)

        mlflow.sklearn.log_model(model, "linear_regression_model")

        logger.info("Model logged to MLflow | MSE: %.4f | R2: %.4f", mse, r2)

    return {"mse": mse, "r2": r2}


if __name__ == "__main__":
    train_and_log_model()
