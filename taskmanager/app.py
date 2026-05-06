from __future__ import annotations

from fastapi import FastAPI
import uvicorn


app = FastAPI(title="Taskmanager Placeholder")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "flink-taskmanager"}


@app.get("/")
def root() -> dict[str, str]:
    return health()


def main() -> None:
    uvicorn.run(app, host="0.0.0.0", port=8082, log_level="info")


if __name__ == "__main__":
    main()
