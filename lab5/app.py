from pathlib import Path
from datetime import datetime, timezone
import json

from flask import Flask, request, Response

app = Flask(__name__)

STORAGE_DIR = (Path(__file__).parent / "storage").resolve()
STORAGE_DIR.mkdir(parents=True, exist_ok=True)


def get_full_path(req_path: str) -> Path:
    full_path = (STORAGE_DIR / req_path).resolve()

    if not full_path.is_relative_to(STORAGE_DIR):
        raise ValueError("Invalid path")

    return full_path


def is_directory_url() -> bool:
    return request.path.endswith("/") and request.path != "/"


def get_http_date(timestamp: float) -> str:
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")


def get_directory_list(directory: Path) -> list[dict]:
    result = []

    for item in directory.iterdir():
        result.append({
            "name": item.name,
            "type": "directory" if item.is_dir() else "file"
        })

    return result


def make_json_response(data, status: int = 200) -> Response:
    json_text = json.dumps(data, ensure_ascii=False, indent=2)

    response = Response(json_text, status=status)
    response.headers["Content-Type"] = "application/json; charset=utf-8"
    response.headers["Content-Length"] = str(len(json_text.encode("utf-8")))

    return response


def make_file_response(file_path: Path) -> Response:
    with open(file_path, "rb") as f:
        content = f.read()

    response = Response(content, status=200)
    response.headers["Content-Length"] = str(len(content))
    response.headers["Last-Modified"] = get_http_date(file_path.stat().st_mtime)

    return response


def make_head_response(path: Path) -> Response:
    stat_info = path.stat()

    response = Response(status=200)
    response.automatically_set_content_length = False
    response.headers["Last-Modified"] = get_http_date(stat_info.st_mtime)

    if path.is_file():
        response.headers["Content-Length"] = str(stat_info.st_size)
        return response

    if path.is_dir():
        directory_json = json.dumps(get_directory_list(path), ensure_ascii=False, indent=2)
        response.headers["Content-Length"] = str(len(directory_json.encode("utf-8")))
        return response

    return Response(status=400)

def delete_directory(directory: Path):
    for item in directory.iterdir():
        if item.is_file():
            item.unlink()
        elif item.is_dir():
            delete_directory(item)

    directory.rmdir()

@app.route("/", defaults={"req_path": ""}, methods=["GET", "PUT", "DELETE", "HEAD"])
@app.route("/<path:req_path>", methods=["GET", "PUT", "DELETE", "HEAD"])
def handle_request(req_path: str):
    try:
        full_path = get_full_path(req_path)
    except ValueError:
        return "Invalid path", 400

    if request.method == "GET":
        if not full_path.exists():
            return "Path not found", 404

        if full_path.is_file():
            return make_file_response(full_path)

        if full_path.is_dir():
            return make_json_response(get_directory_list(full_path))

        return "Unsupported path type", 400

    if request.method == "PUT":
        if full_path == STORAGE_DIR:
            return "Cannot write to root storage", 400

        existed_before = full_path.exists()
        copy_from = request.headers.get("X-Copy-From")

        if copy_from:
            try:
                source_path = get_full_path(copy_from.lstrip("/"))
            except ValueError:
                return "Invalid source path", 400

            if not source_path.exists():
                return "Source file not found", 404

            if not source_path.is_file():
                return "Source path is not a file", 400

            if full_path.exists() and full_path.is_dir():
                return "Cannot overwrite directory with file", 409

            full_path.parent.mkdir(parents=True, exist_ok=True)
            with open(source_path, "rb") as src:
                with open(full_path, "wb") as dst:
                    dst.write(src.read())

            if existed_before:
                return "File copied with overwrite", 200
            return "File copied", 201

        if is_directory_url():
            if full_path.exists() and full_path.is_file():
                return "Cannot create directory: file already exists", 409

            full_path.mkdir(parents=True, exist_ok=True)

            if existed_before:
                return "Directory already exists", 200
            return "Directory created", 201

        if full_path.exists() and full_path.is_dir():
            return "Cannot overwrite directory with file", 409

        full_path.parent.mkdir(parents=True, exist_ok=True)

        with open(full_path, "wb") as f:
            f.write(request.get_data())

        if existed_before:
            return "File overwritten", 200
        return "File created", 201

    if request.method == "HEAD":
        if not full_path.exists():
            return "", 404

        return make_head_response(full_path)

    if request.method == "DELETE":
        if not full_path.exists():
            return "Path not found", 404

        if full_path == STORAGE_DIR:
            return "Cannot delete root storage", 400

        if full_path.is_file():
            full_path.unlink()
            return "", 204

        if full_path.is_dir():
            delete_directory(full_path)
            return "", 204

        return "Unsupported path type", 400

    return "Method not supported", 405


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)