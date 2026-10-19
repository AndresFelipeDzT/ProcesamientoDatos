"""
Módulo del Servidor Web Interactivo del Simulador de Apache Spark.
Implementado con la librería estándar de Python (ThreadingHTTPServer)
para máxima portabilidad, sin requerir librerías externas adicionales.
Provee API REST JSON para interactuar con el simulador y sirve la interfaz web.
"""

import json
import os
import sys
import threading
import time
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

# Asegurar importación de los módulos del simulador
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(CURRENT_DIR)
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from config import ClusterConfig
from engine.simulator import SparkSimulator
from workload.workload_generator import WorkloadGenerator
from scenarios.test_scenarios import ScenarioRunner

class SimulatorWebState:
    """Mantiene la instancia viva del simulador y el hilo de auto-ejecución."""
    def __init__(self):
        self.lock = threading.Lock()
        self.config = ClusterConfig()
        self.simulator = SparkSimulator(self.config)
        self.generator = WorkloadGenerator(self.simulator)
        self.is_auto_playing = False
        self.play_speed_sec = 0.4  # Intervalo de ticks en modo reproducción
        self._play_thread = None

    def reset(self):
        with self.lock:
            self.is_auto_playing = False
            self.simulator = SparkSimulator(self.config)
            self.generator = WorkloadGenerator(self.simulator)

    def step(self):
        with self.lock:
            self.simulator.step()

    def toggle_play(self, play: bool):
        self.is_auto_playing = play
        if play and (self._play_thread is None or not self._play_thread.is_alive()):
            self._play_thread = threading.Thread(target=self._play_loop, daemon=True)
            self._play_thread.start()

    def _play_loop(self):
        while self.is_auto_playing:
            with self.lock:
                self.simulator.step()
            time.sleep(self.play_speed_sec)

# Instancia global del estado de simulación
GLOBAL_STATE = SimulatorWebState()

class SparkSimulatorAPIHandler(SimpleHTTPRequestHandler):
    """Manejador HTTP para la API REST y los archivos estáticos de la interfaz."""

    def __init__(self, *args, **kwargs):
        # Servir archivos estáticos desde ui/static
        static_dir = os.path.join(CURRENT_DIR, "static")
        super().__init__(*args, directory=static_dir, **kwargs)

    def log_message(self, format, *args):
        # Evitar inundar la terminal con peticiones de polling continuo cada 300ms
        if len(args) > 0 and "/api/state" in str(args[0]):
            return
        # Para otras peticiones (ej. submit-job, kill, revive) sí loguear limpiamente
        sys.stdout.write(f"[Spark Server] {args[0]} -> {args[1]}\n")

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/state":
            self._handle_get_state()
        else:
            # Servir frontend estático (HTML, CSS, JS)
            super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8") if length > 0 else "{}"
        try:
            payload = json.loads(body) if body else {}
        except Exception:
            payload = {}

        if parsed.path == "/api/step":
            GLOBAL_STATE.step()
            self._send_json({"success": True, "state": GLOBAL_STATE.simulator.get_cluster_state()})

        elif parsed.path == "/api/play":
            play = payload.get("play", True)
            GLOBAL_STATE.toggle_play(play)
            self._send_json({"success": True, "is_playing": GLOBAL_STATE.is_auto_playing})

        elif parsed.path == "/api/speed":
            speed = float(payload.get("speed_sec", 0.4))
            GLOBAL_STATE.play_speed_sec = max(0.05, min(2.0, speed))
            self._send_json({"success": True, "speed_sec": GLOBAL_STATE.play_speed_sec})

        elif parsed.path == "/api/reset":
            GLOBAL_STATE.reset()
            self._send_json({"success": True, "state": GLOBAL_STATE.simulator.get_cluster_state()})

        elif parsed.path == "/api/worker/kill":
            worker_id = payload.get("worker_id", "")
            with GLOBAL_STATE.lock:
                success = GLOBAL_STATE.simulator.kill_worker(worker_id)
            self._send_json({"success": success, "worker_id": worker_id})

        elif parsed.path == "/api/worker/revive":
            worker_id = payload.get("worker_id", "")
            with GLOBAL_STATE.lock:
                success = GLOBAL_STATE.simulator.revive_worker(worker_id)
            self._send_json({"success": success, "worker_id": worker_id})

        elif parsed.path == "/api/submit-job":
            job_type = payload.get("type", "analyst")
            user_id = payload.get("user_id", "user_demo")
            data_size = float(payload.get("data_size_mb", 100.0))
            partitions = int(payload.get("partitions", 4))
            skew = float(payload.get("skew_factor", 1.0))

            with GLOBAL_STATE.lock:
                if job_type == "analyst":
                    job = GLOBAL_STATE.generator.submit_analyst_query(user_id=user_id, data_size_mb=data_size)
                elif job_type == "etl":
                    job = GLOBAL_STATE.generator.submit_etl_pipeline(
                        user_id=user_id, data_size_mb=data_size, num_partitions=partitions, skew_factor=skew
                    )
                elif job_type == "ml":
                    job = GLOBAL_STATE.generator.submit_ml_pipeline(user_id=user_id)
                elif job_type == "burst":
                    jobs = GLOBAL_STATE.generator.submit_burst_workload()
                    job = jobs[0] if jobs else None
                else:
                    job = None

            self._send_json({"success": job is not None, "job_id": job.job_id if job else None})

        elif parsed.path == "/api/scenario/load":
            scenario_id = int(payload.get("scenario_id", 1))
            GLOBAL_STATE.reset()
            with GLOBAL_STATE.lock:
                if scenario_id == 1:
                    GLOBAL_STATE.generator.submit_analyst_query("analyst_lucia", data_size_mb=60.0)
                    GLOBAL_STATE.generator.submit_etl_pipeline("engineer_marco", data_size_mb=200.0, num_partitions=8)
                elif scenario_id == 2:
                    GLOBAL_STATE.generator.submit_etl_pipeline("engineer_diego", data_size_mb=300.0, num_partitions=12)
                elif scenario_id == 3:
                    GLOBAL_STATE.generator.submit_etl_pipeline("engineer_skewed", data_size_mb=300.0, num_partitions=8, skew_factor=3.5)
                elif scenario_id == 4:
                    GLOBAL_STATE.generator.submit_burst_workload()

            self._send_json({"success": True, "scenario_id": scenario_id, "state": GLOBAL_STATE.simulator.get_cluster_state()})

        else:
            self.send_response(404)
            self.end_headers()

    def _handle_get_state(self):
        with GLOBAL_STATE.lock:
            state = GLOBAL_STATE.simulator.get_cluster_state()
            state["is_playing"] = GLOBAL_STATE.is_auto_playing
            state["speed_sec"] = GLOBAL_STATE.play_speed_sec
        self._send_json(state)

    def _send_json(self, data: dict):
        response_bytes = json.dumps(data, default=str).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(response_bytes)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(response_bytes)

def run_server(port: int = 8080):
    server_address = ("127.0.0.1", port)
    httpd = ThreadingHTTPServer(server_address, SparkSimulatorAPIHandler)
    print(f"===========================================================")
    print(f"  Simulador Apache Spark - Servidor Interactivo Activo")
    print(f"  Abre en tu navegador: http://127.0.0.1:{port}")
    print(f"===========================================================")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nServidor detenido por el usuario.")
        httpd.server_close()

if __name__ == "__main__":
    run_server(8080)
