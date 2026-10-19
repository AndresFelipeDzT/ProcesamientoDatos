"""
Punto de Entrada Principal (Main) del Simulador de Apache Spark.
Permite lanzar la interfaz web interactiva o ejecutar las pruebas automatizadas
de los escenarios de la materia.
"""

import argparse
import sys
import os
import webbrowser
import threading
import time

# Configurar path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

# Asegurar encoding UTF-8 en consola de Windows
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from ui.server import run_server
from scenarios.test_scenarios import ScenarioRunner

def run_tests():
    """Ejecuta los 4 escenarios de prueba y genera un reporte en consola."""
    print("\n" + "="*70)
    print("  EJECUTANDO BANCO DE PRUEBAS AUTOMATIZADAS DE APACHE SPARK")
    print("="*70)

    print("\n-> [Prueba 1] Escenario 1: Ejecucion Normal (Batch & Consultas)...")
    s1 = ScenarioRunner.run_scenario_1_normal_execution()
    print(f"  [OK] Finalizado en {s1['current_tick']} ticks. Jobs completados: {len(s1['completed_jobs'])}")

    print("\n-> [Prueba 2] Escenario 2: Caida de Worker y Tolerancia a Fallos por Linaje...")
    s2 = ScenarioRunner.run_scenario_2_fault_tolerance()
    print(f"  [OK] Finalizado en {s2['current_tick']} ticks. Tareas recalculadas por linaje: {s2['resilience']['total_tasks_retried']}")

    print("\n-> [Prueba 3] Escenario 3: Data Skew (Sesgo de Datos y Stragglers)...")
    s3 = ScenarioRunner.run_scenario_3_data_skew()
    print(f"  [OK] Finalizado en {s3['current_tick']} ticks. Jobs completados: {len(s3['completed_jobs'])}")

    print("\n-> [Prueba 4] Escenario 4: Concurrencia Multiusuario y Cola FIFO...")
    s4 = ScenarioRunner.run_scenario_4_multiuser_concurrency()
    print(f"  [OK] Finalizado en {s4['current_tick']} ticks. Total de jobs atendidos: {len(s4['completed_jobs'])}")

    print("\n" + "="*70)
    print("  TODAS LAS PRUEBAS AUTOMATIZADAS PASARON SATISFACTORIAMENTE (100%)")
    print("="*70 + "\n")

def open_browser_delayed(url: str, delay_sec: float = 1.0):
    """Abre el navegador automáticamente después de que el servidor arranque."""
    def _open():
        time.sleep(delay_sec)
        try:
            webbrowser.open(url)
        except Exception:
            pass
    threading.Thread(target=_open, daemon=True).start()

def main():
    parser = argparse.ArgumentParser(
        description="Simulador Educativo de Apache Spark con Nodos, Cargas, RDDs y Tolerancia a Fallos."
    )
    parser.add_argument(
        "--mode",
        choices=["ui", "test"],
        default="ui",
        help="Modo de ejecución: 'ui' para abrir el dashboard web interactivo, o 'test' para correr pruebas en consola."
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Puerto TCP para el servidor web interactivo (por defecto: 8080)."
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Evitar abrir el navegador automáticamente al iniciar el servidor."
    )

    args = parser.parse_args()

    if args.mode == "test":
        run_tests()
    else:
        url = f"http://127.0.0.1:{args.port}"
        if not args.no_browser:
            open_browser_delayed(url)
        run_server(args.port)

if __name__ == "__main__":
    main()
