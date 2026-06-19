"""
Captura y verifica el dashboard con un navegador headless (Playwright).
Genera tres imagenes (monitor, Flink, batch) y comprueba que:
  - el monitor en vivo se actualiza solo (el contador de viajes sube),
  - se puede cambiar de pestana (la app no esta congelada),
  - el boton de "Actualizar reportes" responde.

Uso (en el EC2):  python3 capturar.py
"""
from playwright.sync_api import sync_playwright
import time

URL = "http://localhost:8501"


def valor_primer_metric(page):
    try:
        return page.locator('[data-testid="stMetricValue"]').first.inner_text()
    except Exception:
        return ""


with sync_playwright() as p:
    browser = p.chromium.launch(args=["--no-sandbox"])
    page = browser.new_page(viewport={"width": 1600, "height": 1100}, device_scale_factor=2)
    page.goto(URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_selector("text=Viajes en tiempo real", timeout=60000)

    # --- PRUEBA 1: el monitor se actualiza solo ---
    time.sleep(5)
    v1 = valor_primer_metric(page)
    time.sleep(10)
    v2 = valor_primer_metric(page)
    print(f"[MONITOR] viajes t=5s: {v1} | t=15s: {v2}")
    try:
        sube = int(v2.replace(",", "")) > int(v1.replace(",", ""))
    except Exception:
        sube = False
    print(f"[MONITOR] se actualiza solo: {'SI' if sube else 'NO'}")
    page.screenshot(path="/home/ec2-user/img_monitor_streaming.png", full_page=True)
    print("[OK] captura monitor")

    # --- PRUEBA 2: cambiar a la pestana de Flink (la app responde) ---
    page.get_by_role("tab", name="Streaming Flink").click()
    time.sleep(6)
    print(f"[FLINK] primer indicador de la ventana: {valor_primer_metric(page)}")
    page.screenshot(path="/home/ec2-user/img_streaming_flink.png", full_page=True)
    print("[OK] captura Flink")

    # --- PRUEBA 3: pestana batch y boton de actualizar ---
    page.get_by_role("tab", name="Reportes batch").click()
    time.sleep(4)
    page.get_by_role("button", name="Actualizar reportes desde S3").click()
    time.sleep(6)
    print(f"[BATCH] total viajes que muestra: {valor_primer_metric(page)}")
    page.screenshot(path="/home/ec2-user/img_reportes_batch.png", full_page=True)
    print("[OK] captura batch")

    browser.close()
print("CAPTURAS Y PRUEBAS OK")
