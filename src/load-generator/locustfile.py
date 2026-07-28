#!/usr/bin/python

# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

import json
import os
import random
import uuid
import logging
import time

from locust import HttpUser, task, between
from locust_plugins.users.playwright import PlaywrightUser, pw, PageWithRetry, event

from opentelemetry import context, baggage, trace
from opentelemetry.context import Context
from opentelemetry.metrics import set_meter_provider, get_meter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.jinja2 import Jinja2Instrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.system_metrics import SystemMetricsInstrumentor
from opentelemetry.instrumentation.urllib3 import URLLib3Instrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource

try:
    import pg8000
    HAS_PG8000 = True
except ImportError:
    pg8000 = None
    HAS_PG8000 = False

from openfeature import api
from openfeature.contrib.provider.ofrep import OFREPProvider
from openfeature.contrib.hook.opentelemetry import TracingHook

from playwright.async_api import Route, Request

# Configure tracer provider first (needed for trace context in logs)
tracer_provider = TracerProvider()
trace.set_tracer_provider(tracer_provider)
tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(insecure=True)))

# Configure logger provider with the same resource
logger_provider = LoggerProvider()
set_logger_provider(logger_provider)

# Set up log exporter and processor
log_exporter = OTLPLogExporter(insecure=True)
logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))

# Create logging handler that will include trace context
handler = LoggingHandler(level=logging.INFO, logger_provider=logger_provider)

# Configure root logger
root_logger = logging.getLogger()
root_logger.addHandler(handler)
root_logger.setLevel(logging.INFO)

# Configure metrics
metric_exporter = OTLPMetricExporter(insecure=True)
set_meter_provider(MeterProvider([PeriodicExportingMetricReader(metric_exporter)]))
meter = get_meter(__name__)
migration_checkout_counter = meter.create_counter(
    "migration.checkout.attempts",
    unit="1",
    description="Checkout attempts during migration, tagged by status and order_id",
)

# Instrument logging to automatically inject trace context
LoggingInstrumentor().instrument(set_logging_format=True)

# Instrumenting manually to avoid error with locust gevent monkey
Jinja2Instrumentor().instrument()
RequestsInstrumentor().instrument()
SystemMetricsInstrumentor().instrument()
URLLib3Instrumentor().instrument()

logging.info("Instrumentation complete - logs will now include trace context")

# Initialize Flagd provider
base_url = f"http://{os.environ.get('FLAGD_HOST', 'localhost')}:{os.environ.get('FLAGD_OFREP_PORT', 8016)}"
api.set_provider(OFREPProvider(base_url=base_url))
api.add_hooks([TracingHook()])

def get_flagd_value(FlagName):
    # Initialize OpenFeature
    client = api.get_client()
    return client.get_integer_value(FlagName, 0)

categories = [
    "binoculars",
    "telescopes",
    "accessories",
    "assembly",
    "travel",
    "books",
    None,
]

products = [
    "0PUK6V6EV0",
    "1YMWWN1N4O",
    "2ZYFJ3GM2N",
    "66VCHSJNUP",
    "6E92ZMYYFZ",
    "9SIQT8TOJO",
    "L9ECAV7KIM",
    "LS4PSXUNUM",
    "OLJCESPC7Z",
    "HQTGWGPNH4",
]

people_file = open('people.json')
people = json.load(people_file)

class WebsiteUser(HttpUser):
    wait_time = between(1, 10)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tracer = trace.get_tracer(__name__)
        self.order_ids = {}
        self.last_verify = 0.0

    def _post_with_retry(self, path, body, attempts=3):
        for attempt in range(attempts):
            with self.client.post(path, json=body, catch_response=True) as resp:
                transient = resp.status_code in (408, 429) or resp.status_code >= 500
                if 200 <= resp.status_code < 300:
                    return resp
                if transient and attempt + 1 < attempts:
                    resp.success()
                    time.sleep(0.1 * (2 ** attempt))
                    continue
                resp.failure(f"POST {path} failed: HTTP {resp.status_code}")
                return None
        return None

    @task(1)
    def index(self):
        with self.tracer.start_as_current_span("user_index", context=context.get_current()):
            logging.info("User accessing index page")
            self.client.get("/")

    @task(10)
    def browse_product(self):
        product = random.choice(products)
        with self.tracer.start_as_current_span("user_browse_product", context=context.get_current(), attributes={"product.id": product}):
            logging.info(f"User browsing product: {product}")
            self.client.get("/api/products/" + product)

    @task(3)
    def get_recommendations(self):
        product = random.choice(products)
        with self.tracer.start_as_current_span("user_get_recommendations", context=context.get_current(), attributes={"product.id": product}):
            logging.info(f"User getting recommendations for product: {product}")
            params = {
                "productIds": [product],
            }
            self.client.get("/api/recommendations", params=params)

    @task(3)
    def get_ads(self):
        category = random.choice(categories)
        with self.tracer.start_as_current_span("user_get_ads", context=context.get_current(), attributes={"category": str(category)}):
            logging.info(f"User getting ads for category: {category}")
            params = {
                "contextKeys": [category],
            }
            self.client.get("/api/data/", params=params)

    @task(3)
    def view_cart(self):
        with self.tracer.start_as_current_span("user_view_cart", context=context.get_current()):
            logging.info("User viewing cart")
            self.client.get("/api/cart")

    @task(2)
    def add_to_cart(self, user=""):
        if user == "":
            user = str(uuid.uuid1())
        product = random.choice(products)
        quantity = random.choice([1, 2, 3, 4, 5, 10])
        with self.tracer.start_as_current_span("user_add_to_cart", context=context.get_current(), attributes={"user.id": user, "product.id": product, "quantity": quantity}):
            logging.info(f"User {user} adding {quantity} of product {product} to cart")
            self.client.get("/api/products/" + product)
            cart_item = {
                "item": {
                    "productId": product,
                    "quantity": quantity,
                },
                "userId": user,
                "operationId": str(uuid.uuid4()),
            }
            return self._post_with_retry("/api/cart", cart_item) is not None

    def _process_checkout(self, user, checkout_person, multi_count=1):
        with self.tracer.start_as_current_span("user_checkout", context=context.get_current(), attributes={"user.id": user, "item.count": multi_count}):
            for i in range(multi_count):
                if not self.add_to_cart(user=user):
                    migration_checkout_counter.add(1, {"status": "failure", "error": "cart_add_failed"})
                    return
            checkout_person["operationId"] = str(uuid.uuid4())
            with self.client.post("/api/checkout", json=checkout_person, catch_response=True) as resp:
                if resp.status_code != 200:
                    resp.failure(f"Checkout failed: HTTP {resp.status_code}")
                    migration_checkout_counter.add(1, {"status": "failure", "error": str(resp.status_code)})
                    return
                body = resp.json()
                order_id = body.get("orderId")
                if not order_id:
                    resp.failure("Checkout response missing order_id")
                    migration_checkout_counter.add(1, {"status": "failure", "error": "missing_order_id"})
                else:
                    self.order_ids[order_id] = time.time()
                    migration_checkout_counter.add(1, {"status": "success"})
                    logging.info(f"Order placed: {order_id}")

    @task(1)
    def checkout(self):
        user = str(uuid.uuid1())
        checkout_person = random.choice(people).copy()
        checkout_person["userId"] = user
        self._process_checkout(user, checkout_person, multi_count=1)

    @task(1)
    def checkout_multi(self):
        user = str(uuid.uuid1())
        item_count = random.choice([2, 3, 4])
        checkout_person = random.choice(people).copy()
        checkout_person["userId"] = user
        self._process_checkout(user, checkout_person, multi_count=item_count)

    @task(5)
    def flood_home(self):
        flood_count = get_flagd_value("loadGeneratorFloodHomepage")
        if flood_count > 0:
            with self.tracer.start_as_current_span("user_flood_home",  context=context.get_current(), attributes={"flood.count": flood_count}):
                logging.info(f"User flooding homepage {flood_count} times")
                for _ in range(0, flood_count):
                    self.client.get("/")

    @task(1)
    def verify_orders(self):
        if not self.order_ids:
            return
        if not HAS_PG8000:
            return
        now = time.time()
        if now - self.last_verify < 30:
            return
        self.last_verify = now
        db_host = os.environ.get("POSTGRES_HOST", "db-router")
        db_port = int(os.environ.get("POSTGRES_PORT", 5432))
        db_password = os.environ.get("POSTGRES_ASTRONOMY_PASSWORD", "astronomy_password")
        try:
            conn = pg8000.connect(
                host=db_host,
                port=db_port,
                database="astronomy_db",
                user="astronomy_user",
                password=db_password,
                timeout=3,
            )
            cur = conn.cursor()
            grace_period = float(os.environ.get("ORDER_VERIFICATION_GRACE_SECONDS", 300))
            for oid, placed_at in list(self.order_ids.items()):
                if now - placed_at < grace_period:
                    continue
                cur.execute('SELECT 1 FROM accounting."order" WHERE order_id = %s', (oid,))
                if cur.fetchone() is None:
                    self.environment.runner.stats.log_error("VERIFY", "/api/checkout", f"Order {oid} not found in DB")
                    logging.error(f"Order {oid} missing from database")
                else:
                    del self.order_ids[oid]
            cur.close()
            conn.close()
        except Exception as ex:
            logging.warning(f"Verifier could not connect to PostgreSQL at {db_host}:{db_port}: {ex}")

    def on_start(self):
        session_id = str(uuid.uuid4())
        logging.info(f"Starting user session: {session_id}")
        # Attach the baggage-bearing context OUTSIDE of any span's `with` block.
        # If this were attached *inside* start_as_current_span(...)'s `with` block,
        # that block's own exit would detach past this manual attach and silently
        # discard the baggage for the rest of the user's session.
        ctx = baggage.set_baggage("session.id", session_id)
        ctx = baggage.set_baggage("synthetic_request", "true", context=ctx)
        context.attach(ctx)
        with self.tracer.start_as_current_span("user_session_start", context=context.get_current()):
            self.index()


browser_traffic_enabled = os.environ.get("LOCUST_BROWSER_TRAFFIC_ENABLED", "").lower() in ("true", "yes", "on")

if browser_traffic_enabled:
    class WebsiteBrowserUser(PlaywrightUser):
        headless = True  # to use a headless browser, without a GUI

        @task
        @pw
        async def open_cart_page_and_change_currency(self, page: PageWithRetry):
            tracer = trace.get_tracer(__name__)
            with tracer.start_as_current_span("browser_change_currency", context=Context()):
                try:
                    page.on("console", lambda msg: print(msg.text))
                    await page.route('**/*', add_baggage_header)
                    await page.goto("/cart", wait_until="domcontentloaded")
                    await page.select_option('[name="currency_code"]', 'CHF')
                    await page.wait_for_timeout(2000)  # giving the browser time to export the traces
                    logging.info("Currency changed to CHF")
                except Exception as e:
                    logging.error(f"Error in change currency task: {str(e)}")

        @task
        @pw
        async def add_product_to_cart(self, page: PageWithRetry):
            tracer = trace.get_tracer(__name__)
            with tracer.start_as_current_span("browser_add_to_cart", context=Context()):
                try:
                    page.on("console", lambda msg: print(msg.text))
                    await page.route('**/*', add_baggage_header)
                    await page.goto("/", wait_until="domcontentloaded")
                    # Wait for Roof Binoculars image to load (awaiting successful XHR response in less than 15 seconds)
                    await page.wait_for_event(
                        "response",
                        predicate=lambda r: '/images/products/RoofBinoculars.jpg' in r.url and r.status == 200,
                        timeout=15000
                    )
                    await page.click('p:has-text("Roof Binoculars")')
                    await page.wait_for_load_state("domcontentloaded")
                    await page.click('button:has-text("Add To Cart")')
                    await page.wait_for_load_state("domcontentloaded")
                    await page.wait_for_timeout(2000)  # giving the browser time to export the traces
                    logging.info("Product added to cart successfully")
                except Exception as e:
                    logging.error(f"Error in add to cart task: {str(e)}")

async def add_baggage_header(route: Route, request: Request):
    existing_baggage = request.headers.get('baggage', '')
    headers = {
        **request.headers,
        'baggage': ', '.join(filter(None, (existing_baggage, 'synthetic_request=true')))
    }
    await route.continue_(headers=headers)
