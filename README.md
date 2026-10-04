# python rotating proxy: Working requests, httpx and threaded rotation code, sticky sessions that actually hold, and which plan is worth paying for

Most people typing "python rotating proxy" into a search bar have the same thing going on: a script that worked fine last week is now returning 403s or empty pages, and they need a new IP for every request without the whole thing falling over.

The tricky part is that "rotating proxy" means two different things in Python, and mixing them up is why a lot of scrapers stay broken. One is a list you rotate yourself. The other is a gateway that rotates upstream for you. Same word, completely different code.

Below: working patterns for both, the sticky-session syntax that trips people up, and a realistic look at what the proxy side of a scraping bill actually costs — using 9Proxy's published plans as the concrete example, since its GB product is built specifically for the high-rotation case.

## Two problems hiding behind one keyword

If you have a `proxies.txt` with 400 entries and you're picking one per request, you're solving pool management. Your code owns the failure handling: dead IPs, retries, backoff, thread safety.

If you point your script at one host:port and each request exits through a different residential IP, you're solving provider-side rotation. Your code gets simpler, and the pool management moves to whatever is behind that endpoint.

Neither is better in the abstract. List rotation is what you do when you already have proxies (free lists, a rotating gateway, an in-house pool). Provider rotation is what you do when you'd rather not own the maintenance. Plenty of production setups do both: a rotating gateway as the primary path, a small list as fallback.

## The five-line version, if you're using a rotating gateway

With a provider that rotates per request, the Python side is almost boring — which is the point:

python
import requests

HOST = "your_proxy_host"          # from the provider dashboard
PORT = "your_port"
USER = "subaccount-country-us"    # no session flags = new IP per request
PASS = "your_password"

proxies = {
    "http": f"http://{USER}:{PASS}@{HOST}:{PORT}",
    "https": f"http://{USER}:{PASS}@{HOST}:{PORT}",
}

r = requests.get("https://ipinfo.io/json", proxies=proxies, timeout=30)
print(r.json())


Run that twice and the `ip` field changes, because nothing in the username pins a session. That username-as-configuration pattern is how 9Proxy's bandwidth product works: targeting and session behaviour are encoded in the username string rather than in separate API calls, and the on-site proxy generator emits the ready-made string plus code samples in several languages, so you aren't hand-building it from scratch.

If you want to hit the ground with credentials in hand, 👉 [grab a 9Proxy account and copy the username string from the dashboard](https://bit.ly/9-Proxy) — the generator outputs rotating and sticky variants side by side.

## Rotating a list you already have

### Round-robin with `itertools.cycle`

python
import itertools
import requests

PROXIES = [
    "http://user:pass@1.2.3.4:8080",
    "http://user:pass@5.6.7.8:8080",
    "socks5://user:pass@9.9.9.9:1080",
]

pool = itertools.cycle(PROXIES)

def fetch(url, timeout=20):
    proxy = next(pool)
    return requests.get(url, proxies={"http": proxy, "https": proxy}, timeout=timeout)


`cycle` is fine for a smoke test and wrong for anything else. It has no memory of which proxies are dead, so a broken entry burns a request every lap. It also isn't thread-safe in the sense you'd want — `next()` on a shared iterator under threads behaves, but the moment you add failure tracking you need a lock anyway.

### One session per IP, for when cookies matter

A `requests.Session` carrying cookies across rotating IPs is a good way to look like a bot: the cookie appears from one address, then another, then another. If a target ties sessions to addresses, give each IP its own session and reuse it for a stretch of requests.

python
from itertools import cycle
import requests

class RotatingSessions:
    def __init__(self, proxies, per_ip=25):
        self._cycle = cycle(proxies)
        self._per_ip = per_ip
        self._count = 0
        self._session = None

    def get(self):
        if self._session is None or self._count >= self._per_ip:
            proxy = next(self._cycle)
            self._session = requests.Session()
            self._session.proxies.update({"http": proxy, "https": proxy})
            self._count = 0
        self._count += 1
        return self._session


The `per_ip` value is a judgement call, not a constant. Twenty-five requests through one residential IP looks normal for a browsing pattern. Two thousand does not.

### Retry on block signals, not on everything

python
import random, time
import requests

RETRY_STATUS = {403, 407, 429, 500, 502, 503, 504}

def get_with_rotation(url, pool, attempts=6):
    last_error = None
    for attempt in range(attempts):
        proxy = random.choice(pool)
        try:
            r = requests.get(
                url,
                proxies={"http": proxy, "https": proxy},
                timeout=20,
                headers={"User-Agent": "Mozilla/5.0 ..."},
            )
            if r.status_code not in RETRY_STATUS:
                return r
            last_error = f"HTTP {r.status_code} via {proxy}"
        except requests.RequestException as exc:
            last_error = f"{type(exc).__name__} via {proxy}"

        time.sleep(min(2 ** attempt, 30) + random.random())
    raise RuntimeError(f"all {attempts} attempts failed: {last_error}")


Two details that matter more than the retry count: backoff should have jitter (identical sleeps synchronise your threads into a burst), and a 429 should slow you down rather than just swap IPs. Rotating away from a rate limit you caused keeps you rate-limited on the new address too.

### Threads need a lock, not hope

A shared rotator touched by eight workers will hand the same proxy to several of them unless you protect the counter. `threading.Lock` around the selection is enough.

python
import threading
import itertools

class SafeRotator:
    def __init__(self, proxies):
        self._lock = threading.Lock()
        self._cycle = itertools.cycle(proxies)

    def next_proxy(self):
        with self._lock:
            return next(self._cycle)


For per-proxy health tracking, keep a dict of failure counts and skip anything past a threshold for a cooldown period. That's ten lines and saves you from re-discovering the same dead IPs on every pass.

### Async with httpx

`httpx` mirrors the `requests` API closely enough that the port is mostly mechanical:

python
import asyncio
import httpx

PROXIES = ["http://user:pass@1.2.3.4:8080", "http://user:pass@5.6.7.8:8080"]

async def fetch(client, url, proxy):
    return await client.get(url, proxy=proxy)

async def main(urls):
    sem = asyncio.Semaphore(10)
    async with httpx.AsyncClient(timeout=20) as client:
        async def worker(url, proxy):
            async with sem:
                return await fetch(client, url, proxy)
        tasks = [worker(u, PROXIES[i % len(PROXIES)]) for i, u in enumerate(urls)]
        return await asyncio.gather(*tasks, return_exceptions=True)

asyncio.run(main([f"https://example.com/page/{i}" for i in range(50)]))


Note the mapping: one proxy per task, assigned up front. Reusing a single long-lived `AsyncClient` with a per-request `proxy=` argument is the supported path; building a new client per request throws away connection pooling and you'll feel it at a few hundred requests per minute.

## Sticky vs rotating sessions — the flag that changes everything

This is where most "python rotating proxy" scripts quietly go wrong. Rotating everything is fine for scraping listings. It is fatal for anything with a login step, a shopping cart, or a multi-page form, because the server sees a new visitor mid-flow.

For a provider-side gateway, session duration lives in the username. The general shape is a chain of key-value segments:


<sub-user>-country-<cc>-st-<state>-city-<city>-isp-<asn>-sst-<minutes>-ssid-<id>


Concretely, with 9Proxy's documented format:

python
# New IP every request — scraping, price checks
USER = "subaccount-country-us"

# Same US IP held for 15 minutes
USER = "subaccount-country-us-sst-15"

# Same city, sticky, with a session id so parallel tasks don't collide
USER = "subaccount-country-us-city-newyork-sst-15-ssid-task07"


The `ssid` part is the one people skip and then regret. Without it, every request sharing the same username config lands on the same IP — so ten parallel workers all funnel through one address and you've accidentally built a DDoS aimed at yourself. A unique `ssid` per worker gives each one its own sticky endpoint.

One trade-off worth knowing: narrowing the filter narrows the pool. Targeting by country alone gives the fastest response and the biggest selection. Piling on state, city and ISP means fewer candidate IPs, slower selection, and more chance of landing somewhere oversaturated. Only pin what the task actually needs.

## Why free proxy lists keep breaking your script

Free lists fail for structural reasons, not because you picked a bad scraper.

- The IPs are public, so everyone scraping the same target is already using them. Blocklists fill up fast.
- Uptime is measured in minutes. A list of 500 that works at 9am is a list of 40 by 11am.
- HTTPS support is inconsistent. A proxy that handles plain HTTP fine may not tunnel TLS at all.
- Validation itself costs you. Checking 500 proxies against a test endpoint is minutes of runtime per run.

Rotating libraries on PyPI — `proxy-rotator`, `proxywhirl`, `rotating-mitmproxy`, `ProxyBroker2`, and similar — are genuinely useful, and several handle health scoring and failover well. But read what they solve: they manage a list. They don't create clean residential IPs. If the list is bad, the smartest selection strategy just picks the best broken option.

The honest split is: use a paid rotating endpoint for the requests that matter, and use a library plus whatever free pool you like for low-stakes work where a failure costs nothing.

## What the proxy layer actually costs

Residential providers price two ways, and the choice is mostly about your request pattern rather than your budget.

**Pay per GB** suits high-rotation work where each request moves little data — JSON APIs, geo-checks, SERP lookups, light HTML. You generate unlimited endpoints and spend only bandwidth, so per-request IP churn is free.

**Pay per IP** suits sustained sessions with unknown bandwidth. You buy a fixed number of IPs with unlimited traffic, and unused IPs don't expire, so nothing is wasted between projects. The catch is that the setup needs the desktop app (local port forwarding), and an IP's natural lifetime is a few hours up to roughly 24 hours.

9Proxy runs both, plus bundles. Note that IP-based and Bundle prices went up on 1 June 2026 — the first change in the company's history, per its own announcement — while GB rates stayed put. Figures below are the current published rates.

### Residential proxy by bandwidth (GB)

| Package | Effective rate | Total price | Validity | Buy |
| --- | --- | --- | --- | --- |
| 5 GB | $3.00 / GB | $15 | 180 days | [Get the 5 GB package](https://bit.ly/9-Proxy) |
| 50 GB + 5 GB bonus | $2.10 / GB | $105 | 180 days | [Get 50 GB + 5 GB free](https://bit.ly/9-Proxy) |
| 100 GB | $1.50 / GB | $150 | 180 days | [Get the 100 GB package](https://bit.ly/9-Proxy) |
| 200 GB | $1.00 / GB | $200 | 180 days | [Get the 200 GB package](https://bit.ly/9-Proxy) |
| 1,000 GB | $0.80 / GB | $800 | 180 days | [Get the 1,000 GB package](https://bit.ly/9-Proxy) |
| 2,000 GB | $0.75 / GB | $1,500 | 180 days | [Get the 2,000 GB package](https://bit.ly/9-Proxy) |
| 3,000 GB (Enterprise) | $0.72 / GB | $2,160 | No expiry | [Compare Enterprise bandwidth plans](https://bit.ly/9-Proxy) |
| 6,000 GB (Enterprise) | $0.70 / GB | $4,200 | No expiry | [Compare Enterprise bandwidth plans](https://bit.ly/9-Proxy) |
| 10,000 GB (Enterprise) | $0.68 / GB | $6,800 | No expiry | [Compare Enterprise bandwidth plans](https://bit.ly/9-Proxy) |

The jump from $3.00 to $1.00 per GB between the 5 GB and 200 GB tiers is the whole argument for not buying small once you know a project will run for months. The 5 GB package is a test, not a workflow.

### Residential proxy by IP

| Package | Effective rate | Total price | Bandwidth | Buy |
| --- | --- | --- | --- | --- |
| 100 IPs | $0.24 / IP | $24 | Unlimited | [Get 100 IPs](https://bit.ly/9-Proxy) |
| 500 IPs | $0.144 / IP | $72 | Unlimited | [Get 500 IPs](https://bit.ly/9-Proxy) |
| 1,000 IPs + 500 bonus | $0.084 / IP | $126 | Unlimited | [Get 1,000 IPs + 500 free](https://bit.ly/9-Proxy) |
| 2,500 IPs | $0.084 / IP | $210 | Unlimited | [Get 2,500 IPs](https://bit.ly/9-Proxy) |
| 5,000 IPs | $0.072 / IP | $360 | Unlimited | [Get 5,000 IPs](https://bit.ly/9-Proxy) |
| 15,000 IPs | $0.048 / IP | $720 | Unlimited | [Get 15,000 IPs](https://bit.ly/9-Proxy) |
| 25,000 IPs | $0.035 / IP | $863 | Unlimited | [Get 25,000 IPs](https://bit.ly/9-Proxy) |
| 50,000 IPs | $0.029 / IP | $1,438 | Unlimited | [Get 50,000 IPs](https://bit.ly/9-Proxy) |
| 100,000 IPs (Business) | $0.023 / IP | $2,300 | Unlimited | [Review Business IP pricing](https://bit.ly/9-Proxy) |
| 200,000 IPs (Business) | $0.021 / IP | $4,140 | Unlimited | [Review Business IP pricing](https://bit.ly/9-Proxy) |
| 500,000 IPs (Business) | $0.018 / IP | $8,625 | Unlimited | [Review Business IP pricing](https://bit.ly/9-Proxy) |

The 1,000 + 500 row is the odd one out and the best entry point: $126 buys 1,500 IPs including the bonus, which is a lower per-IP cost than the 2,500 tier. Read the bonus columns before assuming bigger is always cheaper.

### Bundle packages

| Bundle | What's included | Total price | Buy |
| --- | --- | --- | --- |
| Starter | 100 IPs + 5 GB | $30 | [Get the Starter bundle](https://bit.ly/9-Proxy) |
| Popular | 1,500 IPs + 50 GB | $180 | [Get the Popular bundle](https://bit.ly/9-Proxy) |
| Pro | 5,000 IPs + 500 GB | $720 | [Get the Pro bundle](https://bit.ly/9-Proxy) |

Bundles make sense for mixed workloads — one pipeline holding sessions on fixed IPs, another rotating hard on bandwidth. If your script only does one of those, buying the matching product alone is cheaper. All three bundle tiers are also covered by the June 2026 adjustment, and the bandwidth inside them keeps the 180-day validity window.

On discounts: 9Proxy states that its affiliate program gives referred users 5% off, and the pricing pages occasionally show extra percentage discounts or bonus units on specific payment methods. Checkout is the only place where the real number is settled — the pricing page number isn't always what leaves your card.

If you're unsure which model fits, 👉 [create a 9Proxy account and start with a small GB package](https://bit.ly/9-Proxy). A limited trial is available for new users depending on stock, so it's worth asking support which one you want — IP-based or GB-based — before buying.

## Errors you'll actually see, and what they mean

| What you see | Likely cause | What to change |
| --- | --- | --- |
| `407 Proxy Authentication Required` | Credentials wrong, or the username string is malformed | Rebuild the username from the dashboard generator; check for a stray character in the session flags |
| `429` even after rotating | Rate limit is per account or per fingerprint, not per IP | Slow down, add jitter, rotate the User-Agent too |
| `ConnectTimeout` on most requests | Local firewall, port forwarding not running, or an IP-based setup without the desktop app open | Confirm the app is forwarding before blaming the code |
| Empty responses, HTTP 200 | The block page renders with a 200 and no content | Validate on body length or a known marker, not just status code |
| All parallel workers share one IP | Same `ssid` across threads | Give each worker a unique `ssid` |
| Works in a loop, breaks with `asyncio.gather` | Reusing one session across concurrent tasks | Create a session per task, cap concurrency with a semaphore |

The "HTTP 200 but empty" one catches the most people, because the retry logic never fires. Check for a marker string you know should be on a real page.

## A setup order that wastes less time

Get the request layer right before spending money on proxies. A script that retries badly will burn through a 200 GB package and still miss data.

1. Log every request with the proxy used, status, byte count and latency. Debugging rotation without this is guesswork.
2. Route at least one known-good proxy through the full pipeline end to end. If it fails, it's your parser, not your pool.
3. Add rotation next: per-request for list scraping, sticky with a per-worker `ssid` for anything stateful.
4. Buy the smallest payment to match your traffic shape — per GB when rotation is constant and payloads are small, per IP when sessions run long and bandwidth is unpredictable.
5. Validate on content, not status codes, and cap concurrency so you learn the target's real tolerance instead of discovering it in a ban.

The pattern in production setups is consistently boring: clean residential IPs behind one endpoint, rotation handled by the username string rather than inside the scraper, retries with jitter, and concurrency deliberately limited. The clever part of a rotating proxy setup in Python is mostly deciding what to leave out.
