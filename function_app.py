import azure.functions as func
import feedparser
import json
import logging
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient
import os

app = func.FunctionApp()

TICKERS = {
    "RELIANCE": ["reliance", "ril", "mukesh ambani"],
    "TCS": ["tcs", "tata consultancy"],
    "INFY": ["infosys", "infy"],
    "HDFCBANK": ["hdfc bank", "hdfcbank"],
    "ICICIBANK": ["icici bank", "icicibank"],
    "WIPRO": ["wipro"],
    "SBIN": ["sbi", "state bank"],
    "ADANIENT": ["adani"],
    "BAJFINANCE": ["bajaj finance"],
    "HINDUNILVR": ["hindustan unilever", "hul"]
}

FEEDS = [
    {"source": "Livemint Markets", "url": "https://www.livemint.com/rss/markets"},
    {"source": "Moneycontrol Buzzing", "url": "https://www.moneycontrol.com/rss/buzzingstocks.xml"}
]

def detect_tickers(text):
    text_lower = text.lower()
    found = []
    for ticker, keywords in TICKERS.items():
        for kw in keywords:
            if kw in text_lower:
                found.append(ticker)
                break
    return list(set(found)) if found else ["GENERAL"]

@app.timer_trigger(schedule="0 0 * * * *", arg_name="mytimer", run_on_startup=True)
def ingest_news(mytimer: func.TimerRequest) -> None:
    logging.info("RSS ingestion function triggered")
    connection_string = os.environ["AzureWebJobsStorage"]
    container_name = "raw-news"
    blob_service = BlobServiceClient.from_connection_string(connection_string)
    articles = []
    for feed_info in FEEDS:
        try:
            feed = feedparser.parse(feed_info["url"])
            logging.info(f"Feed {feed_info['source']} returned {len(feed.entries)} entries")
            for entry in feed.entries[:20]:
                title = entry.get("title", "")
                summary = entry.get("summary", "")
                tickers = detect_tickers(title + " " + summary)
                article = {
                    "ticker": tickers[0],
                    "all_tickers": tickers,
                    "headline": title,
                    "summary": summary[:500],
                    "source": feed_info["source"],
                    "url": entry.get("link", ""),
                    "published_at": entry.get("published", str(datetime.now(timezone.utc)))
                }
                articles.append(article)
        except Exception as e:
            logging.error(f"Error fetching {feed_info['source']}: {e}")
    logging.info(f"Total articles fetched: {len(articles)}")
    if articles:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        blob_name = f"news_{timestamp}.json"
        blob_client = blob_service.get_blob_client(container=container_name, blob=blob_name)
        blob_client.upload_blob(json.dumps(articles), overwrite=True)
        logging.info(f"Uploaded {len(articles)} articles to blob: {blob_name}")
    else:
        logging.warning("No articles fetched - nothing uploaded to blob")

@app.blob_trigger(arg_name="myblob", path="raw-news/{name}", connection="AzureWebJobsStorage")
def process_news(myblob: func.InputStream) -> None:
    logging.info(f"Blob trigger fired for: {myblob.name}")
    from azure.ai.textanalytics import TextAnalyticsClient
    from azure.core.credentials import AzureKeyCredential
    import pyodbc
    articles = json.loads(myblob.read().decode("utf-8"))
    language_key = os.environ["LANGUAGE_KEY"]
    language_endpoint = os.environ["LANGUAGE_ENDPOINT"]
    ta_client = TextAnalyticsClient(
        endpoint=language_endpoint,
        credential=AzureKeyCredential(language_key)
    )
    sql_conn_str = os.environ["SQL_CONNECTION_STRING"]
    conn = pyodbc.connect(sql_conn_str)
    cursor = conn.cursor()
    for article in articles:
        try:
            response = ta_client.analyze_sentiment([article["headline"]])
            sentiment_result = response[0]
            sentiment = sentiment_result.sentiment
            score = max(
                sentiment_result.confidence_scores.positive,
                sentiment_result.confidence_scores.negative,
                sentiment_result.confidence_scores.neutral
            )
            label = "bullish" if sentiment == "positive" else "bearish" if sentiment == "negative" else "neutral"
            cursor.execute("""
                INSERT INTO stock_news (ticker, headline, summary, source, url, published_at, sentiment, sentiment_score)
                VALUES (?, ?, ?, ?, ?, GETDATE(), ?, ?)
            """, article["ticker"], article["headline"], article["summary"],
                article["source"], article["url"], label, score)
        except Exception as e:
            logging.error(f"Error processing article: {e}")
    conn.commit()
    cursor.close()
    conn.close()
    logging.info(f"Processed {len(articles)} articles into SQL")

@app.timer_trigger(schedule="0 30 * * * *", arg_name="dashTimer", run_on_startup=True)
def refresh_dashboard(dashTimer: func.TimerRequest) -> None:
    logging.info("Dashboard refresh triggered")
    import pyodbc
    import json as json_mod
    from azure.storage.blob import BlobServiceClient as BSC
    conn_str = os.environ["SQL_CONNECTION_STRING"]
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT sentiment, COUNT(*) FROM stock_news GROUP BY sentiment")
    sentiment_data = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.execute("SELECT TOP 10 ticker, COUNT(*) as cnt FROM stock_news WHERE ticker != 'GENERAL' GROUP BY ticker ORDER BY cnt DESC")
    ticker_data = [(row[0], row[1]) for row in cursor.fetchall()]
    cursor.execute("SELECT TOP 10 ticker, headline, sentiment_score, source, ingested_at, url FROM stock_news WHERE sentiment = 'bullish' ORDER BY ingested_at DESC")
    bullish_articles = cursor.fetchall()
    cursor.execute("SELECT TOP 10 ticker, headline, sentiment_score, source, ingested_at, url FROM stock_news WHERE sentiment = 'bearish' ORDER BY ingested_at DESC")
    bearish_articles = cursor.fetchall()
    cursor.execute("SELECT TOP 20 ticker, headline, sentiment, sentiment_score, source, ingested_at, url FROM stock_news ORDER BY ingested_at DESC")
    latest = cursor.fetchall()
    cursor.close()
    conn.close()
    sentiment_colors = {"bullish": "#22c55e", "bearish": "#ef4444", "neutral": "#f59e0b"}
    def make_rows(articles, color):
        html = ""
        for r in articles:
            url = r[5] or "#"
            html += f'<tr><td style="padding:10px;font-weight:600;color:{color}">{r[0]}</td><td style="padding:10px;max-width:400px"><a href="{url}" target="_blank" style="color:#e2e8f0;text-decoration:none;">{r[1][:80]}...</a></td><td style="padding:10px;color:#64748b">{r[3]}</td><td style="padding:10px;color:#64748b">{str(r[4])[:16]}</td></tr>'
        return html or '<tr><td colspan="4" style="padding:20px;color:#64748b;text-align:center">No signals yet</td></tr>'
    rows_html = ""
    for r in latest:
        color = sentiment_colors.get(r[2], "#gray")
        badge = f'<span style="background:{color};color:white;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:600">{r[2].upper()}</span>'
        url = r[6] or "#"
        rows_html += f'<tr><td style="padding:10px;font-weight:600;color:#6366f1">{r[0]}</td><td style="padding:10px;max-width:400px"><a href="{url}" target="_blank" style="color:#e2e8f0;text-decoration:none;">{r[1][:80]}...</a></td><td style="padding:10px">{badge}</td><td style="padding:10px;color:#64748b">{r[4]}</td><td style="padding:10px;color:#64748b">{str(r[5])[:16]}</td></tr>'
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Stock News Sentinel</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ font-family: 'Segoe UI', sans-serif; background:#0f172a; color:#e2e8f0; }}
  .header {{ background:linear-gradient(135deg,#6366f1,#8b5cf6); padding:30px 40px; }}
  .header h1 {{ font-size:28px; font-weight:700; }}
  .header p {{ opacity:0.8; margin-top:5px; }}
  .container {{ max-width:1200px; margin:0 auto; padding:30px 20px; }}
  .cards {{ display:grid; grid-template-columns:repeat(3,1fr); gap:20px; margin-bottom:30px; }}
  .card {{ background:#1e293b; border-radius:12px; padding:24px; border:1px solid #334155; }}
  .card .label {{ font-size:13px; color:#94a3b8; margin-bottom:8px; }}
  .card .value {{ font-size:32px; font-weight:700; }}
  .charts {{ display:grid; grid-template-columns:1fr 1fr; gap:20px; margin-bottom:30px; }}
  .chart-box {{ background:#1e293b; border-radius:12px; padding:24px; border:1px solid #334155; }}
  .chart-box h3 {{ font-size:16px; margin-bottom:20px; color:#94a3b8; }}
  .signals {{ display:grid; grid-template-columns:1fr 1fr; gap:20px; margin-bottom:30px; }}
  table {{ width:100%; border-collapse:collapse; }}
  thead tr {{ background:#1e293b; }}
  thead th {{ padding:12px 10px; text-align:left; font-size:12px; color:#94a3b8; text-transform:uppercase; }}
  tbody tr {{ border-bottom:1px solid #1e293b; background:#0f172a; }}
  tbody tr:hover {{ background:#1e293b; }}
  .table-box {{ background:#0f172a; border-radius:12px; border:1px solid #334155; overflow:hidden; margin-bottom:30px; }}
  .table-title {{ padding:20px; font-size:16px; color:#94a3b8; border-bottom:1px solid #334155; }}
  .bullish-box {{ background:#052e16; border-radius:12px; border:1px solid #166534; overflow:hidden; }}
  .bearish-box {{ background:#2d0a0a; border-radius:12px; border:1px solid #7f1d1d; overflow:hidden; }}
  .bullish-title {{ padding:20px; font-size:16px; color:#22c55e; border-bottom:1px solid #166534; font-weight:600; }}
  .bearish-title {{ padding:20px; font-size:16px; color:#ef4444; border-bottom:1px solid #7f1d1d; font-weight:600; }}
  .updated {{ text-align:right; font-size:12px; color:#475569; margin-top:20px; }}
</style>
</head>
<body>
<div class="header"><h1>📈 Stock News Sentinel</h1><p>Real-time Indian stock market news sentiment analysis powered by Azure AI</p></div>
<div class="container">
  <div class="cards">
    <div class="card"><div class="label">Total Articles Analyzed</div><div class="value" style="color:#6366f1">{sum(sentiment_data.values())}</div></div>
    <div class="card"><div class="label">Bearish Signals 🔴</div><div class="value" style="color:#ef4444">{sentiment_data.get('bearish', 0)}</div></div>
    <div class="card"><div class="label">Bullish Signals 🟢</div><div class="value" style="color:#22c55e">{sentiment_data.get('bullish', 0)}</div></div>
  </div>
  <div class="charts">
    <div class="chart-box"><h3>Sentiment Distribution</h3><canvas id="sentimentChart"></canvas></div>
    <div class="chart-box"><h3>Top Mentioned Tickers</h3><canvas id="tickerChart"></canvas></div>
  </div>
  <div class="signals">
    <div class="bullish-box"><div class="bullish-title">🟢 Latest Bullish Signals</div><table><thead><tr><th>Ticker</th><th>Headline</th><th>Source</th><th>Time</th></tr></thead><tbody>{make_rows(bullish_articles, '#22c55e')}</tbody></table></div>
    <div class="bearish-box"><div class="bearish-title">🔴 Latest Bearish Signals</div><table><thead><tr><th>Ticker</th><th>Headline</th><th>Source</th><th>Time</th></tr></thead><tbody>{make_rows(bearish_articles, '#ef4444')}</tbody></table></div>
  </div>
  <div class="table-box"><div class="table-title">📰 Latest News & Sentiment</div><table><thead><tr><th>Ticker</th><th>Headline</th><th>Sentiment</th><th>Source</th><th>Time</th></tr></thead><tbody>{rows_html}</tbody></table></div>
  <div class="updated">Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC</div>
</div>
<script>
new Chart(document.getElementById('sentimentChart'), {{type:'doughnut',data:{{labels:['Neutral','Bearish','Bullish'],datasets:[{{data:[{sentiment_data.get('neutral',0)},{sentiment_data.get('bearish',0)},{sentiment_data.get('bullish',0)}],backgroundColor:['#f59e0b','#ef4444','#22c55e'],borderWidth:0}}]}},options:{{plugins:{{legend:{{labels:{{color:'#e2e8f0'}}}}}},cutout:'65%'}}}});
new Chart(document.getElementById('tickerChart'), {{type:'bar',data:{{labels:{json_mod.dumps([t[0] for t in ticker_data])},datasets:[{{label:'Articles',data:{json_mod.dumps([t[1] for t in ticker_data])},backgroundColor:'#6366f1',borderRadius:6}}]}},options:{{plugins:{{legend:{{display:false}}}},scales:{{x:{{ticks:{{color:'#94a3b8'}},grid:{{color:'#1e293b'}}}},y:{{ticks:{{color:'#94a3b8'}},grid:{{color:'#1e293b'}}}}}}}}}});
</script>
</body>
</html>"""
    storage_conn = os.environ["AzureWebJobsStorage"]
    blob_service = BSC.from_connection_string(storage_conn)
    blob_client = blob_service.get_blob_client(container="$web", blob="index.html")
    blob_client.upload_blob(html.encode("utf-8"), overwrite=True, content_settings={"content_type": "text/html"})
    logging.info("Dashboard refreshed successfully")

@app.timer_trigger(schedule="0 0 * * * *", arg_name="alertTimer", run_on_startup=False)
def send_alerts(alertTimer: func.TimerRequest) -> None:
    logging.info("Alert function triggered")
    import pyodbc
    import urllib.request
    conn_str = os.environ["SQL_CONNECTION_STRING"]
    bot_token = os.environ["TELEGRAM_BOT_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ticker, COUNT(*) as bearish_count
        FROM stock_news
        WHERE sentiment = 'bearish'
        AND ticker != 'GENERAL'
        AND ingested_at >= DATEADD(hour, -1, GETDATE())
        GROUP BY ticker
        HAVING COUNT(*) >= 3
        ORDER BY bearish_count DESC
    """)
    alerts = cursor.fetchall()
    cursor.close()
    conn.close()
    if not alerts:
        logging.info("No bearish spike alerts to send")
        return
    for ticker, count in alerts:
        message = f"🚨 *Bearish Spike Alert!*\n\nTicker: *{ticker}*\nBearish articles in last 1 hour: *{count}*\n\nCheck dashboard: https://stocknewsstorage2026.z29.web.core.windows.net/"
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        data = json.dumps({"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req)
        logging.info(f"Alert sent for {ticker} with {count} bearish articles")
