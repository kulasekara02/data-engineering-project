/**
 * DATA ENGINEERING PLATFORM - ADVANCED DASHBOARD
 * Multi-tab dashboard with advanced analytics, streaming, quality, and lineage.
 */

const API = '/api';
const COLORS = {
    blue: 'rgba(37, 99, 235, 0.8)', green: 'rgba(22, 163, 74, 0.8)',
    orange: 'rgba(245, 158, 11, 0.8)', red: 'rgba(220, 38, 38, 0.8)',
    purple: 'rgba(147, 51, 234, 0.8)', teal: 'rgba(20, 184, 166, 0.8)',
    pink: 'rgba(236, 72, 153, 0.8)', indigo: 'rgba(99, 102, 241, 0.8)',
    cyan: 'rgba(6, 182, 212, 0.8)', amber: 'rgba(217, 119, 6, 0.8)',
};
const PALETTE = Object.values(COLORS);

// Set Chart.js defaults for dark theme
Chart.defaults.color = '#94a3b8';
Chart.defaults.borderColor = 'rgba(51, 65, 85, 0.5)';

const fmt = (v) => '$' + Number(v).toLocaleString('en-US', { maximumFractionDigits: 0 });
const fmtN = (v) => Number(v).toLocaleString('en-US');

async function fetchJSON(endpoint) {
    const r = await fetch(`${API}${endpoint}`);
    if (!r.ok) throw new Error(`API ${r.status}`);
    return r.json();
}

// ===== TAB NAVIGATION =====
document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => {
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
        tab.classList.add('active');
        document.getElementById('tab-' + tab.dataset.tab).classList.add('active');
    });
});

// ===== OVERVIEW TAB =====
async function loadOverview() {
    const data = await fetchJSON('/overview');
    document.getElementById('kpi-revenue').textContent = fmt(data.total_revenue);
    document.getElementById('kpi-profit').textContent = fmt(data.total_profit);
    document.getElementById('kpi-orders').textContent = fmtN(data.total_orders);
    document.getElementById('kpi-customers').textContent = fmtN(data.total_customers);
    document.getElementById('kpi-aov').textContent = fmt(data.avg_order_value);
    document.getElementById('kpi-bounce').textContent = data.bounce_rate + '%';
}

async function loadMonthlyChart() {
    const data = await fetchJSON('/sales/monthly');
    new Chart(document.getElementById('chart-monthly'), {
        type: 'line',
        data: {
            labels: data.map(d => `${d.month_name.substring(0, 3)} ${d.year}`),
            datasets: [
                { label: 'Revenue', data: data.map(d => d.revenue), borderColor: COLORS.blue, backgroundColor: 'rgba(37,99,235,0.1)', fill: true, tension: 0.3 },
                { label: 'Profit', data: data.map(d => d.profit), borderColor: COLORS.green, backgroundColor: 'rgba(22,163,74,0.1)', fill: true, tension: 0.3 }
            ]
        },
        options: { responsive: true, scales: { y: { ticks: { callback: v => '$' + (v/1000).toFixed(0) + 'k' } } } }
    });
}

async function loadCategoryChart() {
    const data = await fetchJSON('/sales/by-category');
    new Chart(document.getElementById('chart-category'), {
        type: 'doughnut',
        data: { labels: data.map(d => d.category), datasets: [{ data: data.map(d => d.revenue), backgroundColor: PALETTE }] },
        options: { responsive: true, plugins: { legend: { position: 'right' } } }
    });
}

async function loadChannelChart() {
    const data = await fetchJSON('/sales/by-channel');
    new Chart(document.getElementById('chart-channel'), {
        type: 'bar',
        data: { labels: data.map(d => d.channel_name), datasets: [{ label: 'Revenue', data: data.map(d => d.revenue), backgroundColor: PALETTE, borderRadius: 6 }] },
        options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { ticks: { callback: v => '$' + (v/1000).toFixed(0) + 'k' } } } }
    });
}

async function loadCountryChart() {
    const data = await fetchJSON('/sales/by-country');
    new Chart(document.getElementById('chart-country'), {
        type: 'bar',
        data: { labels: data.map(d => d.country), datasets: [{ label: 'Revenue', data: data.map(d => d.revenue), backgroundColor: COLORS.blue, borderRadius: 6 }] },
        options: { indexAxis: 'y', responsive: true, plugins: { legend: { display: false } } }
    });
}

async function loadSegmentsChart() {
    const data = await fetchJSON('/customers/segments');
    const colors = { 'Platinum': COLORS.purple, 'Gold': COLORS.orange, 'Silver': COLORS.teal, 'Bronze': COLORS.red };
    new Chart(document.getElementById('chart-segments'), {
        type: 'pie',
        data: { labels: data.map(d => `${d.segment} (${d.customer_count})`), datasets: [{ data: data.map(d => d.customer_count), backgroundColor: data.map(d => colors[d.segment] || COLORS.blue) }] },
        options: { responsive: true, plugins: { legend: { position: 'right' } } }
    });
}

async function loadDailyChart() {
    const data = await fetchJSON('/sales/daily-pattern');
    new Chart(document.getElementById('chart-daily'), {
        type: 'bar',
        data: {
            labels: data.map(d => d.day_name),
            datasets: [
                { label: 'Orders', data: data.map(d => d.orders), backgroundColor: COLORS.blue, borderRadius: 6, yAxisID: 'y' },
                { label: 'Avg Order Value', data: data.map(d => d.avg_order), type: 'line', borderColor: COLORS.orange, yAxisID: 'y1', tension: 0.3 }
            ]
        },
        options: { responsive: true, scales: { y: { position: 'left' }, y1: { position: 'right', grid: { drawOnChartArea: false } } } }
    });
}

async function loadEngagementChart() {
    const data = await fetchJSON('/activity/engagement');
    new Chart(document.getElementById('chart-engagement'), {
        type: 'bar',
        data: {
            labels: data.map(d => d.channel_name),
            datasets: [
                { label: 'Avg Duration (sec)', data: data.map(d => d.avg_duration_sec), backgroundColor: COLORS.blue, borderRadius: 6 },
                { label: 'Avg Pages (x50)', data: data.map(d => d.avg_pages * 50), backgroundColor: COLORS.green, borderRadius: 6 },
                { label: 'Bounce Rate (x10)', data: data.map(d => d.bounce_rate_pct * 10), backgroundColor: COLORS.red, borderRadius: 6 }
            ]
        },
        options: { responsive: true }
    });
}

async function loadProductsTable() {
    const data = await fetchJSON('/products/top');
    document.querySelector('#table-products tbody').innerHTML = data.map((p, i) =>
        `<tr><td>${i+1}</td><td>${p.product_name}</td><td>${p.category}</td><td>${fmtN(p.units_sold)}</td><td>${fmt(p.revenue)}</td><td>${fmt(p.profit)}</td></tr>`
    ).join('');
}

// ===== ADVANCED ANALYTICS TAB =====
async function loadAdvancedTrend() {
    const data = await fetchJSON('/advanced/monthly-trend');
    new Chart(document.getElementById('chart-trend-advanced'), {
        type: 'line',
        data: {
            labels: data.map(d => `${d.month_name.substring(0,3)} ${d.year}`),
            datasets: [
                { label: 'Revenue', data: data.map(d => d.revenue), borderColor: COLORS.blue, tension: 0.3 },
                { label: '3M Moving Avg', data: data.map(d => d.moving_avg_3m), borderColor: COLORS.orange, borderDash: [5, 5], tension: 0.3 },
                { label: 'Cumulative Revenue', data: data.map(d => d.cumulative_revenue), borderColor: COLORS.purple, fill: false, tension: 0.3, yAxisID: 'y1' }
            ]
        },
        options: {
            responsive: true,
            scales: {
                y: { position: 'left', ticks: { callback: v => '$' + (v/1000).toFixed(0) + 'k' } },
                y1: { position: 'right', grid: { drawOnChartArea: false }, ticks: { callback: v => '$' + (v/1000000).toFixed(1) + 'M' } }
            }
        }
    });
}

async function loadRFMChart() {
    const data = await fetchJSON('/advanced/rfm-summary');
    new Chart(document.getElementById('chart-rfm'), {
        type: 'bar',
        data: {
            labels: data.map(d => d.rfm_segment),
            datasets: [
                { label: 'Customers', data: data.map(d => d.customer_count), backgroundColor: COLORS.indigo, borderRadius: 6 }
            ]
        },
        options: { indexAxis: 'y', responsive: true, plugins: { legend: { display: false } } }
    });
}

async function loadAttributionChart() {
    const data = await fetchJSON('/advanced/channel-attribution');
    new Chart(document.getElementById('chart-attribution'), {
        type: 'polarArea',
        data: {
            labels: data.map(d => `${d.channel_name} (${d.revenue_share_pct}%)`),
            datasets: [{ data: data.map(d => d.attributed_revenue), backgroundColor: PALETTE.map(c => c.replace('0.8', '0.6')) }]
        },
        options: { responsive: true, plugins: { legend: { position: 'right' } } }
    });
}

async function loadParetoChart() {
    const data = await fetchJSON('/advanced/pareto');
    const step = Math.max(1, Math.floor(data.length / 100));
    const sampled = data.filter((_, i) => i % step === 0 || i === data.length - 1);
    new Chart(document.getElementById('chart-pareto'), {
        type: 'line',
        data: {
            labels: sampled.map(d => d.customer_percentile + '%'),
            datasets: [
                { label: 'Cumulative Revenue %', data: sampled.map(d => d.cumulative_pct), borderColor: COLORS.blue, fill: true, backgroundColor: 'rgba(37,99,235,0.1)', tension: 0.3 },
                { label: '80% Line', data: sampled.map(() => 80), borderColor: COLORS.red, borderDash: [10, 5], pointRadius: 0 }
            ]
        },
        options: {
            responsive: true,
            scales: {
                x: { title: { display: true, text: '% of Customers (ranked by revenue)' } },
                y: { title: { display: true, text: 'Cumulative Revenue %' }, max: 105 }
            }
        }
    });
}

async function loadCLVTable() {
    const data = await fetchJSON('/advanced/clv');
    document.querySelector('#table-clv tbody').innerHTML = data.slice(0, 20).map((c, i) =>
        `<tr><td>${i+1}</td><td>${c.name}</td><td>${c.country}</td><td>${c.total_purchases}</td><td>${fmt(c.total_spent)}</td><td>${c.purchase_freq_monthly}</td><td>${fmt(c.predicted_clv_24m)}</td></tr>`
    ).join('');
}

async function loadRFMTable() {
    const data = await fetchJSON('/advanced/rfm-summary');
    document.querySelector('#table-rfm tbody').innerHTML = data.map(d =>
        `<tr><td>${d.rfm_segment}</td><td>${d.customer_count}</td><td>${fmt(d.avg_monetary)}</td><td>${d.avg_frequency}</td><td>${d.avg_recency_days}</td></tr>`
    ).join('');
}

// ===== LIVE API DATA TAB =====
function fmtBig(v) {
    if (v >= 1e12) return '$' + (v/1e12).toFixed(2) + 'T';
    if (v >= 1e9) return '$' + (v/1e9).toFixed(2) + 'B';
    if (v >= 1e6) return '$' + (v/1e6).toFixed(1) + 'M';
    return '$' + fmtN(v);
}

async function loadCryptoData() {
    try {
        const [coins, summary] = await Promise.all([
            fetchJSON('/live/crypto'), fetchJSON('/live/crypto/summary')
        ]);
        if (!coins.length) return;

        // KPIs
        document.getElementById('crypto-mcap').textContent = fmtBig(summary.total_market_cap || 0);
        document.getElementById('crypto-vol').textContent = fmtBig(summary.total_volume_24h || 0);
        document.getElementById('crypto-change').textContent = (summary.avg_change_pct || 0).toFixed(2) + '%';
        document.getElementById('crypto-gl').textContent = `${summary.gainers || 0} / ${summary.losers || 0}`;

        // Chart - top 15 by market cap
        const top15 = coins.slice(0, 15);
        new Chart(document.getElementById('chart-crypto'), {
            type: 'bar',
            data: {
                labels: top15.map(c => c.name),
                datasets: [
                    { label: 'Market Cap ($B)', data: top15.map(c => (c.market_cap || 0) / 1e9), backgroundColor: COLORS.blue, borderRadius: 6, yAxisID: 'y' },
                    { label: '24h Change %', data: top15.map(c => c.price_change_pct_24h || 0), type: 'line',
                      borderColor: COLORS.orange, pointBackgroundColor: top15.map(c => (c.price_change_pct_24h || 0) >= 0 ? COLORS.green : COLORS.red),
                      yAxisID: 'y1', tension: 0.3 }
                ]
            },
            options: {
                responsive: true,
                scales: {
                    y: { position: 'left', title: { display: true, text: 'Market Cap ($B)' } },
                    y1: { position: 'right', grid: { drawOnChartArea: false }, title: { display: true, text: '24h Change (%)' } }
                }
            }
        });

        // Table
        document.querySelector('#table-crypto tbody').innerHTML = coins.slice(0, 20).map((c, i) => {
            const changeClass = (c.price_change_pct_24h || 0) >= 0 ? 'badge-pass' : 'badge-fail';
            const changeSign = (c.price_change_pct_24h || 0) >= 0 ? '+' : '';
            return `<tr>
                <td>${c.rank || i+1}</td>
                <td><strong>${c.name}</strong></td>
                <td>${(c.symbol || '').toUpperCase()}</td>
                <td>$${Number(c.current_price_usd || 0).toLocaleString('en-US', {maximumFractionDigits: 2})}</td>
                <td><span class="badge ${changeClass}">${changeSign}${(c.price_change_pct_24h || 0).toFixed(2)}%</span></td>
                <td>${fmtBig(c.market_cap || 0)}</td>
                <td>${fmtBig(c.total_volume || 0)}</td>
            </tr>`;
        }).join('');
    } catch (e) { console.warn('Crypto load failed:', e); }
}

async function loadRegionsChart() {
    try {
        const data = await fetchJSON('/live/countries/by-region');
        if (!data.length) return;
        new Chart(document.getElementById('chart-regions'), {
            type: 'doughnut',
            data: {
                labels: data.map(d => `${d.region} (${(d.total_population/1e9).toFixed(2)}B)`),
                datasets: [{ data: data.map(d => d.total_population), backgroundColor: PALETTE }]
            },
            options: { responsive: true, plugins: { legend: { position: 'right' } } }
        });
    } catch (e) { console.warn('Regions load failed:', e); }
}

async function loadGithubChart() {
    try {
        const data = await fetchJSON('/live/github/by-language');
        if (!data.length) return;
        new Chart(document.getElementById('chart-github-lang'), {
            type: 'bar',
            data: {
                labels: data.map(d => d.language),
                datasets: [{ label: 'Total Stars', data: data.map(d => d.total_stars), backgroundColor: PALETTE, borderRadius: 6 }]
            },
            options: { responsive: true, plugins: { legend: { display: false } },
                       scales: { y: { ticks: { callback: v => (v/1000).toFixed(0) + 'k' } } } }
        });
    } catch (e) { console.warn('GitHub load failed:', e); }
}

async function loadWeatherChart() {
    try {
        const data = await fetchJSON('/live/weather');
        if (!data.length) return;
        new Chart(document.getElementById('chart-weather'), {
            type: 'bar',
            data: {
                labels: data.map(d => `${d.city} (${d.country})`),
                datasets: [
                    { label: 'Temperature (C)', data: data.map(d => d.temperature_c),
                      backgroundColor: data.map(d => d.temperature_c > 30 ? COLORS.red : d.temperature_c > 20 ? COLORS.orange : d.temperature_c > 10 ? COLORS.green : COLORS.blue),
                      borderRadius: 6, yAxisID: 'y' },
                    { label: 'Humidity (%)', data: data.map(d => d.humidity_pct), type: 'line',
                      borderColor: COLORS.cyan, yAxisID: 'y1', tension: 0.3 }
                ]
            },
            options: {
                responsive: true,
                scales: {
                    y: { position: 'left', title: { display: true, text: 'Temp (C)' } },
                    y1: { position: 'right', grid: { drawOnChartArea: false }, title: { display: true, text: 'Humidity (%)' }, max: 100 }
                }
            }
        });
    } catch (e) { console.warn('Weather load failed:', e); }
}

async function loadForexChart() {
    try {
        const data = await fetchJSON('/live/exchange-rates');
        if (!data.length) return;
        const major = ['EUR','GBP','JPY','AUD','CAD','CHF','CNY','INR','KRW','BRL','SGD','NZD','MXN','ZAR','SEK'];
        const filtered = data.filter(d => major.includes(d.target_currency));
        new Chart(document.getElementById('chart-forex'), {
            type: 'bar',
            data: {
                labels: filtered.map(d => `USD/${d.target_currency}`),
                datasets: [{ label: 'Exchange Rate', data: filtered.map(d => d.rate), backgroundColor: COLORS.indigo, borderRadius: 6 }]
            },
            options: { responsive: true, plugins: { legend: { display: false } } }
        });
    } catch (e) { console.warn('Forex load failed:', e); }
}

async function loadGithubTable() {
    try {
        const data = await fetchJSON('/live/github');
        if (!data.length) return;
        document.querySelector('#table-github tbody').innerHTML = data.map((r, i) =>
            `<tr><td>${i+1}</td><td><strong>${r.repo_name}</strong></td><td>${r.language || 'N/A'}</td>
             <td>${fmtN(r.stars)}</td><td>${fmtN(r.forks)}</td><td>${(r.description || '').substring(0,80)}</td></tr>`
        ).join('');
    } catch (e) { console.warn('GitHub table failed:', e); }
}

async function loadCountriesTable() {
    try {
        const data = await fetchJSON('/live/countries');
        if (!data.length) return;
        document.querySelector('#table-countries tbody').innerHTML = data.slice(0, 30).map((c, i) =>
            `<tr><td>${i+1}</td><td>${c.flag_emoji || ''} ${c.name}</td><td>${c.capital}</td>
             <td>${c.region}</td><td>${fmtN(c.population)}</td><td>${fmtN(Math.round(c.area || 0))}</td></tr>`
        ).join('');
    } catch (e) { console.warn('Countries table failed:', e); }
}

async function loadIngestionLog() {
    try {
        const data = await fetchJSON('/live/ingestion-log');
        if (!data.length) return;
        document.querySelector('#table-ingestion tbody').innerHTML = data.map(d => {
            const cls = d.status === 'SUCCESS' ? 'badge-success' : 'badge-fail';
            return `<tr><td>${d.source}</td><td><span class="badge ${cls}">${d.status}</span></td>
                    <td>${d.records_count}</td><td>${d.started_at || ''}</td><td>${d.completed_at || ''}</td>
                    <td>${d.error_message || '-'}</td></tr>`;
        }).join('');
    } catch (e) { console.warn('Ingestion log failed:', e); }
}

// ===== STREAMING TAB =====
async function loadStreamData() {
    const data = await fetchJSON('/advanced/stream-windows');
    if (!data.length) {
        document.querySelector('#table-stream tbody').innerHTML = '<tr><td colspan="6">No streaming data yet. Run the pipeline first.</td></tr>';
        return;
    }
    new Chart(document.getElementById('chart-stream'), {
        type: 'bar',
        data: {
            labels: data.map(d => d.window_start.split('T')[1] || d.window_start),
            datasets: [
                { label: 'Events', data: data.map(d => d.total_events), backgroundColor: COLORS.blue, borderRadius: 4, yAxisID: 'y' },
                { label: 'Revenue', data: data.map(d => d.total_revenue), type: 'line', borderColor: COLORS.green, yAxisID: 'y1', tension: 0.3 }
            ]
        },
        options: { responsive: true, scales: { y: { position: 'left' }, y1: { position: 'right', grid: { drawOnChartArea: false } } } }
    });

    document.querySelector('#table-stream tbody').innerHTML = data.map(d =>
        `<tr><td>${d.window_start}</td><td>${d.total_events}</td><td>${fmt(d.total_revenue)}</td><td>${d.unique_customers}</td><td>${d.page_views}</td><td>${d.purchases}</td></tr>`
    ).join('');
}

// ===== DATA QUALITY TAB =====
async function loadQualityData() {
    const data = await fetchJSON('/advanced/data-quality');
    if (data.message) {
        document.getElementById('q-score').textContent = 'N/A';
        return;
    }

    document.getElementById('q-passed').textContent = data.passed;
    document.getElementById('q-warnings').textContent = data.warnings;
    document.getElementById('q-failures').textContent = data.failures;
    const score = Math.round(data.passed / data.total_checks * 100);
    document.getElementById('q-score').textContent = score + '%';

    // Category chart
    const categories = {};
    data.checks.forEach(c => { categories[c.category] = (categories[c.category] || 0) + 1; });
    new Chart(document.getElementById('chart-quality-cat'), {
        type: 'bar',
        data: { labels: Object.keys(categories), datasets: [{ label: 'Checks', data: Object.values(categories), backgroundColor: PALETTE, borderRadius: 6 }] },
        options: { responsive: true, plugins: { legend: { display: false } } }
    });

    // Status distribution
    new Chart(document.getElementById('chart-quality-status'), {
        type: 'doughnut',
        data: {
            labels: ['Passed', 'Warnings', 'Failures'],
            datasets: [{ data: [data.passed, data.warnings, data.failures], backgroundColor: [COLORS.green, COLORS.orange, COLORS.red] }]
        },
        options: { responsive: true }
    });

    // Table
    document.querySelector('#table-quality tbody').innerHTML = data.checks.map(c => {
        const cls = c.status === 'PASS' ? 'badge-pass' : c.status === 'WARN' ? 'badge-warn' : 'badge-fail';
        return `<tr><td><span class="badge ${cls}">${c.status}</span></td><td>${c.severity}</td><td>${c.category}</td><td>${c.check}</td><td>${c.details}</td></tr>`;
    }).join('');
}

// ===== LINEAGE TAB =====
async function loadLineageData() {
    const data = await fetchJSON('/advanced/lineage');
    if (data.message) {
        document.querySelector('#table-lineage tbody').innerHTML = `<tr><td colspan="6">${data.message}</td></tr>`;
        return;
    }

    const tasks = data.lineage || data;
    if (!tasks.length) return;

    // Timeline chart
    new Chart(document.getElementById('chart-lineage'), {
        type: 'bar',
        data: {
            labels: tasks.map(t => t.task_id),
            datasets: [{
                label: 'Duration (sec)',
                data: tasks.map(t => t.duration_sec || 0),
                backgroundColor: tasks.map(t => t.status === 'success' ? COLORS.green : t.status === 'failed' ? COLORS.red : COLORS.teal),
                borderRadius: 6
            }]
        },
        options: { indexAxis: 'y', responsive: true, plugins: { legend: { display: false } } }
    });

    document.querySelector('#table-lineage tbody').innerHTML = tasks.map(t => {
        const cls = t.status === 'success' ? 'badge-success' : t.status === 'failed' ? 'badge-fail' : 'badge-skipped';
        return `<tr><td>${t.task_id}</td><td>${t.description}</td><td><span class="badge ${cls}">${t.status}</span></td><td>${t.attempt}</td><td>${t.duration_sec ? t.duration_sec.toFixed(2) + 's' : 'N/A'}</td><td>${t.error || '-'}</td></tr>`;
    }).join('');
}

// ===== INIT =====
async function init() {
    try {
        await Promise.all([
            loadOverview(), loadMonthlyChart(), loadCategoryChart(), loadChannelChart(),
            loadCountryChart(), loadSegmentsChart(), loadDailyChart(), loadEngagementChart(), loadProductsTable(),
            loadAdvancedTrend(), loadRFMChart(), loadAttributionChart(), loadParetoChart(), loadCLVTable(), loadRFMTable(),
            loadCryptoData(), loadRegionsChart(), loadGithubChart(), loadWeatherChart(), loadForexChart(),
            loadGithubTable(), loadCountriesTable(), loadIngestionLog(),
            loadStreamData(), loadQualityData(), loadLineageData(),
        ]);
        console.log('Dashboard loaded!');
    } catch (e) { console.error('Dashboard error:', e); }
}

document.addEventListener('DOMContentLoaded', init);
