/**
 * DATA ENGINEERING DASHBOARD
 * Fetches data from the FastAPI backend and renders interactive charts.
 */

const API_BASE = '/api';

const COLORS = {
    blue: 'rgba(37, 99, 235, 0.8)',
    green: 'rgba(22, 163, 74, 0.8)',
    orange: 'rgba(245, 158, 11, 0.8)',
    red: 'rgba(220, 38, 38, 0.8)',
    purple: 'rgba(147, 51, 234, 0.8)',
    teal: 'rgba(20, 184, 166, 0.8)',
    pink: 'rgba(236, 72, 153, 0.8)',
    indigo: 'rgba(99, 102, 241, 0.8)',
};

const PALETTE = Object.values(COLORS);

function formatCurrency(value) {
    return '$' + Number(value).toLocaleString('en-US', {
        minimumFractionDigits: 0,
        maximumFractionDigits: 0
    });
}

function formatNumber(value) {
    return Number(value).toLocaleString('en-US');
}

// --- Fetch Helper ---
async function fetchData(endpoint) {
    const response = await fetch(`${API_BASE}${endpoint}`);
    if (!response.ok) throw new Error(`API error: ${response.status}`);
    return response.json();
}

// --- Load KPI Cards ---
async function loadOverview() {
    const data = await fetchData('/overview');
    document.getElementById('kpi-revenue').textContent = formatCurrency(data.total_revenue);
    document.getElementById('kpi-profit').textContent = formatCurrency(data.total_profit);
    document.getElementById('kpi-orders').textContent = formatNumber(data.total_orders);
    document.getElementById('kpi-customers').textContent = formatNumber(data.total_customers);
    document.getElementById('kpi-aov').textContent = formatCurrency(data.avg_order_value);
    document.getElementById('kpi-bounce').textContent = data.bounce_rate + '%';
}

// --- Monthly Revenue Chart (Line) ---
async function loadMonthlyChart() {
    const data = await fetchData('/sales/monthly');
    const labels = data.map(d => `${d.month_name.substring(0, 3)} ${d.year}`);

    new Chart(document.getElementById('chart-monthly'), {
        type: 'line',
        data: {
            labels,
            datasets: [
                {
                    label: 'Revenue',
                    data: data.map(d => d.revenue),
                    borderColor: COLORS.blue,
                    backgroundColor: 'rgba(37, 99, 235, 0.1)',
                    fill: true,
                    tension: 0.3,
                },
                {
                    label: 'Profit',
                    data: data.map(d => d.profit),
                    borderColor: COLORS.green,
                    backgroundColor: 'rgba(22, 163, 74, 0.1)',
                    fill: true,
                    tension: 0.3,
                }
            ]
        },
        options: {
            responsive: true,
            plugins: { legend: { position: 'top' } },
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: { callback: v => '$' + (v / 1000).toFixed(0) + 'k' }
                }
            }
        }
    });
}

// --- Category Chart (Doughnut) ---
async function loadCategoryChart() {
    const data = await fetchData('/sales/by-category');

    new Chart(document.getElementById('chart-category'), {
        type: 'doughnut',
        data: {
            labels: data.map(d => d.category),
            datasets: [{
                data: data.map(d => d.revenue),
                backgroundColor: PALETTE.slice(0, data.length),
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: { position: 'right' },
                tooltip: {
                    callbacks: {
                        label: ctx => `${ctx.label}: ${formatCurrency(ctx.raw)}`
                    }
                }
            }
        }
    });
}

// --- Channel Chart (Bar) ---
async function loadChannelChart() {
    const data = await fetchData('/sales/by-channel');

    new Chart(document.getElementById('chart-channel'), {
        type: 'bar',
        data: {
            labels: data.map(d => d.channel_name),
            datasets: [{
                label: 'Revenue',
                data: data.map(d => d.revenue),
                backgroundColor: PALETTE.slice(0, data.length),
                borderRadius: 6,
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { display: false } },
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: { callback: v => '$' + (v / 1000).toFixed(0) + 'k' }
                }
            }
        }
    });
}

// --- Country Chart (Horizontal Bar) ---
async function loadCountryChart() {
    const data = await fetchData('/sales/by-country');

    new Chart(document.getElementById('chart-country'), {
        type: 'bar',
        data: {
            labels: data.map(d => d.country),
            datasets: [{
                label: 'Revenue',
                data: data.map(d => d.revenue),
                backgroundColor: COLORS.blue,
                borderRadius: 6,
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            plugins: { legend: { display: false } },
            scales: {
                x: {
                    beginAtZero: true,
                    ticks: { callback: v => '$' + (v / 1000).toFixed(0) + 'k' }
                }
            }
        }
    });
}

// --- Customer Segments (Pie) ---
async function loadSegmentsChart() {
    const data = await fetchData('/customers/segments');
    const segmentColors = {
        'Platinum': COLORS.purple,
        'Gold': COLORS.orange,
        'Silver': COLORS.teal,
        'Bronze': COLORS.red,
    };

    new Chart(document.getElementById('chart-segments'), {
        type: 'pie',
        data: {
            labels: data.map(d => `${d.segment} (${d.customer_count})`),
            datasets: [{
                data: data.map(d => d.customer_count),
                backgroundColor: data.map(d => segmentColors[d.segment] || COLORS.blue),
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { position: 'right' } }
        }
    });
}

// --- Daily Pattern (Bar) ---
async function loadDailyChart() {
    const data = await fetchData('/sales/daily-pattern');

    new Chart(document.getElementById('chart-daily'), {
        type: 'bar',
        data: {
            labels: data.map(d => d.day_name),
            datasets: [
                {
                    label: 'Orders',
                    data: data.map(d => d.orders),
                    backgroundColor: COLORS.blue,
                    borderRadius: 6,
                    yAxisID: 'y',
                },
                {
                    label: 'Avg Order Value',
                    data: data.map(d => d.avg_order),
                    type: 'line',
                    borderColor: COLORS.orange,
                    backgroundColor: 'transparent',
                    yAxisID: 'y1',
                    tension: 0.3,
                }
            ]
        },
        options: {
            responsive: true,
            plugins: { legend: { position: 'top' } },
            scales: {
                y: { beginAtZero: true, position: 'left', title: { display: true, text: 'Orders' } },
                y1: { beginAtZero: true, position: 'right', grid: { drawOnChartArea: false },
                       title: { display: true, text: 'Avg Order ($)' } }
            }
        }
    });
}

// --- Engagement Chart (Grouped Bar) ---
async function loadEngagementChart() {
    const data = await fetchData('/activity/engagement');

    new Chart(document.getElementById('chart-engagement'), {
        type: 'bar',
        data: {
            labels: data.map(d => d.channel_name),
            datasets: [
                {
                    label: 'Avg Duration (sec)',
                    data: data.map(d => d.avg_duration_sec),
                    backgroundColor: COLORS.blue,
                    borderRadius: 6,
                },
                {
                    label: 'Avg Pages Viewed',
                    data: data.map(d => d.avg_pages * 50), // scaled for visibility
                    backgroundColor: COLORS.green,
                    borderRadius: 6,
                },
                {
                    label: 'Bounce Rate (%)',
                    data: data.map(d => d.bounce_rate_pct * 10), // scaled for visibility
                    backgroundColor: COLORS.red,
                    borderRadius: 6,
                }
            ]
        },
        options: {
            responsive: true,
            plugins: { legend: { position: 'top' } },
            scales: { y: { beginAtZero: true } }
        }
    });
}

// --- Top Products Table ---
async function loadProductsTable() {
    const data = await fetchData('/products/top');
    const tbody = document.querySelector('#table-products tbody');
    tbody.innerHTML = data.map((p, i) => `
        <tr>
            <td>${i + 1}</td>
            <td>${p.product_name}</td>
            <td>${p.category}</td>
            <td>${formatNumber(p.units_sold)}</td>
            <td>${formatCurrency(p.revenue)}</td>
            <td>${formatCurrency(p.profit)}</td>
        </tr>
    `).join('');
}

// --- Initialize Dashboard ---
async function init() {
    try {
        await Promise.all([
            loadOverview(),
            loadMonthlyChart(),
            loadCategoryChart(),
            loadChannelChart(),
            loadCountryChart(),
            loadSegmentsChart(),
            loadDailyChart(),
            loadEngagementChart(),
            loadProductsTable(),
        ]);
        console.log('Dashboard loaded successfully!');
    } catch (error) {
        console.error('Dashboard error:', error);
    }
}

document.addEventListener('DOMContentLoaded', init);
