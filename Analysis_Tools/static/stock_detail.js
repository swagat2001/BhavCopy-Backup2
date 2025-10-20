// Enhanced Stock Detail Page JavaScript

const ticker = document.getElementById('symbolName').textContent;
let priceChart = null;
let expiryDatesLoaded = false;
let availableTradingDates = [];

// Initialize
document.addEventListener('DOMContentLoaded', function() {
    populateSymbolDropdown();
    loadAvailableDates();
});

async function loadAvailableDates() {
    try {
        const response = await fetch('/get_available_trading_dates');
        const data = await response.json();
        
        if (data.dates && data.dates.length > 0) {
            availableTradingDates = data.dates;
            setDefaultDate(data.dates[0]);
        } else {
            setDefaultDate();
        }
        
        loadExpiryDates();
    } catch (error) {
        console.error('Error loading trading dates:', error);
        setDefaultDate();
        loadExpiryDates();
    }
}

function setDefaultDate(defaultDate = null) {
    const dateInput = document.getElementById('historicalDate');
    const today = new Date().toISOString().split('T')[0];
    
    if (defaultDate) {
        dateInput.value = defaultDate;
    } else {
        dateInput.value = today;
    }
    
    dateInput.max = today;
}

async function loadExpiryDates() {
    try {
        const response = await fetch(`/get_expiry_dates?ticker=${ticker}`);
        const data = await response.json();
        
        const select = document.getElementById('expirySelect');
        select.innerHTML = '';
        
        if (!data.expiry_dates || data.expiry_dates.length === 0) {
            const opt = document.createElement('option');
            opt.value = 'all';
            opt.textContent = 'All Expiries';
            select.appendChild(opt);
        } else {
            data.expiry_dates.forEach((exp, idx) => {
                const opt = document.createElement('option');
                opt.value = exp;
                opt.textContent = exp;
                if (idx === 0) opt.selected = true;
                select.appendChild(opt);
            });
        }
        
        expiryDatesLoaded = true;
        loadStockData();
    } catch (error) {
        console.error('Error loading expiry dates:', error);
        const select = document.getElementById('expirySelect');
        select.innerHTML = '<option value="all">All Expiries</option>';
        expiryDatesLoaded = true;
        loadStockData();
    }
}

function populateSymbolDropdown() {
    fetch('/get_available_tickers')
        .then(r => r.json())
        .then(tickers => {
            const select = document.getElementById('symbolSelect');
            tickers.forEach(t => {
                const opt = document.createElement('option');
                opt.value = t;
                opt.textContent = t;
                opt.selected = (t === ticker);
                select.appendChild(opt);
            });
        })
        .catch(err => console.error('Error loading tickers:', err));
}

function changeSymbol() {
    const newSymbol = document.getElementById('symbolSelect').value;
    if (newSymbol) {
        window.location.href = `/stock/${newSymbol}`;
    }
}

async function loadStockData() {
    if (!expiryDatesLoaded) {
        return;
    }
    
    const expiry = document.getElementById('expirySelect').value;
    const historicalDate = document.getElementById('historicalDate').value;
    
    if (!historicalDate) {
        alert('Please select a date');
        return;
    }
    
    // Validate date
    const selectedDate = new Date(historicalDate);
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    
    if (selectedDate > today) {
        alert('Cannot select future dates');
        if (availableTradingDates.length > 0) {
            document.getElementById('historicalDate').value = availableTradingDates[0];
        }
        return;
    }
    
    document.getElementById('priceChartContainer').innerHTML = '<div class="loading-message">Loading chart...</div>';
    document.getElementById('optionChainTable').innerHTML = '<div class="loading-message">Loading option chain...</div>';
    
    try {
        let url = `/get_stock_data?ticker=${ticker}&mode=historical&date=${historicalDate}`;
        if (expiry && expiry !== 'all') url += `&expiry=${expiry}`;
        
        console.log('Fetching:', url);
        
        const response = await fetch(url);
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || `HTTP ${response.status}`);
        }
        
        const data = await response.json();
        
        console.log('Received data:', data);
        
        if (data.error) {
            alert(`Error: ${data.error}`);
            return;
        }
        
        // Update all sections
        updateExpiryTable(data.expiry_dates || [], historicalDate);
        updateGauges(data.stats, data.option_chain);
        updateStats(data.stats, data.last_updated, data.option_chain);
        updatePriceChart(data.price_data);
        updateOptionChain(data.option_chain);
    } catch (error) {
        console.error('Error loading data:', error);
        alert(`Error loading data: ${error.message}`);
    }
}

function updateExpiryTable(expiryDates, currentDate) {
    const tbody = document.querySelector('#expiryTable tbody');
    
    if (!expiryDates || expiryDates.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; padding: 20px; color: #999;">No expiry data available</td></tr>';
        return;
    }
    
    // Show expiry dates - actual data will come from backend enhancement
    let html = '';
    expiryDates.forEach(expiry => {
        html += `<tr>
            <td>${expiry}</td>
            <td>-</td>
            <td class="positive-change">-</td>
            <td>-</td>
            <td>-</td>
            <td class="positive-change">-</td>
        </tr>`;
    });
    
    tbody.innerHTML = html;
}

function updateGauges(stats, optionChain) {
    // Rollover Gauge (placeholder - needs calculation)
    document.getElementById('rolloverGauge').textContent = '-';
    
    // MWPL Gauge (Max Pain - needs calculation)
    document.getElementById('mwplGauge').textContent = '-';
    
    // IV Gauge (Average IV from option chain)
    if (optionChain && optionChain.length > 0) {
        const avgIV = 20; // Placeholder
        document.getElementById('ivGauge').textContent = avgIV.toFixed(2);
        document.getElementById('ivRange').textContent = '10 - 30';
    } else {
        document.getElementById('ivGauge').textContent = '-';
        document.getElementById('ivRange').textContent = '-';
    }
    
    // PCR Gauge
    if (stats && stats.pcr_oi) {
        document.getElementById('pcrGauge').textContent = stats.pcr_oi.toFixed(2);
        const pcrMin = Math.max(0, stats.pcr_oi - 0.3).toFixed(2);
        const pcrMax = (stats.pcr_oi + 0.3).toFixed(2);
        document.getElementById('pcrRange').textContent = `${pcrMin} - ${pcrMax}`;
    } else {
        document.getElementById('pcrGauge').textContent = '-';
        document.getElementById('pcrRange').textContent = '-';
    }
}

function updateStats(stats, lastUpdated, optionChain) {
    document.getElementById('lastUpdated').textContent = lastUpdated || '-';
    document.getElementById('totalCeOi').textContent = stats ? formatNumber(stats.total_ce_oi) : '-';
    document.getElementById('totalPeOi').textContent = stats ? formatNumber(stats.total_pe_oi) : '-';
    document.getElementById('totalCeOiChg').textContent = stats ? formatNumber(stats.total_ce_oi_chg) : '-';
    document.getElementById('totalPeOiChg').textContent = stats ? formatNumber(stats.total_pe_oi_chg) : '-';
    
    if (stats) {
        const diffOi = stats.total_pe_oi - stats.total_ce_oi;
        const diffOiChg = stats.total_pe_oi_chg - stats.total_ce_oi_chg;
        document.getElementById('diffPeCeOi').textContent = formatNumber(diffOi);
        document.getElementById('diffPeCeOiChg').textContent = formatNumber(diffOiChg);
        
        let trend = 'Neutral';
        let trendChg = 'Neutral';
        if (stats.pcr_oi > 1.2) trend = 'Bullish';
        else if (stats.pcr_oi < 0.8) trend = 'Bearish';
        document.getElementById('trendOi').textContent = trend;
        document.getElementById('trendOiChg').textContent = trendChg;
    } else {
        document.getElementById('diffPeCeOi').textContent = '-';
        document.getElementById('diffPeCeOiChg').textContent = '-';
        document.getElementById('trendOi').textContent = '-';
        document.getElementById('trendOiChg').textContent = '-';
    }
    
    // Calculate Max Strikes from option chain
    if (optionChain && optionChain.length > 0) {
        let maxCeOi = 0, maxCeOiStrike = 0;
        let maxPeOi = 0, maxPeOiStrike = 0;
        let maxCeOiChg = -Infinity, maxCeOiChgStrike = 0;
        let maxPeOiChg = -Infinity, maxPeOiChgStrike = 0;
        
        optionChain.forEach(row => {
            if (row.call_oi > maxCeOi) {
                maxCeOi = row.call_oi;
                maxCeOiStrike = row.strike;
            }
            if (row.put_oi > maxPeOi) {
                maxPeOi = row.put_oi;
                maxPeOiStrike = row.strike;
            }
            if (row.call_oi_chg > maxCeOiChg) {
                maxCeOiChg = row.call_oi_chg;
                maxCeOiChgStrike = row.strike;
            }
            if (row.put_oi_chg > maxPeOiChg) {
                maxPeOiChg = row.put_oi_chg;
                maxPeOiChgStrike = row.strike;
            }
        });
        
        document.getElementById('maxCeOiStrike').textContent = maxCeOiStrike;
        document.getElementById('maxPeOiStrike').textContent = maxPeOiStrike;
        document.getElementById('maxCeOiChgStrike').textContent = maxCeOiChgStrike;
        document.getElementById('maxPeOiChgStrike').textContent = maxPeOiChgStrike;
    } else {
        document.getElementById('maxCeOiStrike').textContent = '-';
        document.getElementById('maxPeOiStrike').textContent = '-';
        document.getElementById('maxCeOiChgStrike').textContent = '-';
        document.getElementById('maxPeOiChgStrike').textContent = '-';
    }
}

function updatePriceChart(priceData) {
    const container = document.getElementById('priceChartContainer');
    container.innerHTML = '';
    
    if (!priceData || priceData.length === 0) {
        container.innerHTML = '<div class="loading-message">No price data available</div>';
        return;
    }
    
    if (priceChart) {
        priceChart.remove();
        priceChart = null;
    }
    
    const oiValue = priceData[0]?.oi || 0;
    const ivValue = priceData[0]?.iv || 0;
    const pcrValue = priceData[0]?.pcr || 1.0;
    
    priceChart = LightweightCharts.createChart(container, {
        width: container.clientWidth,
        height: 480,
        layout: {
            background: { color: '#ffffff' },
            textColor: '#333',
        },
        grid: {
            vertLines: { color: '#f0f0f0' },
            horzLines: { color: '#f0f0f0' },
        },
        crosshair: {
            mode: LightweightCharts.CrosshairMode.Normal,
        },
        timeScale: {
            borderColor: '#d1d4dc',
            timeVisible: true,
            secondsVisible: false,
        },
        rightPriceScale: {
            borderColor: '#d1d4dc',
            visible: true,
        },
    });
    
    // Price Line Series
    const priceSeries = priceChart.addLineSeries({
        color: '#2962FF',
        lineWidth: 2,
        priceScaleId: 'right',
        title: 'Price',
    });
    priceSeries.setData(priceData.map(d => ({
        time: d.time,
        value: d.close
    })));
    
    // VWAP Line Series
    const vwapSeries = priceChart.addLineSeries({
        color: '#FF6D00',
        lineWidth: 2,
        priceScaleId: 'right',
        title: 'VWAP',
    });
    vwapSeries.setData(priceData.map(d => ({
        time: d.time,
        value: d.vwap || d.close
    })));
    
    // Volume Histogram
    const volumeSeries = priceChart.addHistogramSeries({
        color: '#26a69a',
        priceFormat: { type: 'volume' },
        priceScaleId: '',
        scaleMargins: {
            top: 0.7,
            bottom: 0,
        },
        title: 'Volume',
    });
    volumeSeries.setData(priceData.map(d => ({
        time: d.time,
        value: d.volume || 0,
        color: d.close >= d.open ? 'rgba(38, 166, 154, 0.5)' : 'rgba(239, 83, 80, 0.5)'
    })));
    
    // Legend
    const legendDiv = document.createElement('div');
    legendDiv.style.cssText = `
        position: absolute;
        top: 12px;
        left: 50%;
        transform: translateX(-50%);
        z-index: 10;
        font-size: 13px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        background: rgba(255, 255, 255, 0.95);
        padding: 8px 20px;
        border-radius: 4px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    `;
    
    legendDiv.innerHTML = `
        <div style="display: flex; gap: 25px; align-items: center;">
            <span style="display: flex; align-items: center; gap: 6px;">
                <span style="width: 20px; height: 2px; background: #2962FF; display: inline-block;"></span>
                <span style="font-weight: 500;">Price</span>
            </span>
            <span style="display: flex; align-items: center; gap: 6px;">
                <span style="width: 20px; height: 2px; background: #FF6D00; display: inline-block;"></span>
                <span style="font-weight: 500;">VWAP</span>
            </span>
            <span style="display: flex; align-items: center; gap: 6px;">
                <span style="width: 8px; height: 8px; background: #26a69a; border-radius: 50%; display: inline-block;"></span>
                <span style="font-weight: 500;">Volume</span>
            </span>
            <span style="font-size: 11px; color: #666; margin-left: 10px;">
                OI: ${formatNumber(oiValue)} | IV: ${ivValue.toFixed(2)}% | PCR: ${pcrValue.toFixed(2)}
            </span>
        </div>
    `;
    
    container.style.position = 'relative';
    container.appendChild(legendDiv);
    priceChart.timeScale().fitContent();
}

function updateOptionChain(optionChain) {
    const container = document.getElementById('optionChainTable');
    
    if (!optionChain || optionChain.length === 0) {
        container.innerHTML = '<div class="loading-message">No option chain data</div>';
        return;
    }
    
    let html = '<table class="option-chain-table"><thead><tr>';
    html += '<th colspan="4" class="call-header">CALL</th>';
    html += '<th rowspan="2" class="strike-column">Strike</th>';
    html += '<th colspan="4" class="put-header">PUT</th>';
    html += '</tr><tr>';
    html += '<th class="call-header">OI</th>';
    html += '<th class="call-header">OI Chg</th>';
    html += '<th class="call-header">Volume</th>';
    html += '<th class="call-header">Price</th>';
    html += '<th class="put-header">Price</th>';
    html += '<th class="put-header">Volume</th>';
    html += '<th class="put-header">OI Chg</th>';
    html += '<th class="put-header">OI</th>';
    html += '</tr></thead><tbody>';
    
    optionChain.forEach(row => {
        html += '<tr>';
        html += `<td>${formatNumber(row.call_oi)}</td>`;
        html += `<td class="${row.call_oi_chg >= 0 ? 'positive-oi' : 'negative-oi'}">${formatNumber(row.call_oi_chg)}</td>`;
        html += `<td>${formatNumber(row.call_volume)}</td>`;
        html += `<td>${row.call_price.toFixed(2)}</td>`;
        html += `<td class="strike-column">${row.strike}</td>`;
        html += `<td>${row.put_price.toFixed(2)}</td>`;
        html += `<td>${formatNumber(row.put_volume)}</td>`;
        html += `<td class="${row.put_oi_chg >= 0 ? 'positive-oi' : 'negative-oi'}">${formatNumber(row.put_oi_chg)}</td>`;
        html += `<td>${formatNumber(row.put_oi)}</td>`;
        html += '</tr>';
    });
    
    html += '</tbody></table>';
    container.innerHTML = html;
}

function formatNumber(val) {
    if (val === 0) return '0';
    const num = parseFloat(val);
    if (isNaN(num)) return val;
    if (Math.abs(num) >= 1e7) return (num/1e7).toFixed(2) + 'Cr';
    if (Math.abs(num) >= 1e6) return (num/1e6).toFixed(2) + 'M';
    if (Math.abs(num) >= 1e3) return (num/1e3).toFixed(2) + 'K';
    return num.toFixed(2);
}
