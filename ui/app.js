/**
 * SOTA Web Crawler - Frontend Application
 * Real-time crawl monitoring with WebSocket
 */

class CrawlerApp {
    constructor() {
        this.ws = null;
        this.currentCrawlId = null;
        this.maxPages = 50;

        this.initElements();
        this.initEventListeners();
        this.connectWebSocket();
        this.loadHistory();
    }

    initElements() {
        // Connection status
        this.connectionStatus = document.getElementById('connectionStatus');

        // Form elements
        this.crawlForm = document.getElementById('crawlForm');
        this.urlInput = document.getElementById('urlInput');
        this.maxPagesInput = document.getElementById('maxPages');
        this.maxDepthInput = document.getElementById('maxDepth');
        this.delayInput = document.getElementById('delay');
        this.concurrentInput = document.getElementById('concurrent');
        this.respectRobotsCheck = document.getElementById('respectRobots');
        this.renderCheck = document.getElementById('enableRender');
        this.startBtn = document.getElementById('startBtn');

        // Active crawl section
        this.activeCrawlSection = document.getElementById('activeCrawlSection');
        this.crawlUrl = document.getElementById('crawlUrl');
        this.stopBtn = document.getElementById('stopBtn');

        // Stats
        this.statCrawled = document.getElementById('statCrawled');
        this.statFailed = document.getElementById('statFailed');
        this.statQueue = document.getElementById('statQueue');
        this.statSeen = document.getElementById('statSeen');

        // Progress
        this.progressFill = document.getElementById('progressFill');
        this.progressText = document.getElementById('progressText');

        // History
        this.historyBody = document.getElementById('historyBody');
        this.refreshHistoryBtn = document.getElementById('refreshHistory');

        // Modal
        this.dataModal = document.getElementById('dataModal');
        this.modalBody = document.getElementById('modalBody');
        this.closeModalBtn = document.getElementById('closeModal');
    }

    initEventListeners() {
        // Form submission
        this.crawlForm.addEventListener('submit', (e) => {
            e.preventDefault();
            this.startCrawl();
        });

        // Stop button
        this.stopBtn.addEventListener('click', () => this.stopCrawl());

        // Refresh history
        this.refreshHistoryBtn.addEventListener('click', () => this.loadHistory());

        // Close modal
        this.closeModalBtn.addEventListener('click', () => this.closeModal());
        this.dataModal.addEventListener('click', (e) => {
            if (e.target === this.dataModal) this.closeModal();
        });

        // Max pages update
        this.maxPagesInput.addEventListener('change', () => {
            this.maxPages = parseInt(this.maxPagesInput.value) || 50;
        });
    }

    connectWebSocket() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws`;

        try {
            this.ws = new WebSocket(wsUrl);

            this.ws.onopen = () => {
                this.updateConnectionStatus('connected');
            };

            this.ws.onclose = () => {
                this.updateConnectionStatus('disconnected');
                // Reconnect after 3 seconds
                setTimeout(() => this.connectWebSocket(), 3000);
            };

            this.ws.onerror = () => {
                this.updateConnectionStatus('disconnected');
            };

            this.ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                this.handleWebSocketMessage(data);
            };
        } catch (error) {
            console.error('WebSocket connection failed:', error);
            this.updateConnectionStatus('disconnected');
        }
    }

    updateConnectionStatus(status) {
        this.connectionStatus.className = `connection-status ${status}`;
        const statusText = this.connectionStatus.querySelector('.status-text');

        switch (status) {
            case 'connected':
                statusText.textContent = 'Connected';
                break;
            case 'disconnected':
                statusText.textContent = 'Disconnected';
                break;
            default:
                statusText.textContent = 'Connecting...';
        }
    }

    handleWebSocketMessage(data) {
        switch (data.type) {
            case 'progress':
                this.updateProgress(data.stats);
                break;
            case 'crawl_started':
                this.onCrawlStarted(data);
                break;
            case 'crawl_completed':
                this.onCrawlCompleted(data);
                break;
            case 'crawl_error':
                this.onCrawlError(data);
                break;
        }
    }

    async startCrawl() {
        const url = this.urlInput.value.trim();
        if (!url) return;

        this.maxPages = parseInt(this.maxPagesInput.value) || 50;

        const payload = {
            url: url,
            max_pages: this.maxPages,
            max_depth: parseInt(this.maxDepthInput.value) || 10,
            delay: parseFloat(this.delayInput.value) || 1.0,
            concurrent: parseInt(this.concurrentInput.value) || 5,
            respect_robots: this.respectRobotsCheck.checked,
            render: this.renderCheck.checked
        };

        this.startBtn.disabled = true;
        this.startBtn.innerHTML = '<span class="btn-icon">⏳</span> Starting...';

        try {
            const response = await fetch('/api/crawl', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });

            const result = await response.json();

            if (response.ok) {
                this.currentCrawlId = result.crawl_id;
                this.showActiveCrawl(url);
            } else {
                alert('Failed to start crawl: ' + (result.detail || 'Unknown error'));
                this.resetStartButton();
            }
        } catch (error) {
            console.error('Error starting crawl:', error);
            alert('Failed to start crawl. Is the server running?');
            this.resetStartButton();
        }
    }

    async stopCrawl() {
        if (!this.currentCrawlId) return;

        try {
            await fetch(`/api/crawl/${this.currentCrawlId}`, {
                method: 'DELETE'
            });
        } catch (error) {
            console.error('Error stopping crawl:', error);
        }
    }

    showActiveCrawl(url) {
        this.activeCrawlSection.style.display = 'block';
        this.crawlUrl.textContent = url;
        this.resetStats();
    }

    hideActiveCrawl() {
        this.activeCrawlSection.style.display = 'none';
        this.currentCrawlId = null;
        this.resetStartButton();
    }

    resetStartButton() {
        this.startBtn.disabled = false;
        this.startBtn.innerHTML = '<span class="btn-icon">🚀</span> Start Crawling';
    }

    resetStats() {
        this.statCrawled.textContent = '0';
        this.statFailed.textContent = '0';
        this.statQueue.textContent = '0';
        this.statSeen.textContent = '0';
        this.progressFill.style.width = '0%';
        this.progressText.textContent = '0%';
    }

    updateProgress(stats) {
        this.statCrawled.textContent = stats.pages_crawled || 0;
        this.statFailed.textContent = stats.pages_failed || 0;
        this.statQueue.textContent = stats.queue_size || 0;
        this.statSeen.textContent = stats.urls_seen || 0;

        // Calculate progress
        const total = stats.pages_crawled + stats.pages_failed + stats.pages_skipped;
        const percent = Math.min(100, Math.round((total / this.maxPages) * 100));

        this.progressFill.style.width = `${percent}%`;
        this.progressText.textContent = `${percent}%`;
    }

    onCrawlStarted(data) {
        console.log('Crawl started:', data);
    }

    onCrawlCompleted(data) {
        console.log('Crawl completed:', data);
        this.hideActiveCrawl();
        this.loadHistory();

        // Show completion notification
        const stats = data.stats;
        alert(`Crawl completed!\n\nPages crawled: ${stats.pages_crawled}\nPages failed: ${stats.pages_failed}`);
    }

    onCrawlError(data) {
        console.error('Crawl error:', data);
        this.hideActiveCrawl();
        alert('Crawl failed: ' + data.error);
    }

    async loadHistory() {
        try {
            const response = await fetch('/api/history');
            const data = await response.json();

            this.renderHistory(data.sessions);
        } catch (error) {
            console.error('Error loading history:', error);
            this.historyBody.innerHTML = '<tr><td colspan="6" class="empty-state">Failed to load history</td></tr>';
        }
    }

    renderHistory(sessions) {
        if (!sessions || sessions.length === 0) {
            this.historyBody.innerHTML = '<tr><td colspan="6" class="empty-state">No crawl history yet</td></tr>';
            return;
        }

        this.historyBody.innerHTML = sessions.map(session => `
            <tr>
                <td>#${session.id}</td>
                <td class="url-cell" title="${session.seed_url}">${session.seed_url}</td>
                <td><span class="status-badge ${session.status}">${session.status}</span></td>
                <td>${session.pages_crawled || 0}</td>
                <td>${this.formatDate(session.started_at)}</td>
                <td>
                    <button class="btn btn-secondary btn-sm" onclick="app.viewData(${session.id})">
                        View
                    </button>
                </td>
            </tr>
        `).join('');
    }

    formatDate(dateStr) {
        if (!dateStr) return 'N/A';
        const date = new Date(dateStr);
        return date.toLocaleDateString() + ' ' + date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    }

    async viewData(sessionId) {
        try {
            const response = await fetch(`/api/data/${sessionId}?limit=50`);
            const data = await response.json();

            this.showDataModal(data.records);
        } catch (error) {
            console.error('Error loading data:', error);
            alert('Failed to load crawl data');
        }
    }

    showDataModal(records) {
        if (!records || records.length === 0) {
            this.modalBody.innerHTML = '<p class="empty-state">No data available</p>';
        } else {
            this.modalBody.innerHTML = records.map(record => `
                <div class="data-card">
                    <div class="data-card-title">${this.escapeHtml(record.title || 'No Title')}</div>
                    <div class="data-card-url">${this.escapeHtml(record.url)}</div>
                    <div class="data-card-text">${this.escapeHtml(record.description || record.text?.substring(0, 300) || 'No content')}</div>
                </div>
            `).join('');
        }

        this.dataModal.classList.add('active');
    }

    closeModal() {
        this.dataModal.classList.remove('active');
    }

    escapeHtml(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}

// Initialize app
const app = new CrawlerApp();
