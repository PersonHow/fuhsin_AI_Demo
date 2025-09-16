import './StatusIndicator.scss'

export default function StatusIndicator({ status }) {
  const getStatusColor = (isHealthy) => isHealthy ? '#4caf50' : '#f44336'
  const getStatusText = (isHealthy) => isHealthy ? '正常' : '離線'
  
  return (
    <div className="status-indicator">
      <div className="status-item">
        <span className="status-dot" style={{ background: getStatusColor(status.elasticsearch) }}></span>
        <span>Elasticsearch: {getStatusText(status.elasticsearch)}</span>
      </div>
      <div className="status-item">
        <span className="status-dot" style={{ background: getStatusColor(status.openai) }}></span>
        <span>OpenAI: {getStatusText(status.openai)}</span>
      </div>
      <div className="status-item">
        <span className="status-label">系統狀態: </span>
        <span className={`status-badge ${status.status}`}>{status.status}</span>
      </div>
    </div>
  )
}
