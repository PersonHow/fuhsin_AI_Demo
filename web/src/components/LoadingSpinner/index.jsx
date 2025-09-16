import './LoadingSpinner.scss'

export default function LoadingSpinner({ message = '載入中...' }) {
  return (
    <div className="loading-spinner">
      <div className="spinner">
        <div className="spinner-circle"></div>
      </div>
      <p>{message}</p>
    </div>
  )
}
