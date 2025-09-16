import './SearchModeSelector.scss'
import { RiCharacterRecognitionFill, RiCentosFill, RiStackshareLine } from "react-icons/ri";
export default function SearchModeSelector({ mode, onChange, disabled }) {
  const modes = [
    { value: 'keyword', label: '關鍵字搜尋', icon: <RiCharacterRecognitionFill /> },
    { value: 'vector', label: '向量搜尋', icon: <RiCentosFill /> },
    { value: 'hybrid', label: '混合搜尋', icon: <RiStackshareLine /> }
  ]

  return (
    <div className="search-mode-selector">
      <label>搜尋模式：</label>
      <div className="mode-buttons">
        {modes.map(({ value, label, icon }) => (
          <button
            key={value}
            className={`mode-button ${mode === value ? 'active' : ''}`}
            onClick={() => onChange(value)}
            disabled={disabled}
            title={label}
          >
            <span className="mode-icon">{icon}</span>
            <span className="mode-label">{label}</span>
          </button>
        ))}
      </div>
    </div>
  )
}
