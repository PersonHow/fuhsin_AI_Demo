
import './Texts.scss'

export default function Texts({ textType, content, idFor }) {

    function elementControl(type, label, labelFor) {
        switch (type) {
            case "h1":
                return <h1>{label}</h1>

            case "h2":
                return <h2>{label}</h2>

            case "h3":
                return <h3>{label}</h3>

            case "p":
                return <p>{label}</p>

            case "span":
                return <span>{label}</span>

            case "bigTitle":
                return <p className='bigTitle'>{label}</p>

            case "content":
                return <p className='content'>{label}</p>

            case "label":
                return <label className='content' htmlFor={labelFor}>{label}</label>    


        }
    }
    return (
        <>
            {elementControl(textType, content, idFor)}
        </>
    )
}
