import './Button.scss'
import Texts from "../Texts"

export default function Button({color, content, clickEvent}){
    return(
        <>
        <button onClick={clickEvent}>
            <Texts textType="span" content={content}/>
        </button>
        </>
    )
}
