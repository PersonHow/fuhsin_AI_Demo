import {useState} from 'react'
import './App.scss'
import Button from './components/Button'
import Texts  from './components/Texts'

export default function App(){
  
  return(
  <>
  <div className='homeBg'>
    <div className="title">
      <Texts textType={"h1"} content={"Fushin_AI_Search_Demo"}/>
    </div>
    <Button content={"Count"}/>
  </div>
  </>
  )
}
