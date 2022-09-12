import './App.css';

import React from 'react';

import {MenuItem, Button} from '@mui/material';
import {useEffect, useState} from 'react';
import {useSelector, useDispatch} from 'react-redux';

import {get, set} from './store/store';
import {Usage} from './components/usage';
import {Columns} from './components/columns';
import {Notes} from './components/notes';
import {Examples} from './components/examples';

function App() {
  const dispatch = useDispatch();
  const screen = useSelector(get.screen);
  const screens = {
    Usage,
    Columns,
    Notes,
    Examples,
  }

  return (
    <>
      <nav onClick={(e) => dispatch(set.screen(e.target.textContent))}>
        {
          Object.keys(screens).map(key =>
            <button key={key} className={screen === key ? 'selected' : ''}>{key}</button>
          )
        }
      </nav>
      <div id="Screen">
        {screens[screen]()}
      </div>
    </>
  )
}

export default App;
