import './App.css';

import React from 'react';

import { useSelector, useDispatch } from 'react-redux';

import { get, set } from './store/store';
import { Weather } from './components/weather';
import { Frost } from './components/frost';
import { Examples } from './components/examples';
import { Database } from './components/database';

function App() {
  const Screen = () => (
    <div id="Screen">
      {screens[screen]()}
    </div>
  ) // Screen

  const dispatch = useDispatch();
  const screen = useSelector(get.screen);
  const screens = {
    Weather,
    Frost,
    Examples,
  }

  if (window.location.href.includes('debug')) {
    screens.Database = Database;
  }

  return (
    <>
      <nav 
        onClick={(e) => {
          if (e.target.tagName === 'BUTTON') {
            dispatch(set.screen(e.target.textContent));
          }
        }}
      >
        {
          Object.keys(screens).map(key =>
            <button key={key} className={screen === key ? 'selected' : ''}>
              {key}
            </button>
          )
        }
      </nav>
      <Screen />
    </>
  )
}

export default App;
