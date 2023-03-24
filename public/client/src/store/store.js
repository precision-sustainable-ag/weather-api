import React from 'react';
import {createStore, set, get} from './redux-autosetters';

let initialState = {
  screen: 'Weather',
  database: {
    ntables: 0,
    tables: [],
    rows: 0,
    size: 0,
    addresses: [],
    nindexes: 0,
    indexes: [],
    widths: [],
    data: [{}],
    start: 0,
    selected: '',
  },
  data: [{}],
};

const afterChange = {
  data: (state) => {
    const widths = [];
    state.data.forEach(row => {
      Object.values(row).forEach((value, i) => {
        widths[i] = Math.max(widths[i] || 0, (value || '').toString().length);
      });
    });
    state.database.widths = widths;
    state.database.start = 0;
  }
}

export const test = (key, result) => {
  let value = get[key]?.(store.getState())?.toString();
  if (value !== result.toString()) {
    value = store.getState();

    for (const k of key.split('.')) {
      value = value[k];
    }

    if (value?.toString() !== result.toString()) {
      // I'd prefer console.error, but that requires showing all react_devtools_backend.js
      console.info(`${key} should be ${result} instead of ${value}`);
      console.info(get[key]?.(store.getState()));
    }
  }
} // test

export const getDefaults = (parms) => {
  const def = {};
  if (!Array.isArray(parms)) {
    parms = parms.split('|');
  }

  parms.forEach(parm => {
    let s = initialState;
    for (const k of parm.split('.')) {
      s = s[k];
    }
    def[parm] = s;
  });
  // console.log(def);
  return def;
} // getDefaults

export const clearInputs = (defaults) => {
  for (const key in defaults) {
    try {
      let s = set;
      for (const k of key.split('.')) {
        s = s[k];
      }
      // console.log(key, typeof defaults[key], defaults[key]);
      store.dispatch(s(defaults[key]));
    } catch(error) {
      console.log(key, error);
    }
  }
} // clearInputs

export const store = createStore(initialState, {afterChange});

export {set, get} from './redux-autosetters';

export const example = (desc, url) => {
  const path = window.location.origin.replace(/:300\d/, '');
  url = `${path}/${url}&output=html`;
  return (
    <li>
      <p>{desc}:</p>
      <p className="indent"><a target="_blank" href={url} rel="noreferrer"><span className="server"></span>{url}</a></p>
    </li>
  )
} // example
