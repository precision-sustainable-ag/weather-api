import {set, get} from '../store/store';
import {useDispatch, useSelector} from 'react-redux';
import {useRef} from 'react';

const commas = (n) => {
  if (!n) return '-';
  return n.toString().replace(/(\d)(?=(\d{3})+(?!\d))/g, "$1,");
}

let fetched;

export const Database = () => {
  const dispatch = useDispatch();
  const path = window.location.origin.replace(3000, 1010);
  const ntables = useSelector(get.ntables);
  const tables = useSelector(get.tables);
  const rows = useSelector(get.rows);
  const nindexes = useSelector(get.nindexes);
  const indexes = useSelector(get.indexes);
  const size = useSelector(get.size);
  const addresses = useSelector(get.addresses);
  const selected = useSelector(get.selected);
  const data = useSelector(get.data);
  const start = useSelector(get.start);
  const nrows = 15;
  const widths = useSelector(get.widths);
  const scroller = useRef();

  if (!fetched) {
    fetched = true;

    fetch(`${path}/counttablesrows`)
      .then(response => response.json())
      .then(data => {
        dispatch(set.ntables(data[0].tables));
        dispatch(set.rows(data[0].rows));
      });

    fetch(`${path}/countindexes`)
      .then(response => response.json())
      .then(data => {
        dispatch(set.nindexes(data[0].indexes));
      });

    fetch(`${path}/databasesize`)
      .then(response => response.json())
      .then(data => {
        const [size, unit] = data[0].size.split(' ');
        dispatch(set.size(size / 1000));
      });

    fetch(`${path}/addresses`)
      .then(response => response.json())
      .then(data => {
        dispatch(set.addresses(data));
      });
  }

  return (
    <div id="Database">
      <h3>Database</h3>
      <table>
        <tbody>
          <tr>
            <td>Schema</td>
            <td>weather</td>
            <td></td>
          </tr>
          
          <tr
            className={selected === 'Tables' ? 'selected' : ''}
          >
            <td
              onClick={() => {
                scroller.current?.scrollTo(0, 0);

                dispatch(set.selected('Tables'));
               
                if (!tables.length) {
                  dispatch(set.data([{}]));
                  fetch(`${path}/tables`)
                    .then(response => response.json())
                    .then(data => {
                      dispatch(set.tables(data));
                      dispatch(set.data(data));
                    });
                } else {
                  dispatch(set.data(tables));
                }
              }}
            >
              <u>Tables</u>
            </td>
            <td>{commas(ntables)}</td>
            <td>
              {selected === 'Tables' && !tables.length && 'Loading ...'}
            </td>
          </tr>
          
          <tr>
            <td>Rows</td>
            <td>{commas(rows)}</td>
            <td>(estimate)</td>
          </tr>
          
          <tr
            className={selected === 'Indexes' ? 'selected' : ''}
          >
            <td
              onClick={() => {
                scroller.current?.scrollTo(0, 0);

                dispatch(set.selected('Indexes'));
               
                if (!indexes.length) {
                  dispatch(set.data([{}]));
                  fetch(`${path}/indexes`)
                    .then(response => response.json())
                    .then(data => {
                      dispatch(set.indexes(data));
                      dispatch(set.data(data));
                    });
                } else {
                  dispatch(set.data(indexes));
                }
              }}
            >
              <u>Indexes</u>
            </td>
            <td>{commas(nindexes)}</td>
            <td>
              {selected === 'Indexes' && !indexes.length && 'Loading ...'}
            </td>
          </tr>
          
          <tr>
            <td>Size</td>
            <td>{size} TB</td>
            <td></td>
          </tr>
          
          <tr
            className={selected === 'Addresses' ? 'selected' : ''}
          >
            <td
              onClick={() => {
                scroller.current?.scrollTo(0, 0);

                dispatch(set.selected('Addresses'));
                dispatch(set.data(addresses));
              }}
            >
              <u>Address lookups</u>
            </td>
            <td>{commas(addresses.length)}</td>
            <td></td>
          </tr>
        </tbody>
      </table>

      <div
        className="scroller"
        ref={scroller}
        onScroll={(e) => {
          const top = e.currentTarget.scrollTop;
          dispatch(set.start(top / 32));
        }}
        tabIndex="1"
      >
        <div style={{height: data.length * 32}}>
          <table>
            <thead>
              <tr>
                {Object.keys(data[0]).map((key, i) => <th style={{width: widths[i] * 9}}>{key}</th>)}
              </tr>
            </thead>
            <tbody>
              {data.slice(start, start + nrows).map(row => <tr>{Object.values(row).map(data => <td>{data}</td>)}</tr>)}
            </tbody>
          </table>
        </div>
      </div>      
    </div>
  )  
}