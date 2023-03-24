import {set, get} from '../../store/store';
import {useDispatch, useSelector} from 'react-redux';
import {useRef} from 'react';

const commas = (n) => {
  if (!n) return '-';
  return n.toString().replace(/(\d)(?=(\d{3})+(?!\d))/g, "$1,");
}

let fetched;

export const Database = () => {
  const dispatch = useDispatch();
  const path = window.location.origin.replace(/300\d/, 1010);
  const database = useSelector(get.database);
  const data = useSelector(get.data);
  const nrows = 20;
  const scroller = useRef();

  scroller.current?.focus();  

  if (!fetched) {
    fetched = true;

    fetch(`${path}/counttablesrows`)
      .then(response => response.json())
      .then(data => {
        dispatch(set.database.ntables(data[0].tables));
        dispatch(set.database.rows(data[0].rows));
      });

    fetch(`${path}/countindexes`)
      .then(response => response.json())
      .then(data => {
        dispatch(set.database.nindexes(data[0].indexes));
      });

    fetch(`${path}/databasesize`)
      .then(response => response.json())
      .then(data => {
        const [size] = data[0].size.split(' ');
        dispatch(set.database.size(size / 1000));
      });

    fetch(`${path}/addresses`)
      .then(response => response.json())
      .then(data => {
        dispatch(set.database.addresses(data));
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

          <tr>
            <td>Size</td>
            <td>{database.size} TB</td>
            <td></td>
          </tr>
 
          <tr>
            <td>Rows</td>
            <td>{commas(database.rows)}</td>
            <td>(estimate)</td>
          </tr>
           
          <tr
            className={database.selected === 'Tables' ? 'selected' : ''}
          >
            <td
              onClick={() => {
                scroller.current?.scrollTo(0, 0);

                dispatch(set.database.selected('Tables'));
               
                if (!database.tables.length) {
                  dispatch(set.data([{}]));
                  fetch(`${path}/tables`)
                    .then(response => response.json())
                    .then(data => {
                      dispatch(set.database.tables(data));
                      dispatch(set.data(data));
                    });
                } else {
                  dispatch(set.data(database.tables));
                }
              }}
            >
              <u>Tables</u>
            </td>
            <td>{commas(database.ntables)}</td>
            <td>
              {database.selected === 'Tables' && !database.tables.length && 'Loading ...'}
            </td>
          </tr>
          
          <tr
            className={database.selected === 'Indexes' ? 'selected' : ''}
          >
            <td
              onClick={() => {
                scroller.current?.scrollTo(0, 0);

                dispatch(set.database.selected('Indexes'));
               
                if (!database.indexes.length) {
                  dispatch(set.data([{}]));
                  fetch(`${path}/indexes`)
                    .then(response => response.json())
                    .then(data => {
                      dispatch(set.database.indexes(data));
                      dispatch(set.data(data));
                    });
                } else {
                  dispatch(set.data(database.indexes));
                }
              }}
            >
              <u>Indexes</u>
            </td>
            <td>{commas(database.nindexes)}</td>
            <td>
              {database.selected === 'Indexes' && !database.indexes.length && 'Loading ...'}
            </td>
          </tr>
          
          <tr
            className={database.selected === 'Addresses' ? 'selected' : ''}
          >
            <td
              onClick={() => {
                scroller.current?.scrollTo(0, 0);

                dispatch(set.database.selected('Addresses'));
                dispatch(set.data(database.addresses));
              }}
            >
              <u>Address lookups</u>
            </td>
            <td>{commas(database.addresses.length)}</td>
            <td></td>
          </tr>
        </tbody>
      </table>

      <div
        className="scroller"
        ref={scroller}
        onScroll={(e) => {
          const top = e.currentTarget.scrollTop;
          dispatch(set.database.start(top / 24));
        }}
        tabIndex="1"
      >
        <div style={{height: data.length * 24}}>
          <table>
            <thead>
              <tr>
                {Object.keys(data[0]).map((key, i) => <th style={{width: database.widths[i] * 0.7 + 'em'}}>{key}</th>)}
              </tr>
            </thead>
            <tbody>
              {data.slice(database.start, database.start + nrows).map(row => <tr>{Object.values(row).map(data => <td>{data}</td>)}</tr>)}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )  
}