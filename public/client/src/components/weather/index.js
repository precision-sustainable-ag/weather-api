import { set } from '../../store/store';
import { useDispatch } from 'react-redux';
import { Columns } from '../columns';
import { Notes } from '../notes';

export const Weather = () => {
  const path = window.location.origin.replace(/:300\d/, '');
  const dispatch = useDispatch();
  return (
    <div id="Weather">
      <h3>Weather Data</h3>
      <table>
        <tbody>
          <tr>
            <td>Hourly weather data</td>
            <td>{path}/hourly?<i>parameters</i></td>
          </tr>
          <tr>
            <td>5-year hourly weather averages</td>
            <td>{path}/averages?<i>parameters</i></td>
          </tr>
          <tr>
            <td>Daily weather statistics</td>
            <td>{path}/daily?<i>parameters</i></td>
          </tr>
        </tbody>
      </table>

      <p><i>parameters</i> may include one or more of:</p>
      <table>
        <thead>
          <tr>
            <th>Parameter</th>
            <th>Description</th>
            <th>Required</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>
              <p>lat=<i>latitude</i>[,<i>latitude</i>,&hellip;]&lon=<i>longitude</i>[,<i>longitude</i>&hellip;]</p>
              <p>location=<i>location</i></p>
            </td>
            <td>
              <p>You can enter multiple latitudes and longitudes.</p>
              <p>Either <b>lat-lon</b> <em>or</em> <b>location</b> is required.</p>
            </td>
            <td>
              required
            </td>
          </tr>

          <tr>
            <td>
              &start=[<i>year</i>-]<i>month-day</i>
              <br/>
              &end=[<i>year</i>-]<i>month-day</i>
            </td>
            <td>
              <strong>start</strong> defaults to 1-1 for hourly averages, and it defaults to the earliest available date for real hourly data.<br/>
              <strong>end</strong> defaults to 12-31 for hourly averages, and it defaults to the latest available date for real hourly data.<br/>
              <b>year</b> is optional for 5-year averages and will be ignored.  It's required otherwise.
            </td>
          </tr>

          <tr>
            <td>&attributes=<i>column names</i></td>
            <td>
              A comma-separated list of database columns (see below).  If not present, all columns are output.<br/>
              Attributes can be entered as <strong>Shorthand</strong> or <strong>Database column</strong>.
            </td>
          </tr>                                              

          <tr>
            <td>&output=<b>json</b>|csv|html</td>
            <td>Defaults to json.</td>
          </tr>

          <tr>
            <td>&order=<i>column1</i> [desc] [,<i>column2</i> [desc] &hellip;]</td>
            <td>
              Sorts the data by column, optionally in <strong>desc</strong>ending order.
            </td>
          </tr>                                              

          <tr>
            <td>&where=<i>condition</i></td>
            <td>A valid SQL expression, which limits the results of the query.</td>
          </tr>

          <tr>
            <td>&stats=<i>expression</i></td>
            <td>A valid SQL aggregate expression, which references the attributes.</td>
          </tr>

          <tr>
            <td>&options=rect</td>
            <td>
              Given two lat/lons, they will be treated as the corners of a rectangle [NE:SW or NW:SE].<br/>
              Given a location, its northeast and southwest will be treated as the corners of a rectangle.<br/>
              Data within the rectangle will be retrieved based on resolution.<br/>Currently outputs NLDAS precipitation, but will output MRMS in the future.
            </td>
          </tr>

          <tr>
            <td>&options=graph</td>
            <td>Show graphing options for HTML output.  (Does nothing for other output types.)</td>
          </tr>

          <tr>
            <td>&options=gmt|utc</td>
            <td>Output in Coordinated Universal Time instead of local time.</td>
          </tr>

          <tr>
            <td>&options=predicted</td>
            <td>Use real hourly data if available; otherwise, use 5-year average data.</td>
          </tr>

          <tr>
            <td>
              &gddbase=<i>temperature &deg;C</i>
              <br />
              [&gddmin=<i>temperature &deg;C</i>]
              <br />
              [&gddmax=<i>temperature &deg;C</i>]
            </td>
            <td>
              Output Growing Degree Days.
              <br />
              <code>gddmin</code> defaults to <code>gddbase</code>.
              <br />
              <code>gddmax</code> defaults to <code>999</code>.
            </td>
          </tr>
        </tbody>
      </table>

      <hr />
      <Columns />
      <hr />
      <Notes />
    </div>
  )
} // Weather
