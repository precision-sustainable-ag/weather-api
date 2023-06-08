import { example } from '../../store/store';

import './styles.scss';

export const Frost = () => {
  const path = window.location.origin.replace(/:300\d/, '');
  return (
    <div id="Frost">
      <h3>Frost data</h3>
      <p>
        Frost data is based on 30-year climate normals, downloaded from &nbsp;
        <a
          target="_blank"
          rel="noreferrer"
          href="https://www.ncei.noaa.gov/products/land-based-station/us-climate-normals"
        >
          https://www.ncei.noaa.gov/products/land-based-station/us-climate-normals
        </a>
      </p>
      <p>
        Outputs the weather station, the dates of first and last freeze, and the dates of first and last frost.
      </p>
      <table>
        <tbody>
          <tr>
            <td style={{ verticalAlign: 'middle' }}>30-year frost normals</td>
            <td>
              {path}/frost?lat=<i>latitude</i>&lon=<i>longitude</i>
              <br />
              or
              <br />
              {path}/frost?location=<i>location</i>
            </td>
          </tr>
          <tr>
            <td>[&output=<b>json</b>|csv|html]</td>
            <td>Defaults to json.</td>
          </tr>
        </tbody>
      </table>
    </div>
  )
} // Frost
