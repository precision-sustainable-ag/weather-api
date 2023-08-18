import './styles.scss';

export const Yearly = () => {
  const path = window.location.origin.replace(/:300\d/, '');
  return (
    <div id="Yearly">
      <h3>Yearly weather data</h3>
      <p>
        Yearly minimum and maximum air temperature and average precipitation is available from 2018 to the previous year.
      </p>
      <table>
        <tbody>
          <tr>
            <td style={{ verticalAlign: 'middle' }}>Get temperature and precipitation data for a single year</td>
            <td>
              {path}/yearly?lat=<i>latitude</i>&lon=<i>longitude</i>&year=<i>year</i>
              <br />
              or
              <br />
              {path}/yearly?location=<i>location</i>&year=<i>year</i>
            </td>
          </tr>
          <tr>
            <td style={{ verticalAlign: 'middle' }}>Get temperature and precipitation data for a range of years</td>
            <td>
              {path}/yearly?lat=<i>latitude</i>&lon=<i>longitude</i>&year=<i>year1</i>-<i>year2</i>
              <br />
              or
              <br />
              {path}/yearly?location=<i>location</i>&year=<i>year1</i>-<i>year2</i>
            </td>
          </tr>
          <tr>
            <td style={{ verticalAlign: 'middle' }}>Get historical temperature and precipitation data</td>
            <td>
              {path}/yearly?lat=<i>latitude</i>&lon=<i>longitude</i>
              <br />
              or
              <br />
              {path}/yearly?location=<i>location</i>
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
} // Yearly
