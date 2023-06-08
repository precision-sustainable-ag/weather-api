import './styles.scss';

export const Temperature = () => {
  const path = window.location.origin.replace(/:300\d/, '');
  return (
    <div id="Temperature">
      <h3>Temperature data</h3>
      <p>
        Yearly minimum and maximum air temperature is available between 2018 and the previous year.
      </p>
      <table>
        <tbody>
          <tr>
            <td style={{ verticalAlign: 'middle' }}>Get min/max temperature for a single year</td>
            <td>
              {path}/yearlytemperature?lat=<i>latitude</i>&lon=<i>longitude</i>&year=<i>year</i>
              <br />
              or
              <br />
              {path}/yearlytemperature?location=<i>location</i>&year=<i>year</i>
            </td>
          </tr>
          <tr>
            <td style={{ verticalAlign: 'middle' }}>Get min/max temperature for a range of years</td>
            <td>
              {path}/yearlytemperature?lat=<i>latitude</i>&lon=<i>longitude</i>&year=<i>year1</i>-<i>year2</i>
              <br />
              or
              <br />
              {path}/yearlytemperature?location=<i>location</i>&year=<i>year1</i>-<i>year2</i>
            </td>
          </tr>
          <tr>
            <td style={{ verticalAlign: 'middle' }}>Get historical min/max temperature</td>
            <td>
              {path}/yearlytemperature?lat=<i>latitude</i>&lon=<i>longitude</i>
              <br />
              or
              <br />
              {path}/yearlytemperature?location=<i>location</i>
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
} // Temperature
