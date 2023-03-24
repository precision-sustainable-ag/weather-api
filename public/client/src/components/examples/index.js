import { example } from "../../store/store";

import './styles.scss';

export const Examples = () => {
  const format = date => date.getFullYear() + '-' + (date.getMonth() + 1) + '-' + date.getDate();
  const date1 = format(new Date(Date.now() - 12096e5));
  const date2 = format(new Date(Date.now() + 12096e5));

  return (
    <div id="Examples">
      <p>The examples below display HTML tables.  Change <strong>output</strong> for json or csv.</p>

      <ul>
        <li><a href="#weather">Weather examples</a></li>
        <li><a href="#frost">Frost examples</a></li>
      </ul>
      <hr />
      <section id="weather">
        <h3>Weather Examples</h3>
        <ol>
          {example('Show November 2018 real hourly data for Beltsville MD, all parameters', 'hourly?lat=39.032056&lon=-76.873972&start=2018-11-01&end=2018-11-30')}
          {example('Show Beltsville MD hourly March averages', 'averages?lat=39.032056&lon=-76.873972&start=3-1&end=3-31')}
          {example('Show Beltsville MD hourly March averages, all parameters', 'averages?lat=39.032056&lon=-76.873972&start=3-1&end=3-31')}
          {example('Show Beltsville MD hourly March temperature and humidity averages', 'averages?lat=39.032056&lon=-76.873972&start=3-1&end=3-31&attributes=air_temperature,humidity')}
          {example('Show yearly hourly averages for Beltsville MD, all parameters', 'averages?lat=39.032056&lon=-76.873972')}
          {example('Show November 2018 daily ranges and statistics for Beltsville MD, all parameters', 'daily?lat=39.032056&lon=-76.873972&start=2018-11-01&end=2018-11-30')}
          {example('Show November 2018 daily ranges and statistics for 2400 College Station Road Athens GA, all parameters', 'daily?location=2400 College Station Road Athens GA&start=2018-11-01&end=2018-11-30')}
          {example('Show November 2018 daily ranges and statistics for 2400 College Station Road Athens GA, temperature and specific humidity', 'daily?location=2400 College Station Road Athens GA&start=2018-11-01&end=2018-11-30&attr=air_temperature,humidity')}
          {example('Show November 2018 daily ranges and statistics for lat/lons [39.03, -76.87], [40.42, -80.35], [42.33, -84.26]', 'daily?lat=39.03,40.42,42.33&lon=-76.87,-80.35,-84.26&start=2018-11-01&end=2018-11-30&attr=lat,lon,air_temperature,humidity')}
          {example('Show November 2018 real hourly data for Beltsville MD\'s midpoint, ordered by descending air temperature', 'hourly?lat=39.032056&lon=-76.873972&start=2018-11-01&end=2018-11-30&order=air_temperature desc')}
          {example('Show November 1 2018 real hourly data for all of Clarke County GA.  Allow graphing', 'hourly?location=clarke%20county%20georgia&start=2018-11-01&end=2018-11-1&options=rect,graph')}
          {example(`Show real hourly data Clarke County GA, ${date1} through ${date2}`, `hourly?location=clarke%20county%20georgia&start=${date1}&end=${date2}`)}
          {example(`Show hourly data Clarke County GA, ${date1} through ${date2}, substituting 5-year averages for missing data`, `hourly?location=clarke%20county%20georgia&start=${date1}&end=${date2}&options=predicted`)}
          {example(`Growing Degree Days`, `daily?lat=39.032056&lon=-76.873972&start=2018-11-01&end=2018-11-30&attr=lat,lon,gdd&gddbase=10&gddmin=10&gddmax=30`)}
          {example(`Cumulative Growing Degree Days`, `daily?lat=39.032056&lon=-76.873972&start=2018-11-01&end=2018-11-30&gddbase=10&gddmin=10&gddmax=30&stats=sum(gdd)`)}
        </ol>
      </section>

      <section id="frost">
        <h3>Frost Examples</h3>
        <ol>
          {example('Show frost data for Athens Georgia', 'frost?location=Athens Georgia')}
          {example('Show frost data for latitude 39.032056 and longitude -76.873972', 'frost?lat=39.032056&lon=-76.873972')}
        </ol>
      </section>
    </div>
  )
}