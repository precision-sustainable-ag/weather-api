import {set} from '../store/store';
import {useDispatch} from 'react-redux';

export const Examples = () => {
  const path = window.location.origin.replace(3000, 1010);
  const dispatch = useDispatch();

  const example = (desc, url) => {
    url = `${path}/${url}&output=html`;
    return (
      <li>
        <p>{desc}:</p>
        <p className="indent"><a target="_blank" href={url} rel="noreferrer"><span className="server"></span>{url}</a></p>
      </li>
    )
  } // example

  return (
    <>
      <h3>Examples</h3>
      <p>The following display HTML tables.  Change <strong>output</strong> for json or csv.</p>
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
        {example('Show November 2018 real hourly data for Beltsville MD\'s midpoint, ordered by descending air temperature', 'hourly?lat=39.032056&lon=-76.873972&start=2018-11-01&end=2018-11-30&output=html&order=air_temperature desc')}
        {example('Show November 1 2018 real hourly data for all of Clarke County GA.  Allow graphing', 'hourly?location=clarke%20county%20georgia&start=2018-11-01&end=2018-11-1&output=html&options=rect,graph')}
        {example('TODO: Show real hourly data Clarke County GA, <span className="date1"></span> through <span className="date2"></span>', 'hourly?location=clarke%20county%20georgia&start=<span className="date1b"></span>&end=<span className="date2b"></span>')}
        {example('TODO: Show hourly data Clarke County GA, <span className="date1"></span> through <span className="date2"></span>, substituting 5-year averages for missing data', 'hourly?location=clarke%20county%20georgia&start=<span className="date1b"></span>&end=<span className="date2b"></span>&output=html&options=predicted')}
      </ol>
    </>
  )
}