export const Columns = () => {
  return (
    <>
     <h3>Columns</h3>
      <table id="Columns">
        <thead>
          <tr>
            <th colSpan="7">General Information</th><th className="hidden"></th><th className="hidden"></th><th className="hidden"></th><th className="hidden"></th><th className="hidden"></th><th className="hidden"></th>
            <th colSpan="5">Temporal Resolution</th><th className="hidden"></th><th className="hidden"></th><th className="hidden"></th><th className="hidden"></th>
            <th colSpan="2">Hourly Data Availability</th><th className="hidden"></th>
            <th rowSpan="2">Additional Comments</th>
          </tr>
          <tr>
            <th>Parameter</th>
            <th>Level, Layer</th>
            <th>Database Column</th>
            <th>Shorthand</th>
            <th>Unit</th>
            <th>Data Source</th>
            <th>Spatial Resolution</th>
            <th>Hourly</th>
            <th>Daily Average, Min, Max</th>
            <th>Daily Total</th>
            <th>5-Year Hourly Average</th>
            <th>Prediction</th>
            <th>Date Begin</th>
            <th>Date End</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>Air temperature</td>
            <td>2m above ground</td>
            <td>air_temperature</td>
            <td>TMP</td>
            <td>[C]</td>
            <td><a target="_blank" rel="noreferrer" href="https://ldas.gsfc.nasa.gov/nldas/v2/forcing">NLDAS</a></td>
            <td>0.125° x 0.125°</td>
            <td className="checked"></td>
            <td className="checked"></td>
            <td></td>
            <td className="checked"></td>
            <td>TBD</td>
            <td>01/01/2015</td>
            <td>Present</td>
            <td>-</td>
          </tr>
          <tr>
            <td>Specific humidity</td>
            <td>2m above ground</td>
            <td>humidity</td>
            <td>SPFH</td>
            <td>[kg kg-1]</td>
            <td><a target="_blank" rel="noreferrer" href="https://ldas.gsfc.nasa.gov/nldas/v2/forcing">NLDAS</a></td>
            <td>0.125° x 0.125°</td>
            <td className="checked"></td>
            <td className="checked"></td>
            <td></td>
            <td className="checked"></td>
            <td>TBD</td>
            <td>01/01/2015</td>
            <td>Present</td>
            <td>-</td>
          </tr>
          <tr>
            <td>Relative humidity</td>
            <td>2m above ground</td>
            <td>relative_humidity</td>
            <td></td>
            <td>fraction</td>
            <td><a target="_blank" rel="noreferrer" href="https://ldas.gsfc.nasa.gov/nldas/v2/forcing">NLDAS</a></td>
            <td>0.125° x 0.125°</td>
            <td className="checked"></td>
            <td className="checked"></td>
            <td></td>
            <td className="checked"></td>
            <td>TBD</td>
            <td>01/01/2015</td>
            <td>Present</td>
            <td>-</td>
          </tr>
          <tr>
            <td>Atmospheric pressure</td>
            <td>Surface</td>
            <td>pressure</td>
            <td>PRES</td>
            <td>[Pa]</td>
            <td><a target="_blank" rel="noreferrer" href="https://ldas.gsfc.nasa.gov/nldas/v2/forcing">NLDAS</a></td>
            <td>0.125° x 0.125°</td>
            <td className="checked"></td>
            <td className="checked"></td>
            <td></td>
            <td className="checked"></td>
            <td>TBD</td>
            <td>01/01/2015</td>
            <td>Present</td>
            <td>-</td>
          </tr>
          <tr>
            <td>Zonal wind speed</td>
            <td>10m above ground</td>
            <td>zonal_wind_speed</td>
            <td>UGRD</td>
            <td>[m s-1]</td>
            <td><a target="_blank" rel="noreferrer" href="https://ldas.gsfc.nasa.gov/nldas/v2/forcing">NLDAS</a></td>
            <td>0.125° x 0.125°</td>
            <td className="checked"></td>
            <td className="checked"></td>
            <td></td>
            <td className="checked"></td>
            <td>TBD</td>
            <td>01/01/2015</td>
            <td>Present</td>
            <td>-</td>
          </tr>
          <tr>
            <td>Meridional wind speed</td>
            <td>10m above ground</td>
            <td>meridional_wind_speed</td>
            <td>VGRD</td>
            <td>[m s-1]</td>
            <td><a target="_blank" rel="noreferrer" href="https://ldas.gsfc.nasa.gov/nldas/v2/forcing">NLDAS</a></td>
            <td>0.125° x 0.125°</td>
            <td className="checked"></td>
            <td className="checked"></td>
            <td></td>
            <td className="checked"></td>
            <td>TBD</td>
            <td>01/01/2015</td>
            <td>Present</td>
            <td>-</td>
          </tr>
          <tr>
            <td>Wind speed</td>
            <td>10m above ground</td>
            <td>wind_speed</td>
            <td></td>
            <td>[m s-1]</td>
            <td><a target="_blank" rel="noreferrer" href="https://ldas.gsfc.nasa.gov/nldas/v2/forcing">NLDAS</a></td>
            <td>0.125° x 0.125°</td>
            <td className="checked"></td>
            <td className="checked"></td>
            <td></td>
            <td className="checked"></td>
            <td>TBD</td>
            <td>01/01/2015</td>
            <td>Present</td>
            <td>-</td>
          </tr>
          <tr>
            <td>Downward short-wave radiation flux</td>
            <td>Surface</td>
            <td>shortwave_radiation</td>
            <td>DSWRF</td>
            <td>[W m-2]</td>
            <td><a target="_blank" rel="noreferrer" href="https://ldas.gsfc.nasa.gov/nldas/v2/forcing">NLDAS</a></td>
            <td>0.125° x 0.125°</td>
            <td className="checked"></td>
            <td></td>
            <td className="checked"></td>
            <td className="checked"></td>
            <td>TBD</td>
            <td>01/01/2015</td>
            <td>Present</td>
            <td>-</td>
          </tr>
          <tr>
            <td>Downward long-wave radiation flux</td>
            <td>Surface</td>
            <td>longwave_radiation</td>
            <td>DLWRF</td>
            <td>[W m-2]</td>
            <td><a target="_blank" rel="noreferrer" href="https://ldas.gsfc.nasa.gov/nldas/v2/forcing">NLDAS</a></td>
            <td>0.125° x 0.125°</td>
            <td className="checked"></td>
            <td></td>
            <td className="checked"></td>
            <td className="checked"></td>
            <td>TBD</td>
            <td>01/01/2015</td>
            <td>Present</td>
            <td>-</td>
          </tr>
          <tr>
            <td>Rain fraction of total cloud water</td>
            <td>Surface</td>
            <td>convective_precipitation</td>
            <td>FRAIN</td>
            <td>proportion</td>
            <td><a target="_blank" rel="noreferrer" href="https://ldas.gsfc.nasa.gov/nldas/v2/forcing">NLDAS</a></td>
            <td>0.125° x 0.125°</td>
            <td className="checked"></td>
            <td></td>
            <td className="checked"></td>
            <td className="checked"></td>
            <td>TBD</td>
            <td>01/01/2015</td>
            <td>Present</td>
            <td>-</td>
          </tr>
          <tr>
            <td>Convective Available Potential Energy</td>
            <td>180m above ground</td>
            <td>potential_energy</td>
            <td>CAPE</td>
            <td>[J kg-1]</td>
            <td><a target="_blank" rel="noreferrer" href="https://ldas.gsfc.nasa.gov/nldas/v2/forcing">NLDAS</a></td>
            <td>0.125° x 0.125°</td>
            <td className="checked"></td>
            <td></td>
            <td className="checked"></td>
            <td className="checked"></td>
            <td>TBD</td>
            <td>01/01/2015</td>
            <td>Present</td>
            <td>-</td>
          </tr>
          <tr>
            <td>Potential Evaporation</td>
            <td>Surface</td>
            <td>potential_evaporation</td>
            <td>PEVAP</td>
            <td>[kg m-2]</td>
            <td><a target="_blank" rel="noreferrer" href="https://ldas.gsfc.nasa.gov/nldas/v2/forcing">NLDAS</a></td>
            <td>0.125° x 0.125°</td>
            <td className="checked"></td>
            <td></td>
            <td className="checked"></td>
            <td className="checked"></td>
            <td>TBD</td>
            <td>01/01/2015</td>
            <td>Present</td>
            <td>-</td>
          </tr>
          <tr>
            <td rowSpan="2">Rainfall</td>
            <td rowSpan="2">Surface</td>
            <td rowSpan="2">precipitation</td>
            <td rowSpan="2">APCP</td>
            <td rowSpan="2">[mm]</td>
            <td><a target="_blank" rel="noreferrer" href="https://ldas.gsfc.nasa.gov/nldas/v2/forcing">NLDAS</a></td>
            <td>0.125° x 0.125°</td>
            <td rowSpan="2" className="checked"></td>
            <td rowSpan="2"></td>
            <td rowSpan="2" className="checked"></td>
            <td rowSpan="2" className="checked"></td>
            <td rowSpan="2">TBD</td>
            <td>01/01/2015</td>
            <td>05/05/2015</td>
            <td>-</td>
          </tr>
          <tr>
            <td className="hidden"></td>
            <td className="hidden"></td>
            <td className="hidden"></td>
            <td className="hidden"></td>
            <td className="hidden"></td>
            <td><a target="_blank" rel="noreferrer" href="https://www.nssl.noaa.gov/projects/mrms/">MRMS</a></td>
            <td>1 km x 1 km</td>
            <td className="hidden"></td>
            <td className="hidden"></td>
            <td className="hidden"></td>
            <td className="hidden"></td>
            <td className="hidden"></td>
            <td>05/06/2015</td>
            <td>Present</td>
            <td>Only detectable precipitations are stored in the database (rainfall &gt; 0)</td>
          </tr>

          <tr>
            <td>Growing degree days</td>
            <td></td>
            <td>gdd</td>
            <td></td>
            <td></td>
            <td></td>
            <td>0.125° x 0.125°</td>
            <td></td>
            <td></td>
            <td className="checked"></td>
            <td></td>
            <td></td>
            <td>01/01/2015</td>
            <td>Present</td>
            <td>-</td>
          </tr>

        </tbody>              
      </table>
    </>
  )
}
