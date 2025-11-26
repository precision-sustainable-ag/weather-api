# psql -d weather
# julia -t auto ~/weather/importNew.jl

import Pkg
for pkg in ["ConfigEnv", "NCDatasets", "LibPQ"]
  Base.find_package(pkg) === nothing && Pkg.add(pkg)
end

using ConfigEnv, Printf, Dates, NCDatasets, Base.Threads, LibPQ

using LibPQ
using Printf

home = homedir()

dotenv("$(home)/weather/.env")

conn = LibPQ.Connection("
  host      =$(ENV["DB_HOST"])
  port      =$(get(ENV, "DB_PORT", 5432))
  dbname    =$(ENV["DB_DATABASE"])
  user      =$(ENV["DB_USERNAME"])
  password  =$(ENV["DB_PASSWORD"])
")

row = only(execute(conn, "SELECT now() AS current_time;"))
println("Running: ", row.current_time)

@inline function tile_id_from(lat, lon)
  lat_bin = floor(Int, (lat + 90.0)  * 8)
  lon_bin = floor(Int, (lon + 180.0) * 8)
  return (lat_bin << 12) | lon_bin
end

function download(url, output)
  if !isfile(output) || filesize(output) < 1000000
    while true
      try
        run(`
          curl
            --cookie $home/.urs_cookies \
            --cookie-jar $home/.urs_cookies \
            --silent \
            --output $output $url
        `)
        return
      catch e
        @warn "download error" error=e url=url dest=output
        sleep(2)
      end
    end
  end
end

function readNLDAS(fname1, fname2, year, hour, cdate)
  # for fname in [fname1, fname2]
  #   f = GribFile(fname)
  #   println(fname)
  #   GribFile(fname) do f
  #     for message in f
  #       println(message["parameterName"])
  #     end
  #   end
  # end
  # exit()

  @time begin
    println("readNLDAS")
    @inbounds begin
      vals = Vector{Any}(undef, 11 + 2 + 37)

      for fname in [fname1, fname2]
        ds = Dataset(fname)
        datetime = Dates.format(ds["time"][1:1][1], "yyyy-mm-dd HH:MM:ss")

        # variables = keys(ds)
        # println(variables)

        # https://hydro1.gesdisc.eosdis.nasa.gov/data/NLDAS/NLDAS2_README.pdf
        # ["lon", "lat", "time", "time_bnds", "Tair", "Qair", "PSurf", "Wind_E", "Wind_N", "LWdown", "CRainf_frac", "CAPE", "PotEvap", "Rainf", "SWdown"]
        
        # https://hydro1.gesdisc.eosdis.nasa.gov/data/NLDAS/NLDAS_MOS0125_H.2.0/doc/NLDAS2_README.pdf
        # ["lon", "lat", "time", "time_bnds", "SWdown", "LWdown", "SWnet", "LWnet", "Qle", "Qh", "Qg", "Qf", "Snowf", "Rainf",
        #  "Evap", "Qs", "Qsb", "Qsm", "AvgSurfT", "Albedo", "SWE", "SnowDepth", "SnowFrac", "SoilT",
        #  "SoilM_0_10cm", "SoilM_10_40cm", "SoilM_40_200cm", "SoilM_0_40cm", "SoilM_0_100cm", "SoilM_0_200cm", "SMAvail_0_40cm", "SMAvail_0_200cm", "ECanop", "TVeg", "ESoil",
        #  "SubSnow", "CanopInt", "ACond", "CCond", "LAI", "GVEG", "Streamflow"
        # ]

        if fname == fname1
          Tair = map(x -> ismissing(x) ? x : x - 273.15, ds["Tair"][:])
          relative_humidity = []
          wind_speed = []
          for (air_temperature, humidity, pressure, zonal_wind_speed, meridional_wind_speed) in zip(Tair, ds["Qair"][:], ds["PSurf"][:], ds["Wind_E"][:], ds["Wind_N"][:])
            push!(relative_humidity, (((humidity/18)/(((1-humidity)/28.97) + (humidity/18)))*pressure/1000) / (0.61*exp((17.27*air_temperature)/(air_temperature + 237.3))))
            push!(wind_speed, sqrt(zonal_wind_speed ^ 2 + meridional_wind_speed ^ 2))
          end

          vals[1:13] = [
            Tair,                             # air_temperature
            ds["Qair"][:],                    # humidity
            ds["PSurf"][:],                   # pressure
            ds["Wind_E"][:],                  # zonal_wind_speed
            ds["Wind_N"][:],                  # meridional_wind_speed
            ds["LWdown"][:],                  # longwave_radiation
            ds["CRainf_frac"][:],             # convective_precipitation
            ds["CAPE"][:],                    # potential_energy
            ds["PotEvap"][:],                 # potential_evaporation
            ds["Rainf"][:],                   # precipitation
            ds["SWdown"][:],                  # shortwave_radiation
            relative_humidity,                # relative_humidity
            wind_speed                        # wind_speed
          ]
        elseif fname == fname2
          vals[14:50] = [
            ds["SWnet"][:],                   # nswrs
            ds["LWnet"][:],                   # nlwrs
            ds["SWdown"][:],                  # dswrf
            ds["LWdown"][:],                  # dlwrf
            ds["Qle"][:],                     # lhtfl
            ds["Qh"][:],                      # shtfl
            ds["Qg"][:],                      # gflux
            ds["Snowf"][:],                   # snohf ?
            ds["Qf"][:],                      # asnow ?
            ds["Rainf"][:],                   # arain
            ds["Evap"][:],                    # evp
            ds["Qs"][:],                      # ssrun
            ds["Qsb"][:],                     # bgrun
            ds["Qsm"][:],                     # snom
            ds["AvgSurfT"][:],                # avsft
            ds["Albedo"][:],                  # albdo
            ds["SWE"][:],                     # weasd
            ds["SnowFrac"][:],                # snowc
            ds["SnowDepth"][:],               # snod
            ds["SoilT"][:],                   # tsoil
            ds["SoilM_0_10cm"][:],            # soilm1
            ds["SoilM_10_40cm"][:],           # soilm2
            ds["SoilM_40_200cm"][:],          # soilm3
            ds["SoilM_0_100cm"][:],           # soilm4
            ds["SoilM_0_200cm"][:],           # soilm5
            ds["SMAvail_0_200cm"][:],         # mstav1
            ds["SMAvail_0_40cm"][:],          # mstav2
            ds["SoilM_0_40cm"][:],            # soilm6
            ds["ECanop"][:],                  # evcw
            ds["TVeg"][:],                    # trans
            ds["ESoil"][:],                   # evbs
            ds["SubSnow"][:],                 # sbsno
            ds["CanopInt"][:],                # cnwat
            ds["ACond"][:],                   # acond
            ds["CCond"][:],                   # ccond
            ds["LAI"][:],                     # lai
            ds["GVEG"][:]                     # veg
            # ds["Streamflow"][:]
          ]
          lons = ds["lon"][:]
          lats = ds["lat"][:]

          yyyy = Dates.format(ds["time"][1:1][1], "yyyy")
          mm   = Dates.format(ds["time"][1:1][1], "mm")
          sql = "
            COPY weather (
              date, lat, lon,
              air_temperature, humidity, pressure, zonal_wind_speed, meridional_wind_speed,
              longwave_radiation, convective_precipitation, potential_energy, potential_evaporation,
              precipitation, shortwave_radiation, relative_humidity, wind_speed,
              nswrs, nlwrs, dswrf, dlwrf, lhtfl, shtfl, gflux, snohf, asnow, arain,
              evp, ssrun, bgrun, snom, avsft, albdo, weasd, snowc, snod, tsoil,
              soilm1, soilm2, soilm3, soilm4, soilm5, mstav1, mstav2, soilm6, evcw,
              trans, evbs, sbsno, cnwat, acond, ccond, lai, veg, tile_id
            )
            FROM STDIN WITH (FORMAT csv, HEADER false, NULL '')
          "
          ch = Channel{String}(256) do c
            buf = IOBuffer()
            @inbounds begin
              k = 0
              for i in eachindex(lats)
                lat = round(-(floor(-lats[i] * 8) / 8); digits = 3)
                for j in eachindex(lons)
                  lon = round(floor(lons[j] * 8) / 8; digits = 3)
                  k += 1
                  if ismissing(vals[1][k])
                    continue
                  end

                  # build one CSV line
                  print(buf, datetime, ',', lat, ',', lon)
                  for col in vals
                    write(buf, ',')
                    v = col[k]
                    v === missing || print(buf, v)  # empty field => NULL '' on COPY
                  end
                  write(buf, ',')
                  print(buf, tile_id_from(lat, lon))
                  write(buf, '\n')

                  put!(c, String(take!(buf)))  # take! clears buf
                end
              end
            end
          end
          LibPQ.execute(conn, LibPQ.CopyIn(sql, ch))
        end
        close(ds)
      end
    end
  end
end

year0 = 2015
start_date = DateTime(year0, 1, 1, 0, 0, 0)

while true
  try
    local row = first(execute(conn, "SELECT MAX(date) AS dt FROM weather WHERE lat=30 AND lon=-115.875;"))
    if row === nothing || row.dt === nothing
      error("Couldn't determine max date")
    end

    global start_date = row.dt + Dates.Hour(1)
  catch e
    println("Couldn't determine max date. Error: ", e)
    exit();
  end
  
  end_date = now()

  println("Downloading from $(start_date) to $(end_date)")

  # @threads for i in 1:length(dates)
  for date in collect(start_date:Dates.Hour(1):end_date)
    jd = lpad(dayofyear(date), 3, "0")
    y = year(date)
    m2 = lpad(month(date), 2, "0")
    d2 = lpad(day(date), 2, "0")
    h2 = lpad(hour(date), 2, "0")

    fname1 = "NLDAS_FORA0125_H.A$(y)$(m2)$(d2).$(h2)00.020.nc"
    fname2 = "NLDAS_MOS0125_H.A$(y)$(m2)$(d2).$(h2)00.020.nc"
    out1 = "/mnt/data/$(y)$(m2)$(d2)$(h2).nc"
    out2 = "/mnt/data/m$(y)$(m2)$(d2)$(h2).nc"

    download("https://hydro1.gesdisc.eosdis.nasa.gov/data/NLDAS/NLDAS_FORA0125_H.2.0/$(y)/$(jd)/$(fname1)", out1)
    download("https://hydro1.gesdisc.eosdis.nasa.gov/data/NLDAS/NLDAS_MOS0125_H.2.0/$(y)/$(jd)/$(fname2)", out2)

    if !isfile(out1) || !isfile(out2) || filesize(out1) < 1_000_000 || filesize(out2) < 1_000_000
      println("Pausing 60 minutes before checking for new data")
      sleep(60 * 60)
      break
    end

    println(out1)
    readNLDAS(out1, out2, y, hour(date), date)
  end
end