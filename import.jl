# TODO:  Some 3/15/2019 and 3/24/2019 data *may* be duplicated

# Run within /tmp/ramdisk, which should have a folder named nldas:
#   nohup julia import.jl &
# (https://www.linuxbabe.com/command-line/create-ramdisk-linux)

# auth = Base64.base64encode("user:pwd")
# params_diction = Dict("fields"=>fields)
# r = HTTP.request("GET", "https://user:pwd@urs.earthdata.nasa.gov/", ["Authorization" => "Basic $(auth)"]) #, query = params_diction)
# r = HTTP.request("GET", "https://urs.earthdata.nasa.gov/", ["Authorization" => "Basic $(auth)"]) #, query = params_diction)
# r = HTTP.request("GET", "https://user:pwd@urs.earthdata.nasa.gov/") #, query = params_diction)
# HTTP.download("https://hydro1.gesdisc.eosdis.nasa.gov/data/NLDAS/NLDAS_FORA0125_H.002/2021/001/NLDAS_FORA0125_H.A20210101.0000.002.grb")
# exit()
# json_resp = JSON.parse(String(r.body))
# println(json_resp)
# println(r)
# exit()


using GRIB
using LibPQ
using Printf
using Dates
using GZip
using Base64
using HTTP

host="localhost"
db="postgres"

f = open("/home/administrator/API/.env")
s = readline(f)
type, user, pwd = split(s, "|")
close

port="5432"
conn = LibPQ.Connection("host=$host dbname=$db user=$user password=$pwd")
execute(conn, "SET client_min_messages TO WARNING;")

# ________________________________________________________________________________________________________________________
function writeToPostgres(fname, files, year)
  @time begin
    println(fname)
    for file in keys(files)
      result = execute(conn, """
        CREATE TABLE IF NOT EXISTS weather.hourly_$(file)_$(year) (
          date timestamp without time zone,
          lat real,
          lon real,
          air_temperature real,
          humidity real,
          pressure real,
          zonal_wind_speed real,
          meridional_wind_speed real,
          longwave_radiation real,
          convective_precipitation real,
          potential_energy real,
          potential_evaporation real,
          precipitation real,
          shortwave_radiation real,
          relative_humidity real,
          wind_speed real,
          nswrs real,
          nlwrs real,
          dswrf real,
          dlwrf real,
          lhtfl real,
          shtfl real,
          gflux real,
          snohf real,
          asnow real,
          arain real,
          evp real,
          ssrun real,
          bgrun real,
          snom real,
          avsft real,
          albdo real,
          weasd real,
          snowc real,
          snod real,
          tsoil real,
          soilm1 real,
          soilm2 real,
          soilm3 real,
          soilm4 real,
          soilm5 real,
          mstav1 real,
          mstav2 real,
          soilm6 real,
          evcw real,
          trans real,
          evbs real,
          sbsno real,
          cnwat real,
          acond real,
          ccond real,
          lai real,
          veg real
        );
      """)

      # result = execute(conn, "ALTER TABLE weather.hourly_$(file)_$(year) SET UNLOGGED ;") # too slow; maybe SET LOGGED after all data read in?
      result = execute(conn, "COPY weather.hourly_$(file)_$(year) FROM '/tmp/ramdisk/nldas/$(file).csv' delimiter ',' CSV HEADER ;")
      # result = execute(conn, "ALTER TABLE weather.hourly_$(file)_$(year) SET LOGGED ;")
    end
  end
end

# ________________________________________________________________________________________________________________________
function readMRMSOld(fname)
  vals = []
  @time begin
    fh = GZip.open(fname, "r")
    d = read(fh)
    io = open("data.grib2", "w")
    write(io, d)
    close(io)

    files = Dict()

    GribFile("data.grib2") do f
      for message in f
        date = string(message["date"])
        time = string(Int(message["time"] / 100))
        datetime = "$(date[1:4])-$(date[5:6])-$(date[7:8]) $(lpad(time, 2, "0")):00:00"
        lons, lats, values = data(message)
        i = 0
        for lon in lons
          i += 1
          lon -= 360
          # Conterminous US data only
          # if 25 <= lats[i] <= 50 && -125 <= lon <= -67
          if true
            # Exclude non-detectable precipitation to save space
            if values[i] > 0
              fn = "$(trunc(Int, lats[i]))_$(trunc(Int, -lon))"
              if !haskey(files, fn)
                files[fn] = open("csv/$fn.csv", "w");
                write(files[fn], "precipitation,date,lat,lon\n")
              end
              write(files[fn], "$(@sprintf("%.1f", values[i])),$(datetime),$(@sprintf("%.4f", lats[i])),$(@sprintf("%.4f", lon))\r\n")
            end
          end
        end
      end
    end

  end

  return vals
end

# ________________________________________________________________________________________________________________________
function readMRMS(fname)
  vals = Dict()
  @time begin
    fh = GZip.open(fname, "r")
    d = read(fh)
    io = open("data.grib2", "w")
    write(io, d)
    close(io)

    GribFile("data.grib2") do f
      for message in f
        date = string(message["date"])
        time = string(Int(message["time"] / 100))
        datetime = "$(date[1:4])-$(date[5:6])-$(date[7:8]) $(lpad(time, 2, "0")):00:00"
        lons, lats, v = data(message)
        for (lon, lat) in zip(lons, lats)
          lon2 = round(floor((lon - 360) * 8) / 8; digits = 3)
          lat2 = round(-(floor(-lat * 8) / 8); digits = 3)
          println("$lon2 $lat2")
          if !haskey(vals, "$lon2 $lat2")
            vals["$lon2 $lat2"] = []
          end
          push!(vals["$lon2 $lat2"], vcat(lon, lat, v))
        end
      end
    end
  end
  println("ok")
  println(vals)
  return vals
end

# ________________________________________________________________________________________________________________________
files = Dict()

function readNLDAS(fname1, fname2, year, hour)
  global files
  @time begin
    vals = [[], []]

    vn = 0

    for fname in [fname1, fname2]
      vn += 1
      m = 0
      GribFile(fname) do f
        for message in f
          m += 1

          lons, lats, v = data(message)

          # Convert Kelvin to Celsius
          if vn == 1 && m == 1
            v = map(x -> x - 273.15, v)
          end

          push!(vals[vn], v)

          if (fname == fname1 && m == 11)
            relative_humidity = []
            wind_speed = []
            for (air_temperature, humidity, pressure, zonal_wind_speed, meridional_wind_speed) in zip(vals[1][1], vals[1][2], vals[1][3], vals[1][4], vals[1][5])
              push!(relative_humidity, (((humidity/18)/(((1-humidity)/28.97) + (humidity/18)))*pressure/1000) / (0.61*exp((17.27*air_temperature)/(air_temperature + 237.3))))
              push!(wind_speed, sqrt(zonal_wind_speed ^ 2 + meridional_wind_speed ^ 2))
            end
            push!(vals[1], relative_humidity)
            push!(vals[1], wind_speed)
          end

          if (fname == fname2 && m == 37)
            date = string(message["date"])
            time = string(Int(message["time"] / 100))
            datetime = "$(date[1:4])-$(date[5:6])-$(date[7:8]) $(lpad(time, 2, "0")):00:00"

            # println(length(vals[1][1]), " ", length(vals[2][1]))
            i = 0
            for (lon, lat) in zip(lons, lats)
              lon = round(floor(lon * 8) / 8; digits = 3)
              lat = round(-(floor(-lat * 8) / 8); digits = 3)
              i += 1
              if vals[1][1][i] != 9999.0
                fn = "$(trunc(Int, lat))_$(trunc(Int, -lon))"
                if !haskey(files, fn)
                  files[fn] = open("nldas/$fn.csv", "w")
                  write(files[fn], "date,lat,lon,air_temperature,humidity,pressure,zonal_wind_speed,meridional_wind_speed,longwave_radiation,convective_precipitation,potential_energy,potential_evaporation,precipitation,shortwave_radiation,relative_humidity,wind_speed,nswrs,nlwrs,dswrf,dlwrf,lhtfl,shtfl,gflux,snohf,asnow,arain,evp,ssrun,bgrun,snom,avsft,albdo,weasd,snowc,snod,tsoil,soilm1,soilm2,soilm3,soilm4,soilm5,mstav1,mstav2,soilm6,evcw,trans,evbs,sbsno,cnwat,acond,ccond,lai,veg\n")
                end
                if (fname == fname2)
                  write(files[fn], "$(datetime),$(@sprintf("%.4f", lat)),$(@sprintf("%.4f", lon)),$(join(map(x -> round(x[i], sigdigits=6), vcat(vals[1], vals[2])), ","))\n")
                end
              end
            end
          end
        end
      end
    end
  end

  if hour == 23
    for file in values(files)
      close(file)
    end

    writeToPostgres(fname1, files, year)
    files = Dict()
  end
end

# ________________________________________________________________________________________________________________________
function processNLDAS()
  for year=2021:2021
    # for date in Dates.Date(year, 1, 1):Day(1):today()
    for date in Dates.Date(year, 1, 1):Day(1):Dates.Date(year, 12, 31)
      day = lpad(dayofyear(date),   3, "0")
      m2  = lpad(Dates.month(date), 2, "0")
      d2  = lpad(Dates.day(date),   2, "0")
      for hour in 0:23
        h2 = lpad(hour, 2, "0")
        mrms   = "MultiSensor_QPE_01H_Pass2_00.00_$(year)$(m2)$(d2)-$(h2)0000.grib2.gz"
        fname1 = "NLDAS_FORA0125_H.A$(year)$(m2)$(d2).$(h2)00.002.grb"
        fname2 = "NLDAS_MOS0125_H.A$(year)$(m2)$(d2).$(h2)00.002.grb"

        @time begin
          if !isfile("nldas/$fname1")
            cd("./nldas")

            url = "https://mtarchive.geol.iastate.edu/$(year)/$(m2)/$(d2)/mrms/ncep/MultiSensor_QPE_01H_Pass2/$(mrms)"
            run(`curl -O -b ~/.urs_cookies -c ~/.urs_cookies -L -n "$(url)"`)
            println(1)
            mrmsdata = readMRMS(mrms)
            println(2)

            url = "https://hydro1.gesdisc.eosdis.nasa.gov/data/NLDAS/NLDAS_FORA0125_H.002/$(year)/$(day)/$(fname1)"
            run(`curl -O -b ~/.urs_cookies -c ~/.urs_cookies -L -n "$(url)"`)
            
            url = "https://hydro1.gesdisc.eosdis.nasa.gov/data/NLDAS/NLDAS_MOS0125_H.002/$(year)/$(day)/$(fname2)"
            run(`curl -O -b ~/.urs_cookies -c ~/.urs_cookies -L -n "$(url)"`)

            cd("..")

            println("Downloaded $fname1, $fname2, $mrms")
            exit()

            readNLDAS("nldas/$fname1", "nldas/$fname2", year, hour)
            rm("nldas/$fname1")
            rm("nldas/$fname2")
            rm("nldas/$mrms")
          end
        end
      end
    end
  end
end

# ________________________________________________________________________________________________________________________

processNLDAS()
exit()

while true
  processNLDAS()
  print("Pausing for 15 minutes")
  sleep(60 * 15)
end

# readMRMS("mrms.grib2.gz")