# using GRIB
using HTTP
using Downloads
using SparseArrays
using Suppressor
using LibPQ
using Printf
using Dates
using GZip
using NCDatasets

# using Base64
# using Base: run
# using Base

# ________________________________________________________________________________________________________________________
function showSyntax()
  function green(string)
    return "\u001b[1m\u001b[32m$(string)\u001b[0m"
  end
  
  println("""
    Syntax:
      These create queries but don't execute them:
        autovacuum "wildcard" true|false :  $(green("autovacuum \"hourly%2015\" true"))
        deleteDate "yyyy-mm-hh hh:mm:ss" "wildcard":  $(green("deleteDate \"2015-11-01 05:00:00\" \"hourly%2015\""))
        dropTables "wildcard":  $(green("dropTables \"hourly%2015\""))
        renameTables "wildcard":  $(green("renameTables \"hourly%2015\""))

      These execute queries:
        createIndexes "wildcard":  $(green("createIndexes \"hourly%2015\""))
        hourlyAverages
        schemas
        showQueries
        schemaSize schema:  $(green("schemaSize plants3"))
        yearlyNLDAS:  $(green("yearlyNLDAS"))
  """)
end

if (length(ARGS) < 1)
  showSyntax()
  exit()
end

# ________________________________________________________________________________________________________________________
# cd /dev/shm
# sudo -s
# nohup julia ~/API/importNew.jl yearlyNLDAS > output2.log 2>&1 &

# psql:
#   SET work_mem = '100MB';   -- 8MB
#   SET work_mem = '128MB';   -- 64MB
#   SELECT pg_reload_conf();

# auth = Base64.base64encode("user:pwd")
# params_diction = Dict("fields"=>fields)
# r = HTTP.request("GET", "https://user:pwd@urs.earthdata.nasa.gov/", ["Authorization" => "Basic $(auth)"]) #, query = params_diction)
# r = HTTP.request("GET", "https://urs.earthdata.nasa.gov/", ["Authorization" => "Basic $(auth)"]) #, query = params_diction)
# r = HTTP.request("GET", "https://user:pwd@urs.earthdata.nasa.gov/") #, query = params_diction)
# HTTP.download("https://hydro1.gesdisc.eosdis.nasa.gov/data/NLDAS/NLDAS_FORA0125_H.002/2021/001/NLDAS_FORA0125_H.A20210101.0000.002.grb")

# lat = 38.032056
# lon = -75.0
# lat = round(-(floor(-lat * 8) / 8); digits = 3)
# lon = round(floor(lon * 8) / 8; digits = 3)
# println(lat)
# println(lon)
# exit();

open("/home/administrator/API/.env") do f
  s = readline(f)
  type, user, pwd = split(s, "|")
  host="localhost"
  db="postgres"
  global conn = LibPQ.Connection("host=$host dbname=$db user=$user password=$pwd")
  execute(conn, "SET client_min_messages TO WARNING;")  
end

files = Dict()

# ________________________________________________________________________________________________________________________
function writeToPostgres(fname, files, year)
  @time begin
    println("writeToPostgres start")
    flush(stdout)

    for file in keys(files)
      table = "nldas_hourly"
      result = execute(conn, """
        CREATE TABLE IF NOT EXISTS weather.$(table) (
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
        ) WITH (autovacuum_enabled = false);
      """)

      waitIdle()
      result = execute(conn, "COPY weather.$(table) FROM '/dev/shm/$(file).csv' delimiter ',' CSV HEADER ;")
    end
  end
  # exit()
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
function readMRMSOK(fname)
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
function readMRMS(fname)  # Chat-GPT
  @time begin
    println("MRMS" * fname)
    vals = sparse(zeros(Float64, 360 * 8, 180 * 8))
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
        for (lon, lat, val) in zip(lons, lats, v)
          lon2 = round(floor((lon - 360) * 8) / 8; digits = 3)
          lat2 = round(-(floor(-lat * 8) / 8); digits = 3)
          row = Int((lon2 + 360) * 8)
          col = Int((lat2 + 90) * 8)
          # println("$lon2 $lat2")
          vals[row, col] += val
        end
      end
    end
  end
  return vals
end

# ________________________________________________________________________________________________________________________
function haltIfFileExists(file_path)
  if isfile(file_path)
    error("File $file_path already exists. Halting execution.")
  end
end

# ________________________________________________________________________________________________________________________
function getRunningQueries()
  result = execute(conn, "SELECT COUNT(*) FROM pg_stat_activity WHERE state = 'active' AND query NOT LIKE '%sleep%';")
  return first(result)[1]
end

# ________________________________________________________________________________________________________________________
function showQueries()
  result = execute(conn, "SELECT query FROM pg_stat_activity WHERE state = 'active' AND query NOT LIKE '%sleep%';")
  for row in result
    println(row[1]);
  end
end

# ________________________________________________________________________________________________________________________
function waitIdle(s = "")
  running_queries = getRunningQueries()
  
  if running_queries > 0
    print("Waiting for running queries to complete $s")
    sleep(3)
    
    while running_queries > 1
      print(".")
      sleep(3)
      running_queries = getRunningQueries()
    end
    println()
  end
end

# ________________________________________________________________________________________________________________________
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
      global files
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
          k = 0
          for i in 1:size(lats)[1]
            for j in 1:size(lons)[1]
              lat = round(-(floor(-lats[i] * 8) / 8); digits = 3)
              lon = round(floor(lons[j] * 8) / 8; digits = 3)
              k += 1
              if !ismissing(vals[1][k])
                # fn = "$(trunc(Int, lat))_$(trunc(Int, -lon))"
                fn = "nldas"
                if !haskey(files, fn)
                  files[fn] = open("$fn.csv", "w")
                  write(files[fn], "date,lat,lon,air_temperature,humidity,pressure,zonal_wind_speed,meridional_wind_speed,longwave_radiation,convective_precipitation,potential_energy,potential_evaporation,precipitation,shortwave_radiation,relative_humidity,wind_speed,nswrs,nlwrs,dswrf,dlwrf,lhtfl,shtfl,gflux,snohf,asnow,arain,evp,ssrun,bgrun,snom,avsft,albdo,weasd,snowc,snod,tsoil,soilm1,soilm2,soilm3,soilm4,soilm5,mstav1,mstav2,soilm6,evcw,trans,evbs,sbsno,cnwat,acond,ccond,lai,veg\n")
                end
                write(files[fn], "$(datetime),$(@sprintf("%.4f", lat)),$(@sprintf("%.4f", lon)),$(join(map(x -> ismissing(x[k]) ? 9999.0 : round(x[k], sigdigits=6), vals), ","))\n")
              end
            end
          end
        end
        close(ds)
      end

      if true # hour == 23 # && dayofweek(cdate) == 7
        for file in values(files)
          close(file)
        end

        waitIdle(fname1)

        writeToPostgres(fname1, files, year)
        sleep(2)
        files = Dict()
      end
    end
  end
  # exit()
end

function contains(filename, string)
  file = open(filename, "r")
  file_contents = read(file, String)
  close(file)
  return occursin(string, file_contents)
end

# ________________________________________________________________________________________________________________________
# Downloads and imports NLDAS data for a given year.
function yearlyNLDAS()
  home = homedir()

  # start_date = nothing  
  # try
  #   println("SELECT MAX(date) FROM weather.nldas_hourly");
  #   result = execute(conn, "SELECT MAX(date) FROM weather.nldas_hourly;")
  #   rows = collect(result)  # Convert the result into a collection
    
  #   if !isempty(rows)
  #     start_date = rows[1][1] + Dates.Hour(1)  # Extract the MAX(date) value
  #     println("Start date determined: ", start_date)
  #   else
  #     println("No rows returned in the result.")
  #   end    
  # catch e
  #   println("Couldn't determine max date. Error: ", e)
  #   exit();
  # end

  # start_date = DateTime(2018, 5, 20, 11, 0, 0)
  # end_date = start_date
  # end_date = now()

  start_date = DateTime(2020, 03, 16, 21, 0, 0)
  end_date = DateTime(2024, 12, 31, 23, 0, 0)

  date = start_date
  println("$(start_date) $(end_date)")
  while date <= end_date
    @time begin
      println("yearlyNLDAS")
      day = lpad(dayofyear(date), 3, "0")
      year = Dates.year(date)
      m2 = lpad(Dates.month(date), 2, "0")
      d2 = lpad(Dates.day(date),  2, "0")
      hour = Dates.hour(date)
      h2 = lpad(hour, 2, "0")

      mrms   = "MultiSensor_QPE_01H_Pass2_00.00_$(year)$(m2)$(d2)-$(h2)0000.grib2.gz"
      # fname1 = "NLDAS_FORA0125_H.A$(year)$(m2)$(d2).$(h2)00.002.grb"
      # fname2 = "NLDAS_MOS0125_H.A$(year)$(m2)$(d2).$(h2)00.002.grb"

      fname1 = "NLDAS_FORA0125_H.A$(year)$(m2)$(d2).$(h2)00.020.nc"
      fname2 = "NLDAS_MOS0125_H.A$(year)$(m2)$(d2).$(h2)00.020.nc"

      # download(year, m2, d2, h2)
      # url = "https://mtarchive.geol.iastate.edu/$(year)/$(m2)/$(d2)/mrms/ncep/MultiSensor_QPE_01H_Pass2/$(mrms)"
      # run(`curl -s -O -b ~/.urs_cookies -c ~/.urs_cookies -L -n "$(url)" > /dev/null 2>&1`)
      # mrmsdata = readMRMS(mrms)
      # exit()

      url = "https://hydro1.gesdisc.eosdis.nasa.gov/data/NLDAS/NLDAS_FORA0125_H.2.0/$(year)/$(day)/$(fname1)"
      println(url)
      while true
        try
          run(`curl -s -o "file1" -b $(home)/.urs_cookies -c $(home)/.urs_cookies -L -n $(url)`)
          if contains("file1", "DOCTYPE")
            println("No more data.  Waiting one hour.")
            flush(stdout)
            sleep(3600)
          else
            ds = Dataset("file1")
            datetime = Dates.format(ds["time"][1:1][1], "yyyy-mm-dd HH:MM:ss")
            break
          end
        catch e
          println("Error: ", e)
          println("Waiting one hour.")
          flush(stdout)
          sleep(3600)
        end
      end

      url = "https://hydro1.gesdisc.eosdis.nasa.gov/data/NLDAS/NLDAS_MOS0125_H.2.0/$(year)/$(day)/$(fname2)"
      println(url)
      while true
        try
          run(`curl -s -o "file2" -b $(home)/.urs_cookies -c $(home)/.urs_cookies -L -n $(url)`)
          if contains("file2", "DOCTYPE")
            println("No more data.  Waiting one hour.")
            flush(stdout)
            sleep(3600)
          else
            ds = Dataset("file2")
            datetime = Dates.format(ds["time"][1:1][1], "yyyy-mm-dd HH:MM:ss")
            break
          end
        catch e
          println("Error: ", e)
          println("Waiting one hour.")
          flush(stdout)
          sleep(3600)
        end
      end
      
      println("Downloaded $fname1, $fname2, $mrms")
      flush(stdout)

      readNLDAS("file1", "file2", year, hour, date)
    end
    date += Hour(1)
    haltIfFileExists("/home/administrator/API/halt")
  end
end

# ________________________________________________________________________________________________________________________
function tables(match, condition="", having="")
  return execute(
    conn,
    """
      SELECT t.tablename
      FROM pg_tables t
      LEFT JOIN pg_indexes i ON t.schemaname = i.schemaname AND t.tablename = i.tablename
      WHERE t.tablename LIKE '$(match)' $(condition)
      GROUP BY t.tablename
      $(having)
      ORDER BY 1;
    """
  )
end

# ________________________________________________________________________________________________________________________
# Creates date and lat/lon indexes
# Example: createIndexes("nldas_hourly%2016")
function createIndexes(match)
  for row in tables(match, "", "HAVING COUNT(i.indexname) < 2")
    println("Creating date and lat/lon indexes on $(row[1])")
    waitIdle()
    execute(conn, "CREATE INDEX ON weather.$(row[1]) (date)");
    waitIdle()
    execute(conn, "CREATE INDEX ON weather.$(row[1]) (lat, lon)");
  end
end

# ________________________________________________________________________________________________________________________
# Deletes a specific date-time from the weather database.
# Useful if a GRIB file is aborted during an import.
# Example: deleteDate("2015-11-01 05:00:00", "nldas_hourly%2015")
function deleteDate(date, match)
  open("/dev/shm/output.sql", "w") do f
    for row in tables(match)
      println(f, "DELETE FROM weather.$(row[1]) WHERE \"date\" = '$(date)';")
    end
  end
  println("Proof /dev/shm/output.sql, then import it using \\i");
end

function autovacuum(match, bool)
  open("/dev/shm/vac.sql", "w") do f
    for row in tables(match)
      println(f, "ALTER TABLE weather.$(row[1]) SET (autovacuum_enabled = $(bool));")
    end
  end
  println("Proof /dev/shm/vac.sql, then import it using \\i");
end

# ________________________________________________________________________________________________________________________
# Drops tables from the weather database.
# Example: dropTables("nldas_hourly%2015")
function dropTables(match)
  open("/dev/shm/output.sql", "w") do f
    for row in tables(match)
      println(f, "DROP TABLE IF EXISTS weather.$(row[1]);")
    end
  end
  println("Proof /dev/shm/output.sql, then import it using \\i");
end

# ________________________________________________________________________________________________________________________
# Renames hourly tables to their nldas equivalent.
# Use after importing a year's data.
# Example: renameTables("hourly%2016")
function renameTables(match)
  open("/dev/shm/output.sql", "w") do f
    for row in tables(match)
      println(f, "DROP TABLE IF EXISTS weather.nldas_$(row[1]);")
      println(f, "ALTER TABLE weather.$(row[1]) RENAME TO nldas_$(row[1]);")
    end
  end
  println("Proof /dev/shm/output.sql, then import it using \\i");
end

# ________________________________________________________________________________________________________________________
function schemaSize(schema)
  # result = execute(conn, """
  #   SELECT count(*), pg_catalog.pg_size_pretty(sum(pg_catalog.pg_total_relation_size(c.oid)))
  #   FROM pg_catalog.pg_namespace n
  #   LEFT JOIN pg_catalog.pg_class c ON n.oid = c.relnamespace
  #   WHERE n.nspname = '$(schema)';
  # """)

  result = execute(conn, """
    SELECT 
      (SELECT COUNT(*) 
      FROM pg_catalog.pg_namespace n2
      LEFT JOIN pg_catalog.pg_class c2 ON n2.oid = c2.relnamespace
      WHERE n2.nspname = '$(schema)') AS row_count,
      COUNT(*) AS object_count,
      pg_catalog.pg_size_pretty(sum(pg_catalog.pg_total_relation_size(c.oid))) AS total_size
    FROM pg_catalog.pg_namespace n
    LEFT JOIN pg_catalog.pg_class c ON n.oid = c.relnamespace  
    WHERE n.nspname = '$(schema)'  
    """)
  println(first(result))
  println(first(result)[1], " tables")
  println(first(result)[2])
end

# ________________________________________________________________________________________________________________________
function schemas()
  result = execute(conn, """
    SELECT nspname AS schema_name 
    FROM pg_catalog.pg_namespace
    WHERE nspname NOT IN ('information_schema', 'pg_catalog', 'public')
    ORDER BY schema_name;  
  """)
  for row in result
    println(row[1])
  end
end

# ________________________________________________________________________________________________________________________
function hourlyAverages()
  execute(conn, "DELETE FROM weather.queries WHERE url LIKE '%averages%'")

  year = Dates.year(Dates.today()) - 1
  
  columns = join(
    [
      "lat", "lon", "air_temperature", "humidity", "pressure", "zonal_wind_speed", "meridional_wind_speed", "longwave_radiation",
      "convective_precipitation", "potential_energy", "potential_evaporation", "precipitation", "shortwave_radiation",
      "relative_humidity", "wind_speed"
    ],
    ", "
  )

  # for row in tables("nldas%$(year)")
  for row in tables("nldas%$(year)", "AND t.tablename > 'nldas_hourly_52_106'")
    _, hourly, lat, lon = split(row[1], "_")
    
    println("weather.ha_$(lat)_$(lon)")
    
    execute(conn, "DROP TABLE IF EXISTS weather.ha_$(lat)_$(lon);")
    
    waitIdle()
    execute(conn, """
      SELECT * INTO weather.ha_$(lat)_$(lon)
      FROM (
        SELECT
          date2 AS date, lat, lon, 
          avg(air_temperature) AS air_temperature,
          avg(humidity) AS humidity,
          avg(pressure) AS pressure,
          avg(zonal_wind_speed) AS zonal_wind_speed,
          avg(meridional_wind_speed) AS meridional_wind_speed,
          avg(longwave_radiation) AS longwave_radiation,
          avg(convective_precipitation) AS convective_precipitation,
          avg(potential_energy) AS potential_energy,
          avg(potential_evaporation) AS potential_evaporation,
          avg(precipitation) AS precipitation,
          avg(shortwave_radiation) AS shortwave_radiation,
          avg(relative_humidity) AS relative_humidity,
          avg(wind_speed) AS wind_speed
        FROM (
          SELECT date + INTERVAL '$(2103 - year) years' AS date2, $(columns) FROM weather.nldas_hourly_$(lat)_$(lon)_$(year - 4)
          UNION ALL
          SELECT date + INTERVAL '$(2102 - year) years' AS date2, $(columns) FROM weather.nldas_hourly_$(lat)_$(lon)_$(year - 3)
          UNION ALL
          SELECT date + INTERVAL '$(2101 - year) years' AS date2, $(columns) FROM weather.nldas_hourly_$(lat)_$(lon)_$(year - 2)
          UNION ALL
          SELECT date + INTERVAL '$(2100 - year) years' AS date2, $(columns) FROM weather.nldas_hourly_$(lat)_$(lon)_$(year - 1)
          UNION ALL
          SELECT date + INTERVAL '$(2099 - year) years' AS date2, $(columns) FROM weather.nldas_hourly_$(lat)_$(lon)_$(year - 0)
        ) a
        GROUP BY date2, lat, lon
      ) b;
    """)
  end
end

# ________________________________________________________________________________________________________________________
try
  if ARGS[1] == "createIndexes" && length(ARGS) == 2
    createIndexes(ARGS[2])
  elseif ARGS[1] == "deleteDate" && length(ARGS) == 3
    deleteDate(ARGS[2], ARGS[3])
  elseif ARGS[1] == "dropTables" && length(ARGS) == 2
    dropTables(ARGS[2])
  elseif ARGS[1] == "renameTables" && length(ARGS) == 2
    renameTables(ARGS[2])
  elseif ARGS[1] == "yearlyNLDAS"
    yearlyNLDAS()
  elseif ARGS[1] == "schemaSize" && length(ARGS) == 2
    schemaSize(ARGS[2])
  elseif ARGS[1] == "schemas"
    schemas()
  elseif ARGS[1] == "showQueries"
    showQueries()
  elseif ARGS[1] == "hourlyAverages"
    hourlyAverages()
  elseif ARGS[1] == "autovacuum" && length(ARGS) == 3
    autovacuum(ARGS[2], ARGS[3])
  else
    showSyntax()
  end
catch e
  Base.show_backtrace(stderr, catch_backtrace())
  for (i, bt) in enumerate(Base.catch_backtrace())
    println(bt)
    if i >= 5  # Limit to the first 5 frames
      println("... (stack trace truncated)")
      break
    end
  end
end

# readMRMS("mrms.grib2.gz")

# GribFile("/home/administrator/Public/NLDAS_MOS0125_H.A20170101.0000.002.grb") do f
#   for message in f
#     date = string(message["date"])
#     time = string(Int(message["time"] / 100))
#     lons, lats, values = data(message)
#     println(message["parameterName"])
#     # for key in keys(message)
#     #   println(values)
#     # end
#     # exit()
#   end
# end
# exit()
