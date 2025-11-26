# psql -d weather
# julia -t auto ~/weather/importMRMS.jl

using ConfigEnv, Printf, Dates, NCDatasets, Base.Threads, LibPQ

home = homedir()

cd("/mnt/data/")

function download(url, output)
  if !isfile("mrms/$(output)")
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

start_date = DateTime(2015, 5, 7, 0, 0, 0) # first available MRMS data

start_date = DateTime(2025, 11, 9, 15, 0, 0)

while true
  end_date = now()
  println("Downloading from $(start_date) to $(end_date)")

  for date in collect(start_date:Dates.Hour(1):end_date)
    jd = lpad(dayofyear(date), 3, "0")
    y = year(date)
    m2 = lpad(month(date), 2, "0")
    d2 = lpad(day(date), 2, "0")
    h2 = lpad(hour(date), 2, "0")

    if date < DateTime(2020, 10, 13, 0, 0, 0) # old format
      mrms = "GaugeCorr_QPE_01H_00.00_$(y)$(m2)$(d2)-$(h2)0000.grib2.gz"
      mrms_url = "https://mtarchive.geol.iastate.edu/$(y)/$(m2)/$(d2)/mrms/ncep/GaugeCorr_QPE_01H/$(mrms)"
      out3 = "omrms$(y)$(m2)$(d2)$(h2).gz"
    else
      mrms = "MultiSensor_QPE_01H_Pass2_00.00_$(y)$(m2)$(d2)-$(h2)0000.grib2.gz"
      mrms_url = "https://mtarchive.geol.iastate.edu/$(y)/$(m2)/$(d2)/mrms/ncep/MultiSensor_QPE_01H_Pass2/$(mrms)"
      out3 = "mrms$(y)$(m2)$(d2)$(h2).gz"
    end

    if (isfile(out3))
      continue
    end

    download(mrms_url, out3)
    if isfile(out3)
      if filesize(out3) < 10_000
        # println("Empty file $(out3)")
        rm(out3)
      else
        println(out3)
      end
    end
  end

  cmd = Cmd([
    "/usr/bin/time",
    "-f", "Elapsed %E  CPU %P  MaxRSS %M KB",
    "bash", "-lc",
    "ls *.gz | parallel -j4 '~/wct/wct-export {} {/.}.csv csv ~/wct/wctBatchConfig.xml'"
  ])
  run(cmd)

  run(`bash -lc 'mkdir -p mrms; mv *mrms*gz mrms/'`)

  run(`psql -d weatherdb -c "\copy mrms(precipitation,date,lat,lon) FROM PROGRAM 'tail -n +2 -q *.csv' CSV"`)

  println("Pausing 15 minutes before checking for new data")
  sleep(15 * 60)
end