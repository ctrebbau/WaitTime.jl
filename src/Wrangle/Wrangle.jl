using CSV, DataFrames, PyCall, Dates, Statistics

export wrangle, interptime
"""
    wrangle(;path)::DataFrame
    Reads in CSV file and performs feature engineering

    interptime(milliseconds)
    Interpretation of milliseconds in hours or minutes
"""
function wrangle(path="/Users/CAT79/Job/sql_scripts/RequestArrivalTime/request_pick.csv")
	df = CSV.read(path, DataFrame; dateformat = "y-m-d H:M:S")

	es_holidays = pyimport("holidays").ES(years=[2018, 2019, 2020, 2021, 2022])

	# NorthWest <-> NorthEast = 32.05Km (Las Rozas <-> Paracuellos)
	NorthWestMad = (lat = 40.502537940865665, lon = -3.8484834317689725) 
	NorthEastMad = (lat = 40.502537940865665, lon = -3.469455101300924)
	
	# SouthWest <-> SouthEast = 32.15 Km (Loranca <-> Arganda del Rey)
	SouthWestMad = (lat = 40.28813158355016, lon = -3.8484834317689725)
	SouthEastMad = (lat = 40.28813158355016, lon = -3.469455101300924)

	global EastWestBound = collect(range(NorthWestMad.lon, NorthEastMad.lon, length=200))
	global NorthSouthBound = collect(range(SouthWestMad.lat, NorthWestMad.lat, length=200))
	#= 
	SouthWest <-> NorthEast = 39.98Km (Diag Loranca <-> Pracuellos) 
	North <-> South = 23.84Km
	=#
	function assignblock(coords, ret)
		blocks = []
		for coord in coords
			for i in eachindex(ret)
				if i < length(ret)
					if ret[i] <= coord < ret[i+1]
						push!(blocks, coord => i)
					end
				end
			end
		end
		blocks
	end
	
	df[!, :RequestHr]  = Dates.hour.(df[!,   :RequestServiceTime])
	df[!, :RequestMin] = Dates.minute.(df[!, :RequestServiceTime])
	df[!, :RequestSec] = Dates.second.(df[!, :RequestServiceTime])
	
	df[!, :StartServiceHr] = Dates.hour.(df[!, :StartServiceTime])
	df[!, :StartServiceMin] = Dates.minute.(df[!, :StartServiceTime])
	df[!, :StartServiceSec] = Dates.second.(df[!, :StartServiceTime])

	df[!, :WeekDay] = Dates.dayname.(df[!, :RequestServiceTime])
	df[!, :IsWeekend] = map(d -> d == "Saturday" || d == "Sunday", df.WeekDay)
	df[!, :Holiday] = map(d -> Date(d) in keys(es_holidays) ? es_holidays[Date(d)] : "workday", df[!, :StartServiceTime])
	df[!, :IsHoliday] = df[!, :Holiday] .!= "workday"

	df[!, :WaitTime] = df[!, :StartServiceTime] .- df[!, :RequestServiceTime]
	df[!, :WaitTime] = Dates.value.(df[!, :WaitTime]) ./ 1000

	df = filter!(df -> SouthWestMad.lat .<= df.ClientLat .<= NorthWestMad.lat, df)
	df = filter!(df -> SouthWestMad.lat .<= df.DriverLat .<= NorthWestMad.lat, df)
	
	df = filter!(df -> NorthWestMad.lon .<= df.ClientLong .< NorthEastMad.lon, df)
	df = filter!(df -> NorthWestMad.lon .<= df.DriverLong .< NorthEastMad.lon, df)
	
	clientlatblocks = assignblock(df.ClientLat, NorthSouthBound)
	clientlonblocks = assignblock(df.ClientLong, EastWestBound)

	df[!, :ClientLatBlock] = [block[2] for block in clientlatblocks]
	df[!, :ClientLonBlock] = [block[2] for block in clientlonblocks]

	driverlatblocks = assignblock(df.DriverLat, NorthSouthBound)
	driverlonblocks = assignblock(df.DriverLong, EastWestBound)
	
	df[!, :DriverLatBlock] = [block[2] for block in driverlatblocks]
	df[!, :DriverLonBlock] = [block[2] for block in driverlonblocks]

	clientblockgroup = groupby(df, [:ClientLatBlock, :ClientLonBlock])
	driverblockgroup = groupby(df, [:DriverLatBlock, :DriverLonBlock])
	global meantimetoclientblock = combine(clientblockgroup, :WaitTime => mean)
	global meantimefromdriverblock = combine(driverblockgroup, :WaitTime => mean)
	global countclientblock = combine(clientblockgroup, nrow)
	global countdriverblock = combine(driverblockgroup, nrow)
	
	df[!, :ClientBlockMeanTime] = innerjoin(df, meantimetoclientblock, on = [:ClientLatBlock, :ClientLonBlock]).WaitTime_mean
	df[!, :DriverBlockMeanTime] = innerjoin(df, meantimefromdriverblock, on = [:DriverLatBlock, :DriverLonBlock]).WaitTime_mean

	df[!, :IncomingCount] = innerjoin(df, countclientblock, on=[:ClientLatBlock, :ClientLonBlock]).nrow
	df[!, :OutgoingCount] = innerjoin(df, countdriverblock, on=[:DriverLatBlock, :DriverLonBlock]).nrow

	
	df = filter!(:Distance => d -> d > 0, df)
	df = filter!(:WaitTime => d -> d > 0, df)
	df = df[df[!, :WaitTime] .< quantile(df[!, :WaitTime], 0.9), :]
	
	return df
end

interptime(t) = "$(t * 2.777778e-4) hours or $(t ÷ 60) minutes"
