using CSV, DataFrames, PyCall, Dates, Statistics, MLJ, GLMakie

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

df = wrangle()


function xyz()
	nsamples = 140
	sampledf(df, n) = df[shuffle(1:nrow(df))[1:n], :]
	xyz = sampledf(meantimetoclientblock, nsamples)
	x, y, z = (x=xyz.ClientLonBlock), (y=xyz.ClientLatBlock), (z=xyz.WaitTime_mean)
end

function heat()
	x,y,z = xyz()
	d = DataFrame(:x=>x, :y=>y, :z=>z)
	grid = zeros(eltype(z), length(NorthSouthBound), length(EastWestBound))
	for i in axes(grid, 1)
		for j in axes(grid, 2)
			if findfirst((d.x .== i) .& (d.y .== j)) != nothing
				 grid[i,j] = d[findfirst((d.x .== i) .& (d.y .== j)), :z]
			end
		end
	end
	return grid
end
heatmap(heat())
vscodedisplay(heat())

function plot_heatmap()
	x, y, z = xyz()
	fig = Figure()
	ax = Axis(fig[1,1]; xlabel = "Longitude Block", ylabel = "Latitude Block")
	heatmap!(ax,x,y,heat())
	fig
end

plot_heatmap()

function plot_contour()
	x, y, z = xyz()
	fig = Figure()
	ax = Axis(fig[1,1]; xlabel = "Longitude", ylabel = "Latitude", aspect=DataAspect())
	contour!(ax,x,y,z; levels=10)
	fig
end

plot_contour()


function scatter_in_3D()
	fig = Figure()
	ax1 = Axis3(fig[1,1]; perspectiveness=0.5, xlabel="Longitude Block", ylabel="Latitude Block", zlabel="WaitTime (ms)")
	ax2 = Axis3(fig[1,2]; aspect=:data, perspectiveness=0.5, xlabel="Longitude Block", ylabel="Latitude Block", zlabel="WaitTime (ms)")
	ax3 = Axis3(fig[1,3]; perspectiveness=0.5, xlabel="Longitude Block", ylabel="Latitude Block", zlabel="WaitTime (ms)")

	scatter!(ax1, clientpoints.ClientLatBlock, clientpoints.ClientLonBlock, clientpoints.WaitTime_mean;)
	meshscatter!(ax2, clientpoints.ClientLatBlock, clientpoints.ClientLonBlock, clientpoints.WaitTime_mean; markersize=0.25)
	hm = meshscatter!(ax3, clientpoints.ClientLatBlock, clientpoints.ClientLonBlock, clientpoints.WaitTime_mean;
						marker=Rect3f(Vec3f(0), Vec3f(1)), color=1:size(clientpoints,2), colormap=:plasma, transparency=false)
	Colorbar(fig[1,4], hm, label="WaitTime", height=Relative(0.5))
	fig
end

scatter_in_3D()


function bubblemap()




fig, ax, pltobj = scatter(clientpoints[:, :ClientLatBlock], clientpoints[:, :ClientLonBlock]; color=clientpoints[:, :WaitTime_mean],
    label="Mean Time (log10)", colormap=:plasma, markersize= sqrt.((clientpoints[:, :WaitTime_mean])),
    figure=(; resolution=(600, 400)), axis=(; aspect=DataAspect()))
Colorbar(fig[1, 2], pltobj, height=Relative(3 / 4))










fig = Figure()

ax1 = fig[1,1] = Axis(fig,

	title="Wait Time",
# x-axis
	xlabel = "Time to request (s)",
	xgridcolor = :darkgrey,
	xgridwidth = 1,
	xlabel = "Longitude Block",
	# y-axis
	ygridcolor = :darkgrey,
	ylabel = "Latitude Block",
	ygridwidth = 1
	
	title = "Time to request"
)

	







CSV.write("/Users/CAT79/Desktop/sample.xyz")
using GMT
grdconvert("/Users/CAT79/Desktop/sample.xyz");
G = gmt("grdconvert /Users/CAT79/Desktop/sample.xyz")

G = GMT.peaks();
D = grd2xyz(G)
