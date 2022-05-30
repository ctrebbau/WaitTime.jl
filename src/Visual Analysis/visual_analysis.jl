using DataFrames, GLMakie
include("../Wrangle/Wrangle.jl")

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
