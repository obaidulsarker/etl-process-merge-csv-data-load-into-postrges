INSERT INTO public.soda_30(
	tstamp, lat, lon, directinclined, diffuseinclined, 
	reflected, globalinclined, directhoriz, diffusehoriz, globalhoriz, 
	clearsky, topofatmosphere, code, temperature, relativehumidity, 
	pressure, windspeed, winddirection, rainfall, snowfall, snowdepth)
SELECT "datetime","Latitude", "Longitude", "Direct Inclined", "Diffuse Inclined", 
	"Reflected", "Global Inclined", "Direct Horiz", "Diffuse Horiz", "Global Horiz", 
	"Clear-Sky", "Top of Atmosphere", "Code", "Temperature", "Relative Humidity", 
	"Pressure", "Wind speed", "Wind direction", "Rainfall", "Snowfall", "Snow depth" 
FROM stg.soda_30 AS stg
WHERE NOT EXISTS (SELECT 1 FROM public.soda_30 AS soda 
				  WHERE soda.tstamp=stg."datetime" 
				  AND soda.lat=stg."Latitude"
				  AND soda.lon=stg."Longitude");