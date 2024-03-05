INSERT INTO public.measurements_v1_realtime(
	logger_id, station_name, tstamp, pyrgeometer_body_temperature_avg, pyrgeometer_body_temperature_max, 
	pyrgeometer_body_temperature_min, pyrgeometer_body_temperature_stdev, 
	pyrgeometer_body_temperature_count, dhi_solar_irradiance_avg, dhi_solar_irradiance_max, 
	dhi_solar_irradiance_min, dhi_solar_irradiance_stdev, dhi_solar_irradiance_count, 
	ghi_solar_irradiance_avg, ghi_solar_irradiance_max, ghi_solar_irradiance_min, 
	ghi_solar_irradiance_stdev, ghi_solar_irradiance_count, eppley_tuvr_voltage_avg, 
	eppley_tuvr_voltage_max, eppley_tuvr_voltage_min, eppley_tuvr_voltage_stdev, 
	eppley_tuvr_voltage_count, dni_voltage_avg, dni_voltage_max, dni_voltage_min, 
	dni_voltage_stdev, dni_voltage_count, pyrgeometer_voltage_avg, pyrgeometer_voltage_max, 
	pyrgeometer_voltage_min, pyrgeometer_voltage_stdev, pyrgeometer_voltage_count, 
	pyrgeometer_ein_solar_ir_avg, pyrgeometer_ein_solar_ir_max, pyrgeometer_ein_solar_ir_min, 
	pyrgeometer_ein_solar_ir_stdev, pyrgeometer_ein_solar_ir_count, pyrgeometer_enet_solar_ir_avg, 
	pyrgeometer_enet_solar_ir_max, pyrgeometer_enet_solar_ir_min, pyrgeometer_enet_solar_ir_stdev, 
	pyrgeometer_enet_solar_ir_count, a1_avg, a1_max, a1_min, a1_stdev, a1_count, a2_avg, a2_max, 
	a2_min, a2_stdev, a2_count, a3_avg, a3_max, a3_min, a3_stdev, a3_count, a4_avg, a4_max, a4_min, 
	a4_stdev, a4_count, a5_avg, a5_max, a5_min, a5_stdev, a5_count, a6_avg, a6_max, a6_min, a6_stdev, 
	a6_count, voltage_avg, voltage_max, voltage_min, current_avg, current_max, current_min, temp_avg, addr)
SELECT logger_id, station_name, tstamp, pyrgeometer_body_temperature_avg, 
pyrgeometer_body_temperature_max, pyrgeometer_body_temperature_min, pyrgeometer_body_temperature_stdev,
pyrgeometer_body_temperature_count, dhi_solar_irradiance_avg, dhi_solar_irradiance_max, 
dhi_solar_irradiance_min, dhi_solar_irradiance_stdev, dhi_solar_irradiance_count, 
ghi_solar_irradiance_avg, ghi_solar_irradiance_max, ghi_solar_irradiance_min, 
ghi_solar_irradiance_stdev, ghi_solar_irradiance_count, eppley_tuvr_voltage_avg, 
eppley_tuvr_voltage_max, eppley_tuvr_voltage_min, eppley_tuvr_voltage_stdev, 
eppley_tuvr_voltage_count, dni_voltage_avg, dni_voltage_max, dni_voltage_min, 
dni_voltage_stdev, dni_voltage_count, pyrgeometer_voltage_avg, pyrgeometer_voltage_max, 
pyrgeometer_voltage_min, pyrgeometer_voltage_stdev, pyrgeometer_voltage_count, 
pyrgeometer_ein_solar_ir_avg, pyrgeometer_ein_solar_ir_max, pyrgeometer_ein_solar_ir_min, 
pyrgeometer_ein_solar_ir_stdev, pyrgeometer_ein_solar_ir_count, pyrgeometer_enet_solar_ir_avg, 
pyrgeometer_enet_solar_ir_max, pyrgeometer_enet_solar_ir_min, pyrgeometer_enet_solar_ir_stdev, 
pyrgeometer_enet_solar_ir_count, a1_avg, a1_max, a1_min, a1_stdev, a1_count, a2_avg, a2_max, 
a2_min, a2_stdev, a2_count, a3_avg, a3_max, a3_min, a3_stdev, a3_count, a4_avg, a4_max, a4_min, 
a4_stdev, a4_count, a5_avg, a5_max, a5_min, a5_stdev, a5_count, a6_avg, a6_max, a6_min, a6_stdev, 
a6_count, voltage_avg, voltage_max, voltage_min, current_avg, current_max, current_min, temp_avg, addr
FROM public.measurements_v1
WHERE tstamp >= CURRENT_DATE - INTERVAL '7 days';