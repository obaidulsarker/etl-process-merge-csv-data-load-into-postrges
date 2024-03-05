DELETE
FROM public.measurements_v1_realtime
WHERE tstamp > CURRENT_DATE - INTERVAL '7 days';