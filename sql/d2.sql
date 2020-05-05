-- !preview conn=con

SELECT
  *
FROM public.d2
WHERE
  ptid      >= 'UM00000543'::text AND
  form_date >= '2017-03-15'::date
ORDER BY ptid, form_date;