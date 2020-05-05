-- !preview conn=con

SELECT
  ptid,
  form_date,
  -- mrn,
  dob
FROM public.header_a1
WHERE
  ptid      >= 'UM00000543'::text AND
  form_date >= '2017-03-15'::date
ORDER BY ptid, form_date;
