-- !preview conn=con

SELECT
  ptid,
  form_date,
  mocaz,
  digforctz,
  digforspanz,
  digbacctz,
  digbacspanz,
  traila_c2z,
  udsbentcz,
  udsverfcz,
  udsverlcz,
  mintpcngz,
  craftvrsz,
  craftparaz,
  animals_c2z,
  veg_c2z,
  minttotsz,
  craftdvrz,
  craftdrez,
  trailb_c2z
FROM public.c1c2
WHERE
  ptid      >= 'UM00000543'::text AND
  form_date >= '2017-03-15'::date
ORDER BY ptid, form_date;
