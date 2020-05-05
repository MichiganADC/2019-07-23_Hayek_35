-- !preview conn=con

SELECT
  ptid,
  form_date,
  mocatots,
  mocaz,
  digforct,
  digforctz,
  digforsl,
  digforspanz,
  digbacct,
  digbacctz,
  digbacls,
  digbacspanz,
  traila_c2,
  traila_c2z,
  trailb_c2,
  trailb_c2z,
  udsbentc,
  udsbentcz,
  udsbentd,
  udsbentdz,
  udsverfc,
  udsverfcz,
  udsverlc,
  udsverlcz,
  mintpcng,
  mintpcngz,
  craftvrs,
  craftvrsz,
  crafturs,
  craftparaz,
  animals_c2,
  animals_c2z,
  veg_c2,
  veg_c2z,
  minttots,
  minttotsz,
  craftdvr,
  craftdvrz,
  craftdre,
  craftdrez,
  -- break --
  fu_mocatots,
  fu_mocaz,
  fu_digforct,
  fu_digforctz,
  fu_digforsl,
  fu_digforspanz,
  fu_digbacct,
  fu_digbacctz,
  fu_digbacls,
  fu_digbacspanz,
  fu_traila_c2,
  fu_traila_c2z,
  fu_trailb_c2,   -- check this
  fu_trailb_c2z,
  fu_udsbentc,    -- check this
  -- fu_udsbentc_c1, -- check this
  fu_udsbentcz,
  fu_udsbentd,    -- check this
  -- fu_udsbentd_c1, -- check this
  fu_udsbentdz,
  fu_udsverfc,    -- check this
  -- fu_udsverfc_c1, -- check this
  fu_udsverfcz,
  fu_udsverlc,    -- check this
  -- fu_udsverlc_c1, -- check this
  fu_udsverlcz,
  fu_mintpcng,
  fu_mintpcngz,
  fu_craftvrs,
  fu_craftvrsz,
  fu_crafturs,
  fu_craftparaz,
  -- fu_animals,     -- check this
  fu_animals_c2,  -- check this
  fu_animals_c2z,
  -- fu_veg,         -- check this
  fu_veg_c2,      -- check this
  fu_veg_c2z,
  fu_minttots,
  fu_minttotsz,
  fu_craftdvr,
  fu_craftdvrz,
  fu_craftdre,
  fu_craftdrez
  -- fu_trailb,      -- check this
FROM public.c1c2
WHERE
  ptid      >= 'UM00000543'::text AND
  form_date >= '2017-03-15'::date
ORDER BY ptid, form_date;
