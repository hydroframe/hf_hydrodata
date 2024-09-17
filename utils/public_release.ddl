
BEGIN TRANSACTION;
DROP TABLE IF EXISTS public.entry_dates;
DROP TABLE IF EXISTS public.dataset_dates;
SELECT id, entry_start_date, entry_end_date INTO public.entry_dates FROM public.data_catalog_entry;
SELECT id, dataset_start_date, dataset_end_date INTO public.dataset_dates FROM public.dataset;

DELETE FROM public.data_catalog_entry;
DELETE FROM public.dataset;
DELETE FROM public.datasource;
DELETE FROM public.temporal_resolution;
DELETE FROM public.dataset_type;
DELETE FROM public.variable;
DELETE FROM public.variable_type;
DELETE FROM public.units;
DELETE FROM public.unit_type;
DELETE FROM public.aggregation;
DELETE FROM public.file_type;
DELETE FROM public.grid;
DELETE FROM public.security_level;
DELETE FROM public.site_type;
DELETE FROM public.structure_type;
DELETE FROM public.substitution_keys;

INSERT INTO public.unit_type SELECT * from development.unit_type;
INSERT INTO public.units SELECT * from development.units;
INSERT INTO public.variable_type SELECT * from development.variable_type;
INSERT INTO public.variable SELECT * from development.variable;
INSERT INTO public.temporal_resolution SELECT * from development.temporal_resolution;
INSERT INTO public.datasource SELECT * from development.datasource;
INSERT INTO public.structure_type SELECT * from development.structure_type;
INSERT INTO public.dataset_type SELECT * from development.dataset_type;
INSERT INTO public.dataset SELECT * from development.dataset;
INSERT INTO public.file_type SELECT * from development.file_type;
INSERT INTO public.aggregation SELECT * from development.aggregation;
INSERT INTO public.grid SELECT * from development.grid;
INSERT INTO public.security_level SELECT * from development.security_level;
INSERT INTO public.site_type SELECT * from development.site_type;
INSERT INTO public.substitution_keys SELECT * FROM development.substitution_keys;
INSERT INTO public.data_catalog_entry SELECT * from development.data_catalog_entry;

UPDATE public.data_catalog_entry 
SET entry_start_date=entry_dates.entry_start_date,
    entry_end_date=entry_dates.entry_end_date
FROM public.entry_dates 
WHERE public.data_catalog_entry.id = entry_dates.id;

UPDATE public.dataset 
SET dataset_start_date=dataset_dates.dataset_start_date,
    dataset_end_date=dataset_dates.dataset_end_date
FROM public.dataset_dates 
WHERE public.dataset.id = dataset_dates.id;

DROP TABLE IF EXISTS public.entry_dates;
DROP TABLE IF EXISTS public.dataset_dates;
COMMIT;