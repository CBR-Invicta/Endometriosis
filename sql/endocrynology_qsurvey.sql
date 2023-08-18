select rqs.id_client , rqs.id_meeting, rqs.id_process, rqs.who, rqs.when, rqs.version, rqs.survey_values, rqs.survey_results, 
rqs.survey_values ->> 'hypothyroidism':: text as hypothyroidism,
rqs.survey_values ->> 'overactive_thyroid':: text as overactive_thyroid,
rqs.survey_values ->> 'thyroiditis':: text as thyroiditis,
rqs.survey_values ->> 'overactive_adrenal_glands':: text as overactive_adrenal_glands,
rqs.survey_values ->> 'hipogonadism':: text as hipogonadism,
rqs.survey_values ->> 'hiperinsulinemy':: text as hiperinsulinemy,
rqs.survey_values ->> 'low_ovarian_reserve':: text as low_ovarian_reserve,
rqs.survey_values ->> 'growth_hormone_deficiency':: text as growth_hormone_deficiency,
rqs.survey_values ->> 'dhes_deficiency':: text as dhes_deficiency,
rqs.survey_values ->> 'testorone_deficiency':: text as testorone_deficiency
from public.report_qualifications_surveys rqs