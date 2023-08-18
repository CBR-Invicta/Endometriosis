case
	when rqs.survey_values ->> 'idiopathic' = '0' then true
	when rqs.survey_values ->> 'idiopathic' = '1' then false
	else null
end nieplodnosc_idiopatyczna,
case
	when rqs.survey_values ->> 'ovarian_factor' = '0' then true
	when rqs.survey_values ->> 'ovarian_factor' = '1' then false
	else null
end czynnik_jajowodowy,
case
	when rqs.survey_values ->> 'pco' = '0' then true
	when rqs.survey_values ->> 'pco' = '1' then false
	else null
end pco,
case
	when (
		rqs.survey_values ->> 'no_wiggling_sperm' = '0'
		or rqs.survey_values ->> 'no_alive_sperm' = '0'
		or rqs.survey_values ->> 'sperm_density_in_a_partner' = any(array ['>15', ' ml'])
	) is true then true
	else false
end czynnik_meski,
--sprawdzic '3-15'
case
	when (
		rqs.survey_values ->> 'her_cariotype' = '1'
		or rqs.survey_values ->> 'her_translocation' = '0'
	) is true then true
	else false
end genetic_female,
case
	when (
		rqs.survey_values ->> 'his_cariotype' = '1'
		or rqs.survey_values ->> 'his_translocation' = '0'
	) is true then true
	else false
end genetic_male,
case
	when (
		rqs.survey_values ->> 'overactive_thyroid' = '0'
		or rqs.survey_values ->> 'hypothyroidism' = '0'
		or rqs.survey_values ->> 'thyroiditis' = '0'
	) then true
	when (
		rqs.survey_values ->> 'overactive_thyroid' = '1'
		and rqs.survey_values ->> 'hypothyroidism' = '1'
		and rqs.survey_values ->> 'thyroiditis' = '1'
	) then false
	else null
end endocrine_thyroid,
case
	when (
		rqs.survey_values ->> 'overactive_adrenal_glands' = '0'
		or rqs.survey_values ->> 'hiperinsulinemy' = '0'
		or rqs.survey_values ->> 'hipogonadism' = '0'
		or rqs.survey_values ->> 'growth_hormone_deficiency' = '0'
		or rqs.survey_values ->> 'testorone_deficiency' = '0'
		or rqs.survey_values ->> 'dhes_deficiency' = '0'
	) then true
	when (
		rqs.survey_values ->> 'overactive_adrenal_glands' = '1'
		and rqs.survey_values ->> 'hiperinsulinemy' = '1'
		and rqs.survey_values ->> 'hipogonadism' = '1'
		and rqs.survey_values ->> 'growth_hormone_deficiency' = '1'
		and rqs.survey_values ->> 'testorone_deficiency' = '1'
		and rqs.survey_values ->> 'dhes_deficiency' = '1'
	) then false
	else null
end endocrine_other,
case
	when rqs.survey_values ->> 'immunological_disorder' = '0' then true
	when rqs.survey_values ->> 'immunological_disorder' = '1' then false
	else null
end immuno,
case
	when rqs.survey_values ->> 'poi' = '0' then true
	when rqs.survey_values ->> 'poi' = '1' then false
	else null
end poi,
case
	when rqs.survey_values ->> 'endometriosis' = '0' then true
	when rqs.survey_values ->> 'endometriosis' = '1' then false
	else null
end endometriosis,
case
	when rqs.survey_values ->> 'low_ovarian_reserves' = '0' then true
	when rqs.survey_values ->> 'low_ovarian_reserve' = '1' then false
	else null
end low_ovar_res,
case
	--Nawracające poronienia i braki implantacji
	when (
		rqs.survey_values ->> 'recurrent_pregnancy_loss_in_relationship' = '0'
		or rqs.survey_values ->> 'earlier_lack_of_implantation' = '0'
	) then true
	when (
		rqs.survey_values ->> 'recurrent_pregnancy_loss_in_relationship' = '1'
		or rqs.survey_values ->> 'earlier_lack_of_implantation' = '1'
	) then false
	else null
end rpl_rif,
case
	when rqs.survey_values ->> 'sperm_density_in_a_partner' = '> 15 ml' then true
	else false
end sperm_density_in_a_partner,
case
	when rqs.survey_values ->> 'no_wiggling_sperm' = '0' then true
	when rqs.survey_values ->> 'no_wiggling_sperm' = '1' then false
	else null
end no_wiggling_sperm,
case
	when rqs.survey_values ->> 'no_alive_sperm' = '0' then true
	when rqs.survey_values ->> 'no_alive_sperm' = '1' then false
	else null
end no_alive_sperm,
case
	when rqs.survey_values ->> 'high_sperm_fragmentation' = '413' then true
	else false
end high_sperm_fragmentation,
case
	when rqs.survey_values ->> 'low_hialuron' = '0' then true
	when rqs.survey_values ->> 'low_hialuron' = '1' then false
	else null
end low_hialuron,
case
	when rqs.survey_values ->> 'high_oxydation_stres' = '0' then true
	when rqs.survey_values ->> 'high_oxydation_stres' = '1' then false
	else null
end high_oxydation_stres,
case
	when rqs.survey_values ->> 'no_sperm_in_testicle' = '0' then true
	when rqs.survey_values ->> 'no_sperm_in_testicle' = '1' then false
	else null
end no_sperm_in_testicle,
case
	when rqs.survey_values ->> 'no_liquid_sperm' = '0' then true
	when rqs.survey_values ->> 'no_liquid_sperm' = '1' then false
	else null
end no_liquid_sperm,
case
	when rqs.survey_values ->> 'wrong_intercourse_test' = '0' then true
	when rqs.survey_values ->> 'wrong_intercourse_test' = '1' then false
	else null
end wrong_intercourse_test,
case
	when rqs.survey_values ->> 'azoospermy' = '0' then true
	when rqs.survey_values ->> 'azoospermy' = '1' then false
	else null
end azoospermy