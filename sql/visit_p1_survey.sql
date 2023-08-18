/*
 zwraca odpowiedzi dla ankiet pierwszorazowych
 */
-- platforma.vm_p1_survey_wizyty source
CREATE MATERIALIZED VIEW platforma.vm_p1_survey_wizyty TABLESPACE pg_default AS WITH tlumaczenia_slownik AS (
	SELECT
		t.key,
		t.value ->> 'PL' :: text AS pytanie_pl
	FROM
		platforma.form_translation t
),
pytania_slownik AS (
	SELECT
		qtt.id,
		qtt.name,
		qq.stamp,
		qq.data,
		CASE
			WHEN qtt.name = 'SELECT_ONEINPUT' :: text THEN (
				(qq.data -> 'select' :: text) -> 'translation' :: text
			) ->> 'label' :: text
			WHEN qtt.name = 'RADIO_INPUT' :: text THEN (
				(qq.data -> 'radio' :: text) -> 'translation' :: text
			) ->> 'label' :: text
			WHEN qtt.name = 'DOUBLE_RADIOINPUT' :: text THEN (
				(
					(qq.data -> 'question1st' :: text) -> 'radio' :: text
				) -> 'translation' :: text
			) ->> 'label' :: text
			WHEN qtt.name = 'RADIO_MULTIPLEINPUT' :: text THEN (
				(qq.data -> 'radio' :: text) -> 'translation' :: text
			) ->> 'label' :: text
			WHEN qtt.name = 'SIMPLETYPE_CHECKBOX' :: text THEN (
				(qq.data -> 'value' :: text) -> 'translation' :: text
			) ->> 'label' :: text
			WHEN qtt.name = 'DATE_CHECKBOX' :: text THEN ((qq.data -> 'date' :: text) -> 'translation' :: text) ->> 'label' :: text
			WHEN qtt.name = 'NUMBER_DEPENDENTGRID' :: text THEN (
				(qq.data -> 'question1st' :: text) -> 'translation' :: text
			) ->> 'label' :: text
			WHEN qtt.name = 'NUMBER_DEPENDENTGRID' :: text THEN (
				(qq.data -> 'question1st' :: text) -> 'translation' :: text
			) ->> 'label' :: text
			WHEN qtt.name = 'EXTENDABLE_GRID' :: text
			AND (
				(
					(
						(qq.data -> 'rows' :: text) -> 'OTHER_ADDRESS' :: text
					) -> 'translation' :: text
				) ->> 'label' :: text
			) IS NOT NULL THEN (
				(
					(qq.data -> 'rows' :: text) -> 'OTHER_ADDRESS' :: text
				) -> 'translation' :: text
			) ->> 'label' :: text
			WHEN qtt.name = 'EXTENDABLE_GRID' :: text
			AND (
				(
					(
						(qq.data -> 'rows' :: text) -> 'OTHER_ADDRESS' :: text
					) -> 'translation' :: text
				) ->> 'label' :: text
			) IS NULL THEN (qq.data -> 'translation' :: text) ->> 'label' :: text
			ELSE (qq.data -> 'translation' :: text) ->> 'label' :: text
		END AS pytanie_eng
	FROM
		platforma.form_question qq
		JOIN platforma.form_question_type qtt ON qtt.id = qq.id_question_type
)
SELECT
	wws.wizyta_id,
	wws.patient_id,
	wws.p1_visit_date,
	wws.receiver_id,
	wws.ankieta_uuid,
	wws.insert_time,
	wws.id_form,
	wws.id_answer,
	wws.answer_data,
	wws.answer_uuid,
	wws.id_question,
	wws.response_date,
	q.stamp,
	q.id_section,
	qt.id AS id_question_type,
	qt.name AS typ_pytania,
	s.name AS section_question,
	jsonb_pretty(q.data) AS json_pyt,
	jsonb_pretty(wws.answer_data) AS json_odp,
	pyta.pytanie_eng,
	trsl.pytanie_pl,
	CASE
		WHEN qt.name = 'TEXT' :: text THEN wws.answer_data :: text
		WHEN qt.name = 'DATE' :: text THEN wws.answer_data :: text
		WHEN qt.name = 'RADIO' :: text THEN wws.answer_data :: text
		WHEN qt.name = 'SELECT_ONE' :: text THEN wws.answer_data :: text
		WHEN qt.name = 'NUMBER' :: text THEN wws.answer_data :: text
		WHEN qt.name = 'CHECKBOX' :: text THEN wws.answer_data :: text
		WHEN qt.name = 'SIMPLETYPE_CHECKBOX' :: text THEN ROW(
			wws.answer_data ->> 'value' :: text,
			wws.answer_data ->> 'checkbox' :: text
		) :: text
		WHEN qt.name = 'SELECT_ONEINPUT' :: text THEN (
			(wws.answer_data ->> 'select' :: text) || ';' :: text
		) || (wws.answer_data ->> 'questions' :: text)
		WHEN qt.name = 'RADIO_INPUT' :: text
		AND (
			(wws.answer_data -> 'input' :: text) ->> 'defined' :: text
		) <> '{}' :: text THEN (wws.answer_data -> 'input' :: text) ->> 'defined' :: text
		WHEN qt.name = 'RADIO_INPUT' :: text
		AND (wws.answer_data ->> 'radio' :: text) = 'NO' :: text THEN wws.answer_data ->> 'radio' :: text
		WHEN qt.name = 'RADIO_INPUT' :: text
		AND (
			(wws.answer_data -> 'input' :: text) ->> 'user' :: text
		) IS NOT NULL THEN (wws.answer_data -> 'input' :: text) ->> 'user' :: text
		WHEN qt.name = 'RADIO_INPUT' :: text
		AND (
			(wws.answer_data -> 'input' :: text) ->> 'input' :: text
		) IS NOT NULL THEN ARRAY [(wws.answer_data -> 'input'::text) -> 'input'::text] :: text
		WHEN qt.name = 'RADIO_INPUT' :: text
		AND (wws.answer_data ->> 'input' :: text) IS NOT NULL THEN wws.answer_data ->> 'input' :: text
		WHEN qt.name = 'RADIO_INPUT' :: text
		AND (wws.answer_data ->> 'radio' :: text) = 'YES' :: text THEN wws.answer_data ->> 'radio' :: text
		WHEN qt.name = 'RADIO_INPUT' :: text
		AND (wws.answer_data ->> 'radio' :: text) = 'DONT_REMEMBER' :: text THEN wws.answer_data ->> 'radio' :: text
		WHEN qt.name = 'RADIO_MULTIPLEINPUT' :: text
		AND (wws.answer_data ->> 'radio' :: text) = 'NO' :: text THEN wws.answer_data ->> 'radio' :: text
		WHEN qt.name = 'RADIO_MULTIPLEINPUT' :: text
		AND (wws.answer_data ->> 'radio' :: text) = 'YES' :: text THEN wws.answer_data ->> 'questions' :: text
		WHEN qt.name = 'RADIO_MULTIPLEINPUT' :: text
		AND (
			(wws.answer_data ->> 'radio' :: text) = ANY (ARRAY ['LIVE'::text, 'NOT_LIVE'::text])
		) THEN (
			(wws.answer_data ->> 'radio' :: text) || ' : ' :: text
		) || (wws.answer_data ->> 'questions' :: text)
		WHEN qt.name = 'RADIO_MULTIPLEINPUT' :: text
		AND (wws.answer_data ->> 'radio' :: text) IS NOT NULL THEN (
			(wws.answer_data ->> 'radio' :: text) || ' : ' :: text
		) || (wws.answer_data ->> 'questions' :: text)
		WHEN qt.name = 'CHECKBOX_INPUT' :: text THEN wws.answer_data :: text
		WHEN qt.name = 'DATE_CHECKBOX' :: text THEN ((wws.answer_data ->> 'date' :: text) || ':' :: text) || (wws.answer_data -> 'checkbox' :: text)
		WHEN qt.name = 'DOUBLE_RADIOINPUT' :: text THEN (
			(wws.answer_data ->> 'question1st' :: text) || ',' :: text
		) || (wws.answer_data ->> 'question2nd' :: text)
		WHEN qt.name = 'NUMBER_DEPENDENTGRID' :: text THEN wws.answer_data :: text
		WHEN qt.name = 'EXTENDABLE_GRID' :: text
		AND (
			pyta.pytanie_eng = ANY (
				ARRAY ['WO_F__NARCOTICS_USED__LABEL'::text, 'WO_M__NARCOTICS_USED__LABEL'::text]
			)
		) THEN (wws.answer_data -> 'defined' :: text) :: text
		WHEN qt.name = 'EXTENDABLE_GRID' :: text
		AND (wws.answer_data ->> 'checked' :: text) <> '[]' :: text THEN (wws.answer_data -> 'defined' :: text) ->> 'OTHER_ADDRESS' :: text
		ELSE NULL :: text
	END AS odpowiedzi
FROM
	platforma.wizyta_with_survey wws
	JOIN platforma.form_question q ON q.id = wws.id_question
	JOIN platforma.form_question_type qt ON qt.id = q.id_question_type
	JOIN platforma.form_section s ON s.id = q.id_section
	LEFT JOIN platforma.form_translation tr ON tr.key = replace(
		((q.data -> 'translation' :: text) -> 'label' :: text) :: text,
		'"' :: text,
		'' :: text
	)
	LEFT JOIN pytania_slownik pyta ON pyta.stamp = q.stamp
	LEFT JOIN tlumaczenia_slownik trsl ON trsl.key = pyta.pytanie_eng
WHERE
	pyta.pytanie_eng <> ALL (
		ARRAY ['WO_F__LAST_NAME__LABEL'::text, 'WO_M__LAST_NAME__LABEL'::text, 'WO_F__FIRST_NAME__LABEL'::text, 'WO_M__FIRST_NAME__LABEL'::text, 'WO_F__PESEL__LABEL'::text, 'WO_M__PESEL__LABEL'::text, 'WO_F__PHONE_NUMBER__LABEL'::text, 'WO_M__PHONE_NUMBER__LABEL'::text, 'WO_F__EMAIL__LABEL'::text, 'WO_M__EMAIL__LABEL'::text, 'WO_F__IDENTITY_DOCUMENT_NUMBER__LABEL'::text, 'WO_M__IDENTITY_DOCUMENT_NUMBER__LABEL'::text, 'WO_F__ADDRESS_NUMBER__LABEL'::text, 'WO_M__ADDRESS_NUMBER__LABEL'::text, 'WO_F__ADDRESS_STREET__LABEL'::text, 'WO_M__ADDRESS_STREET__LABEL'::text, 'WO_F__IDENTITY_DOCUMENT_TYPE__LABEL'::text, 'WO_M__IDENTITY_DOCUMENT_TYPE__LABEL'::text, 'WO_F__SECOND_NAME__LABEL'::text, 'WO_M__SECOND_NAME__LABEL'::text, 'WO_M__ALTERNATIVE_EMAIL__LABEL'::text, 'WO_F__ALTERNATIVE_EMAIL__LABEL'::text, 'COMMON__LAST_NAME__LABEL'::text, 'COMMON__FIRST_NAME__LABEL'::text, 'WO_M__ALTERNATIVE_PHONE_NUMBER__LABEL'::text, 'WO_F__ALTERNATIVE_PHONE_NUMBER__LABEL'::text, 'COMMON__PHONE_NUMBER__LABEL'::text, 'COMMON__PHONE_NUMBER__LABEL'::text, 'COMMON__FIRST_NAME__LABEL'::text, 'COMMON__LAST_NAME__LABEL'::text]
	)
ORDER BY
	wws.wizyta_id,
	wws.id_form WITH DATA;