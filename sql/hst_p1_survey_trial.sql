
select f.uuid, f.id, qt."name",--an.id, an.uuid,
an."data" ,
--jsonb_object_keys(an."data") 
--jsonb_each_text(an."data") 
jsonb_pretty(an."data"),
case
when qt."name"= 'TEXT' then an.data::text 
when qt."name"= 'DATE' then an.data::text 
when qt."name"= 'RADIO' then (an.data)::text 
--when qt."name"= 'RADIO_INPUT'then (an.data -> 'radio')::text
when qt."name"= 'SELECT_ONE' then (an.data)::text 
when qt."name"= 'NUMBER' then (an.data)::text 
when qt."name"= 'CHECKBOX' then (jsonb_array_elements(an.data))::text --unnest
when qt."name"= 'SIMPLETYPE_CHECKBOX' then unnest(array[an.data ->> 'value'::text, an.data->>  'checkbox'::text])::text
--when qt."name"= 'SIMPLETYPE_CHECKBOX' then (an.data ->> 'value')::text || ';' || (an.data->>  'checkbox')::text
when qt."name"= 'SELECT_ONEINPUT' then (an.data ->> 'select')::text ||';' || (an.data->>  'questions')::text

--RADIO INPUT
when qt."name"= 'RADIO_INPUT' and (an.data -> 'input'->> 'defined' != '{}') then (an.data -> 'input' ->> 'defined' )::text
--when qt."name"= 'RADIO_INPUT' and (an.data ->> 'radio')::text = 'YES' then an.data ->> 'radio'::text
when qt."name"= 'RADIO_INPUT' and (an.data ->> 'radio')::text = 'NO' then an.data ->> 'radio'::text
--when qt."name"= 'RADIO_INPUT' and (an.data -> 'input' ->> 'user' is not null) then replace(replace((an.data -> 'input' -> 'user')::text, '[', '')::text, ']', '')::text
when qt."name"= 'RADIO_INPUT' and (an.data -> 'input' ->> 'user' is not null) then (an.data -> 'input' ->> 'user')::text
when qt."name"= 'RADIO_INPUT' and (an.data -> 'input' ->> 'input' is not null) then (unnest(array[an.data -> 'input' -> 'input']))::text
when qt."name"= 'RADIO_INPUT' and (an.data ->> 'input'  is not null) then (an.data ->> 'input' )::text
when qt."name"= 'RADIO_INPUT' and (an.data ->> 'radio')::text = 'YES' then an.data ->> 'radio'::text

--RADIO_MULTIPLEINPUT
when qt."name"= 'RADIO_MULTIPLEINPUT' and (an.data ->> 'radio')::text = 'NO' then an.data ->> 'radio'::text
when qt."name"= 'RADIO_MULTIPLEINPUT' and (an.data ->> 'radio')::text = 'YES' then an.data ->> 'questions'::text

when qt."name"= 'CHECKBOX_INPUT'  then an.data ::text
when qt."name"= 'DATE_CHECKBOX'  then (array[(an.data ->>'date'::text) || ':' ||(an.data ->'checkbox' ::text )])::text
when qt."name"= 'DOUBLE_RADIOINPUT'  then unnest(array[(an.data ->>'question1st'::text) || ',' ||(an.data ->>'question2nd' ::text )])::text
when qt."name"= 'NUMBER_DEPENDENTGRID'  then unnest(array[(an.data::text)])::text
--when qt."name"= 'EXTENDABLE_GRID' and (an.data -> 'user' ->> 'checked') = 'OTHER_ADDRESS' then 'asdsad' -- (an.data -> 'user' -> 'defined' -> 'OTHER_ADDRESS')::text
when qt."name"= 'EXTENDABLE_GRID' and (an.data ->> 'checked') != '[]' then unnest(array[an.data -> 'defined'->> 'OTHER_ADDRESS'::text])::text
else 
null end odpowiedzi
from form.form f
    JOIN form.form_type AS ft ON f.id_form_type = ft.id
    join form.answer as an on an.id_form = f.id
    join form.question q on q.id = an.id_question 
    join form.question_type qt on qt.id = q.id_question_type 
    join form.section s on s.id = q.id_section 
--  where 
--  f.id = 30276 and
--  where f.uuid =  'e717353f-19a7-4381-874b-4660373ac9c8' 
--and  
--qt."name"= 'EXTENDABLE_GRID'
--and qt.id=13
order by q.id_section 

    
    



select f.uuid, f.id, ft.id, ft."name", an.id, an.uuid, an.id_form , 
 q.uuid as uuid_pytanie, q.stamp, an.id_question, q.id_section, qt.id, qt."name",
s."name",
an."data", an.creation_date, an."version", 
--jsonb_to_record(an."data") ,


(q.data -> 'translation' ->> 'label') as label,
tr.value ->> 'PL' as translated,

case 
    when qt.id=any(array[1,2,3,4,5,8,9,10,11,14]) then replace((q.data -> 'translation' -> 'label')::text, '"', '')
    when qt.id = 6 and (an."data" -> 'radio')::text is not null 
        then replace((q.data -> 'input' -> 'translation' -> 'label')::text, '"', '')
    when qt.id = 7 and (an."data" -> 'question1st' -> 'radio')::text is not null
        then replace((q.data -> 'question1st' -> 'input' ->'translation' -> 'label')::text, '"', '')
    --7 nr 100 dopracowac
        --when qt.id = 7 and (an."data" -> 'question1st' -> 'radio')::text is not null
        --then replace((q.data -> 'question1st' ->'translation' -> 'label')::text, '"', '')
    when qt.id = 15 --and (an."data" -> 'question1st' -> 'radio')::text is not null
        then replace((q.data -> 'select' -> 'translation' -> 'label')::text, '"', '')
    when qt.id = 16 --and (an."data" -> 'question1st' -> 'radio')::text is not null
        then replace((q.data -> 'value' -> 'translation' -> 'label')::text, '"', '')
    else null
    end trial

from form.form f
    JOIN form.form_type AS ft ON f.id_form_type = ft.id
    join form.answer as an on an.id_form = f.id
    join form.question q on q.id = an.id_question 
    join form.question_type qt on qt.id = q.id_question_type 
    join form.section s on s.id = q.id_section 
    left join form.translation tr on tr."key" = replace((q.data -> 'translation' -> 'label')::text, '"', '')
    --JOIN form."section" AS s ON s.id_form_type = ft.id
where f.uuid =  'e717353f-19a7-4381-874b-4660373ac9c8' 
--and qt.id=13
order by q.id_section 



-----


select f.uuid as uuid_ankieta, f.id as ankieta_id, an.id_form ,
an.id pytanie_id, an.uuid as uuid_pytanie, 
q.stamp, an.id_question, q.id_section,

qt."name" as typ_pytania, s."name" as section_question,
jsonb_pretty(q.data),
(q.data -> 'translation' ->> 'label') as label,
case 
	when qt."name" = 'SELECT_ONEINPUT' then (q.data -> 'select' -> 'translation' ->> 'label')
else (q.data -> 'translation' ->> 'label')
end poprawa,

tr.value ->> 'PL' as translated,
an."data" ,
--jsonb_object_keys(an."data") 
--jsonb_each_text(an."data") 
jsonb_pretty(an."data"),
case
when qt."name"= 'TEXT' then an.data::text 
when qt."name"= 'DATE' then an.data::text 
when qt."name"= 'RADIO' then (an.data)::text 
--when qt."name"= 'RADIO_INPUT'then (an.data -> 'radio')::text
when qt."name"= 'SELECT_ONE' then (an.data)::text 
when qt."name"= 'NUMBER' then (an.data)::text 
when qt."name"= 'CHECKBOX' then (jsonb_array_elements(an.data))::text --unnest
when qt."name"= 'SIMPLETYPE_CHECKBOX' then unnest(array[an.data ->> 'value'::text, an.data->>  'checkbox'::text])::text
--when qt."name"= 'SIMPLETYPE_CHECKBOX' then (an.data ->> 'value')::text || ';' || (an.data->>  'checkbox')::text
when qt."name"= 'SELECT_ONEINPUT' then (an.data ->> 'select')::text ||';' || (an.data->>  'questions')::text

--RADIO INPUT
when qt."name"= 'RADIO_INPUT' and (an.data -> 'input'->> 'defined' != '{}') then (an.data -> 'input' ->> 'defined' )::text
--when qt."name"= 'RADIO_INPUT' and (an.data ->> 'radio')::text = 'YES' then an.data ->> 'radio'::text
when qt."name"= 'RADIO_INPUT' and (an.data ->> 'radio')::text = 'NO' then an.data ->> 'radio'::text
--when qt."name"= 'RADIO_INPUT' and (an.data -> 'input' ->> 'user' is not null) then replace(replace((an.data -> 'input' -> 'user')::text, '[', '')::text, ']', '')::text
when qt."name"= 'RADIO_INPUT' and (an.data -> 'input' ->> 'user' is not null) then (an.data -> 'input' ->> 'user')::text
when qt."name"= 'RADIO_INPUT' and (an.data -> 'input' ->> 'input' is not null) then (unnest(array[an.data -> 'input' -> 'input']))::text
when qt."name"= 'RADIO_INPUT' and (an.data ->> 'input'  is not null) then (an.data ->> 'input' )::text
when qt."name"= 'RADIO_INPUT' and (an.data ->> 'radio')::text = 'YES' then an.data ->> 'radio'::text

--RADIO_MULTIPLEINPUT
when qt."name"= 'RADIO_MULTIPLEINPUT' and (an.data ->> 'radio')::text = 'NO' then an.data ->> 'radio'::text
when qt."name"= 'RADIO_MULTIPLEINPUT' and (an.data ->> 'radio')::text = 'YES' then an.data ->> 'questions'::text

when qt."name"= 'CHECKBOX_INPUT'  then an.data ::text
when qt."name"= 'DATE_CHECKBOX'  then (array[(an.data ->>'date'::text) || ':' ||(an.data ->'checkbox' ::text )])::text
when qt."name"= 'DOUBLE_RADIOINPUT'  then unnest(array[(an.data ->>'question1st'::text) || ',' ||(an.data ->>'question2nd' ::text )])::text
when qt."name"= 'NUMBER_DEPENDENTGRID'  then unnest(array[(an.data::text)])::text
--when qt."name"= 'EXTENDABLE_GRID' and (an.data -> 'user' ->> 'checked') = 'OTHER_ADDRESS' then 'asdsad' -- (an.data -> 'user' -> 'defined' -> 'OTHER_ADDRESS')::text
when qt."name"= 'EXTENDABLE_GRID' and (an.data ->> 'checked') != '[]' then unnest(array[an.data -> 'defined'->> 'OTHER_ADDRESS'::text])::text
else 
null end odpowiedzi
from form.form f
	JOIN form.form_type AS ft ON f.id_form_type = ft.id
	join form.answer as an on an.id_form = f.id
	join form.question q on q.id = an.id_question 
	join form.question_type qt on qt.id = q.id_question_type 
	join form.section s on s.id = q.id_section 
	left join form.translation tr on tr."key" = replace((q.data -> 'translation' -> 'label')::text, '"', '')
--	where 
--	f.id = 30276 and
	where f.uuid =  'e717353f-19a7-4381-874b-4660373ac9c8' 
--and  
--qt."name"= 'EXTENDABLE_GRID'
--and qt.id=13
order by q.id_section 
with tlumaczenia_slownik as (   

select 


)


select f.uuid as uuid_ankieta, f.id as ankieta_id, an.id_form ,
an.id pytanie_id, an.uuid as uuid_pytanie, 
q.stamp, an.id_question, q.id_section,

qt."name" as typ_pytania, s."name" as section_question,
jsonb_pretty(q.data),
(q.data -> 'translation' ->> 'label') as label,
case 
	when qt."name" = 'SELECT_ONEINPUT' then (q.data -> 'select' -> 'translation' ->> 'label')
	when qt."name" = 'RADIO_INPUT' then (q.data -> 'radio' -> 'translation' ->> 'label')
	when qt."name" = 'DOUBLE_RADIOINPUT' then (q.data -> 'question1st' -> 'radio' -> 'translation' ->> 'label')
	when qt."name" = 'RADIO_MULTIPLEINPUT' then (q.data -> 'radio' -> 'translation' ->> 'label')
	when qt."name" = 'SIMPLETYPE_CHECKBOX' then (q.data -> 'value' -> 'translation' ->> 'label')
	when qt."name" = 'DATE_CHECKBOX' then (q.data -> 'date' -> 'translation' ->> 'label')
	when qt."name" = 'NUMBER_DEPENDENTGRID' then (q.data -> 'question1st' -> 'translation' ->> 'label') --tutaj jest wiele pod pytan
	when qt."name" = 'NUMBER_DEPENDENTGRID' then (q.data -> 'question1st' -> 'translation' ->> 'label')
	when qt."name" = 'EXTENDABLE_GRID' then (q.data -> 'rows' -> 'OTHER_ADDRESS' -> 'translation' ->> 'label')
else (q.data -> 'translation' ->> 'label')
end poprawa,

tr.value ->> 'PL' as translated,
an."data" ,
--jsonb_object_keys(an."data") 
--jsonb_each_text(an."data") 
jsonb_pretty(an."data"),
case
when qt."name"= 'TEXT' then an.data::text 
when qt."name"= 'DATE' then an.data::text 
when qt."name"= 'RADIO' then (an.data)::text 
--when qt."name"= 'RADIO_INPUT'then (an.data -> 'radio')::text
when qt."name"= 'SELECT_ONE' then (an.data)::text 
when qt."name"= 'NUMBER' then (an.data)::text 
when qt."name"= 'CHECKBOX' then (jsonb_array_elements(an.data))::text --unnest
when qt."name"= 'SIMPLETYPE_CHECKBOX' then unnest(array[an.data ->> 'value'::text, an.data->>  'checkbox'::text])::text
--when qt."name"= 'SIMPLETYPE_CHECKBOX' then (an.data ->> 'value')::text || ';' || (an.data->>  'checkbox')::text
when qt."name"= 'SELECT_ONEINPUT' then (an.data ->> 'select')::text ||';' || (an.data->>  'questions')::text

--RADIO INPUT
when qt."name"= 'RADIO_INPUT' and (an.data -> 'input'->> 'defined' != '{}') then (an.data -> 'input' ->> 'defined' )::text
--when qt."name"= 'RADIO_INPUT' and (an.data ->> 'radio')::text = 'YES' then an.data ->> 'radio'::text
when qt."name"= 'RADIO_INPUT' and (an.data ->> 'radio')::text = 'NO' then an.data ->> 'radio'::text
--when qt."name"= 'RADIO_INPUT' and (an.data -> 'input' ->> 'user' is not null) then replace(replace((an.data -> 'input' -> 'user')::text, '[', '')::text, ']', '')::text
when qt."name"= 'RADIO_INPUT' and (an.data -> 'input' ->> 'user' is not null) then (an.data -> 'input' ->> 'user')::text
when qt."name"= 'RADIO_INPUT' and (an.data -> 'input' ->> 'input' is not null) then (unnest(array[an.data -> 'input' -> 'input']))::text
when qt."name"= 'RADIO_INPUT' and (an.data ->> 'input'  is not null) then (an.data ->> 'input' )::text
when qt."name"= 'RADIO_INPUT' and (an.data ->> 'radio')::text = 'YES' then an.data ->> 'radio'::text

--RADIO_MULTIPLEINPUT
when qt."name"= 'RADIO_MULTIPLEINPUT' and (an.data ->> 'radio')::text = 'NO' then an.data ->> 'radio'::text
when qt."name"= 'RADIO_MULTIPLEINPUT' and (an.data ->> 'radio')::text = 'YES' then an.data ->> 'questions'::text

when qt."name"= 'CHECKBOX_INPUT'  then an.data ::text
when qt."name"= 'DATE_CHECKBOX'  then (array[(an.data ->>'date'::text) || ':' ||(an.data ->'checkbox' ::text )])::text
when qt."name"= 'DOUBLE_RADIOINPUT'  then unnest(array[(an.data ->>'question1st'::text) || ',' ||(an.data ->>'question2nd' ::text )])::text
when qt."name"= 'NUMBER_DEPENDENTGRID'  then unnest(array[(an.data::text)])::text
--when qt."name"= 'EXTENDABLE_GRID' and (an.data -> 'user' ->> 'checked') = 'OTHER_ADDRESS' then 'asdsad' -- (an.data -> 'user' -> 'defined' -> 'OTHER_ADDRESS')::text
when qt."name"= 'EXTENDABLE_GRID' and (an.data ->> 'checked') != '[]' then unnest(array[an.data -> 'defined'->> 'OTHER_ADDRESS'::text])::text
else 
null end odpowiedzi
from form.form f
	JOIN form.form_type AS ft ON f.id_form_type = ft.id
	join form.answer as an on an.id_form = f.id
	join form.question q on q.id = an.id_question 
	join form.question_type qt on qt.id = q.id_question_type 
	join form.section s on s.id = q.id_section 
	left join form.translation tr on tr."key" = replace((q.data -> 'translation' -> 'label')::text, '"', '')
--	where 
--	f.id = 30276 and
	--where f.uuid =  'e717353f-19a7-4381-874b-4660373ac9c8' 
--and  
--qt."name"= 'EXTENDABLE_GRID'
--and qt.id=13
order by q.id_section 
with tlumaczenia_slownik as (   
	select t."key", t.value ->> 'PL' as pytanie_pl
	from form."translation" t 
	
	),

	pytania_slownik as ( 
	
		select qtt.id,qtt."name",qq.stamp, qq.data,
		case 
			when qtt."name" = 'SELECT_ONEINPUT' then (qq.data -> 'select' -> 'translation' ->> 'label')
			when qtt."name" = 'RADIO_INPUT' then (qq.data -> 'radio' -> 'translation' ->> 'label')
			when qtt."name" = 'DOUBLE_RADIOINPUT' then (qq.data -> 'question1st' -> 'radio' -> 'translation' ->> 'label')
			when qtt."name" = 'RADIO_MULTIPLEINPUT' then (qq.data -> 'radio' -> 'translation' ->> 'label')
			when qtt."name" = 'SIMPLETYPE_CHECKBOX' then (qq.data -> 'value' -> 'translation' ->> 'label')
			when qtt."name" = 'DATE_CHECKBOX' then (qq.data -> 'date' -> 'translation' ->> 'label')
			when qtt."name" = 'NUMBER_DEPENDENTGRID' then (qq.data -> 'question1st' -> 'translation' ->> 'label') --tutaj jest wiele pod pytan
			when qtt."name" = 'NUMBER_DEPENDENTGRID' then (qq.data -> 'question1st' -> 'translation' ->> 'label')
			when qtt."name" = 'EXTENDABLE_GRID' then (qq.data -> 'rows' -> 'OTHER_ADDRESS' -> 'translation' ->> 'label')
		else (qq.data -> 'translation' ->> 'label')
		end pytanie_eng
		
		from form.question qq 
		join form.question_type qtt on qtt.id = qq.id_question_type 
		
	)



select 
	f.uuid as uuid_ankieta, 
	f.id as ankieta_id, 
	an.id_form ,
	an.id pytanie_id, 
	an.uuid as uuid_pytanie, 
	q.stamp, 
	an.id_question, 
	q.id_section,
	qt.id,
	qt."name" as typ_pytania, 
	s."name" as section_question,
	jsonb_pretty(q.data),
	(q.data -> 'translation' ->> 'label') as label,
case 
	when qt."name" = 'SELECT_ONEINPUT' then (q.data -> 'select' -> 'translation' ->> 'label')
	when qt."name" = 'RADIO_INPUT' then (q.data -> 'radio' -> 'translation' ->> 'label')
	when qt."name" = 'DOUBLE_RADIOINPUT' then (q.data -> 'question1st' -> 'radio' -> 'translation' ->> 'label')
	when qt."name" = 'RADIO_MULTIPLEINPUT' then (q.data -> 'radio' -> 'translation' ->> 'label')
	when qt."name" = 'SIMPLETYPE_CHECKBOX' then (q.data -> 'value' -> 'translation' ->> 'label')
	when qt."name" = 'DATE_CHECKBOX' then (q.data -> 'date' -> 'translation' ->> 'label')
	when qt."name" = 'NUMBER_DEPENDENTGRID' then (q.data -> 'question1st' -> 'translation' ->> 'label') --tutaj jest wiele pod pytan
	when qt."name" = 'NUMBER_DEPENDENTGRID' then (q.data -> 'question1st' -> 'translation' ->> 'label')
	when qt."name" = 'EXTENDABLE_GRID' then (q.data -> 'rows' -> 'OTHER_ADDRESS' -> 'translation' ->> 'label')
else (q.data -> 'translation' ->> 'label')
end poprawa,

tr.value ->> 'PL' as translated,
an."data" ,
--jsonb_object_keys(an."data") 
--jsonb_each_text(an."data") 
jsonb_pretty(an."data"),
case
when qt."name"= 'TEXT' then an.data::text 
when qt."name"= 'DATE' then an.data::text 
when qt."name"= 'RADIO' then (an.data)::text 
--when qt."name"= 'RADIO_INPUT'then (an.data -> 'radio')::text
when qt."name"= 'SELECT_ONE' then (an.data)::text 
when qt."name"= 'NUMBER' then (an.data)::text 
when qt."name"= 'CHECKBOX' then (jsonb_array_elements(an.data))::text --unnest
when qt."name"= 'SIMPLETYPE_CHECKBOX' then unnest(array[an.data ->> 'value'::text, an.data->>  'checkbox'::text])::text
--when qt."name"= 'SIMPLETYPE_CHECKBOX' then (an.data ->> 'value')::text || ';' || (an.data->>  'checkbox')::text
when qt."name"= 'SELECT_ONEINPUT' then (an.data ->> 'select')::text ||';' || (an.data->>  'questions')::text

--RADIO INPUT
when qt."name"= 'RADIO_INPUT' and (an.data -> 'input'->> 'defined' != '{}') then (an.data -> 'input' ->> 'defined' )::text
--when qt."name"= 'RADIO_INPUT' and (an.data ->> 'radio')::text = 'YES' then an.data ->> 'radio'::text
when qt."name"= 'RADIO_INPUT' and (an.data ->> 'radio')::text = 'NO' then an.data ->> 'radio'::text
--when qt."name"= 'RADIO_INPUT' and (an.data -> 'input' ->> 'user' is not null) then replace(replace((an.data -> 'input' -> 'user')::text, '[', '')::text, ']', '')::text
when qt."name"= 'RADIO_INPUT' and (an.data -> 'input' ->> 'user' is not null) then (an.data -> 'input' ->> 'user')::text
when qt."name"= 'RADIO_INPUT' and (an.data -> 'input' ->> 'input' is not null) then (unnest(array[an.data -> 'input' -> 'input']))::text
when qt."name"= 'RADIO_INPUT' and (an.data ->> 'input'  is not null) then (an.data ->> 'input' )::text
when qt."name"= 'RADIO_INPUT' and (an.data ->> 'radio')::text = 'YES' then an.data ->> 'radio'::text

--RADIO_MULTIPLEINPUT
when qt."name"= 'RADIO_MULTIPLEINPUT' and (an.data ->> 'radio')::text = 'NO' then an.data ->> 'radio'::text
when qt."name"= 'RADIO_MULTIPLEINPUT' and (an.data ->> 'radio')::text = 'YES' then an.data ->> 'questions'::text

when qt."name"= 'CHECKBOX_INPUT'  then an.data ::text
when qt."name"= 'DATE_CHECKBOX'  then (array[(an.data ->>'date'::text) || ':' ||(an.data ->'checkbox' ::text )])::text
when qt."name"= 'DOUBLE_RADIOINPUT'  then unnest(array[(an.data ->>'question1st'::text) || ',' ||(an.data ->>'question2nd' ::text )])::text
when qt."name"= 'NUMBER_DEPENDENTGRID'  then unnest(array[(an.data::text)])::text
--when qt."name"= 'EXTENDABLE_GRID' and (an.data -> 'user' ->> 'checked') = 'OTHER_ADDRESS' then 'asdsad' -- (an.data -> 'user' -> 'defined' -> 'OTHER_ADDRESS')::text
when qt."name"= 'EXTENDABLE_GRID' and (an.data ->> 'checked') != '[]' then unnest(array[an.data -> 'defined'->> 'OTHER_ADDRESS'::text])::text
else 
null end odpowiedzi,
pyta.*, trsl.pytanie_pl
from form.form f
	JOIN form.form_type AS ft ON f.id_form_type = ft.id
	join form.answer as an on an.id_form = f.id
	join form.question q on q.id = an.id_question 
	join form.question_type qt on qt.id = q.id_question_type 
	join form.section s on s.id = q.id_section 
	left join form.translation tr on tr."key" = replace((q.data -> 'translation' -> 'label')::text, '"', '')
	left join pytania_slownik pyta on pyta.stamp = q.stamp
	left join tlumaczenia_slownik trsl on  trsl.key = pyta.pytanie_eng
--	where 
--	f.id = 30276 and
	where f.uuid =  'e717353f-19a7-4381-874b-4660373ac9c8' 
--and  
--qt."name"= 'EXTENDABLE_GRID'
--and qt.id=13
order by q.id_section 