-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {# 1. Airflow 등 외부에서 전달한 svc_nm 변수를 가져옴 #}
    {%- set svc_nm = var('svc_nm', none) -%}

    {%- if svc_nm and custom_schema_name -%}
        {# 2. svc_nm이 있고 폴더별 schema 설정이 있으면 합침 (예: svc_a_bronze) #}
        {{ svc_nm | trim }}_{{ custom_schema_name | trim }}
    {%- elif custom_schema_name -%}
        {# 3. svc_nm이 없으면 그냥 폴더 설정 이름 사용 #}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {# 4. 둘 다 없으면 기본 스키마(main) 사용 #}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}