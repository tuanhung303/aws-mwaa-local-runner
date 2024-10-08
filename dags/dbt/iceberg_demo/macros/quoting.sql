{% macro nq(obj) %}

{#- remove double quote -#}
{% set string = modules.re.sub('"', '', obj | string) %}
{#- remove single quote -#}
{% set string = modules.re.sub("'", '', string) %}
{{ return(string) }}

{% endmacro %}