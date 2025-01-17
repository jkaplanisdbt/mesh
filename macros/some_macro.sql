{% macro select_all(model) %}

    

    select * 

    from {{ model }} 
    limit 100



{% endmacro %}