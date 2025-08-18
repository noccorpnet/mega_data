{%- set start_date = modules.datetime.date(2025,8,6).strftime('%Y-%m-%d') -%}
{%- set current_end_date = (modules.datetime.date.today() + modules.datetime.timedelta(days=5*365)).strftime('%Y-%m-%d') -%}
{%- set threshold_days = 60 -%}
{%- set new_end_date = (modules.datetime.date.today() + modules.datetime.timedelta(days=5*365 + threshold_days)).strftime('%Y-%m-%d') -%}
{%- if modules.datetime.date.today() > modules.datetime.datetime.strptime(current_end_date, '%Y-%m-%d').date() - modules.datetime.timedelta(days=threshold_days) -%}
  {%- set final_end_date = new_end_date -%}
{%- else -%}
  {%- set final_end_date = current_end_date -%}
{%- endif -%}

{{ dbt_date.get_date_dimension(
    start_date=start_date,
    end_date=final_end_date
) }}