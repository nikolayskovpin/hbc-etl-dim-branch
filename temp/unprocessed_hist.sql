select distinct id_batch AS dw_batch_id, 'gilt_us_raw_inc_ops#dim_brands_hist#dimension#dw.dim_brands_hist'::varchar as transname_target
from audit_transformation_log d1,
    (select coalesce(max(id_batch),-1) as max_clean_batch_id
     from  audit_transformation_log
     where transname='gilt_us_raw_inc_ops#dim_brands_hist#dimension#dw.dim_brands_hist'
			AND
			status = 'end'
    ) x
where
	d1.id_batch>x.max_clean_batch_id
	AND
	transname='gilt_us_ops#IOE_set_time_bounds_value#raw_inc##'
	AND
	status = 'end'
order by id_batch