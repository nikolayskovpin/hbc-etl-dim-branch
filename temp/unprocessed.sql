select dw_batch_id, max_hist_batch_id, 'gilt_us_raw_inc_ops#dim_brands#dimension#dw.dim_brands'::varchar as transname_target
from
	(
		select coalesce(max(id_batch),-1)  as dw_batch_id
		from audit_transformation_log
		where transname='gilt_us_raw_inc_ops#dim_brands#dimension#dw.dim_brands'
				AND
				status = 'end'
	) A,
	(
		select max(id_batch) as max_hist_batch_id
		from audit_transformation_log
		where transname='gilt_us_raw_inc_ops#dim_brands_hist#dimension#dw.dim_brands_hist'
				AND
				status = 'end'
	) B
where a.dw_batch_id<b.max_hist_batch_id