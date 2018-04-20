BEGIN;
-- Start log
INSERT INTO audit_transformation_log (transname, id_batch, startdate, status) VALUES ('{transname_target}', '{dw_batch_id}' , '{starttime}', 'start');
END;

BEGIN;
-- create temp table for data that need to be inserted from the current batch (new and updated records)
CREATE temp DIMENSION TABLE dim_brands_hist_new AS
(
select
dw_id as brand_hist_id,
dw_id as brand_id,
bc.id as ops_brand_id,
trim(lower(bc.name)) as brand_name,
bc.ups_account_number,
bc.deleted_at,
bc.short_code,
bc.round_msrp,
bc.show_brand_code,
bc.admin_vendor_id as ops_admin_vendor_id,
bc.updated_at as from_time,
'3000-01-01'::timestamp as to_time,
bc.dw_batch_id as dw_batch_id,
bc.created_at,
bc.updated_at,
bc.archived_at
from raw_inc_ops.brands bc
left join dw.dim_brands_hist db on db.ops_brand_id=bc.id
      and
        md5(bc.id || coalesce(trim(lower(bc.name)) ,'-1')  ||  coalesce(bc.ups_account_number,'0') || coalesce(bc.deleted_at,'01-01-1000') || coalesce(bc.short_code,'-1') ||
        coalesce(bc.round_msrp,-1) || coalesce(bc.show_brand_code,-1) || coalesce(bc.admin_vendor_id,-1) || coalesce(bc.archived_at,'01-01-1000')) =
        md5(db.ops_brand_id || coalesce(db.brand_name ,'-1')  ||  coalesce(db.ups_account_number,'0') || coalesce(db.deleted_at,'01-01-1000') || coalesce(db.short_code,'-1') ||
        coalesce(db.round_msrp,-1) || coalesce(db.show_brand_code,-1) || coalesce(db.ops_admin_vendor_id,-1) || coalesce(db.archived_at,'01-01-1000') )
      and db.to_time='3000-01-01 00:00:00+00'
where bc.dw_batch_id='{dw_batch_id}'
and db.brand_id is null
)
;

-- create temp table for records that need to update (to_time)

CREATE  temp DIMENSION TABLE dim_brands_hist_change  AS
select db.*
from dw.dim_brands_hist db
join dim_brands_hist_new dbht on
		db.ops_brand_id=dbht.ops_brand_id
		AND
		db.to_time='3000-01-01 00:00:00+00'
;


DELETE FROM dw.dim_brands_hist
	USING dim_brands_hist_change dbht
	WHERE
		dw.dim_brands_hist.brand_hist_id=dbht.brand_hist_id
		AND
		dw.dim_brands_hist.to_time='3000-01-01 00:00:00+00'
;

--insert new and updated rows
insert into dw.dim_brands_hist (brand_hist_id, brand_id, ops_brand_id, brand_name, ups_account_number, deleted_at, short_code,
round_msrp,show_brand_code,ops_admin_vendor_id,from_time,to_time,dw_batch_id,created_at,updated_at, archived_at
)
select
brand_hist_id,
case when seq=1 then brand_id else brand_id_cal end as brand_id,
ops_brand_id,
brand_name,
ups_account_number,
deleted_at,
short_code,
round_msrp,
show_brand_code,
ops_admin_vendor_id,
(case when type='n' and seq=1 then created_at else from_time end) as from_time,
to_time_cal as to_time,
dw_batch_id,
created_at,
updated_at,
archived_at
from (
select  ROW_NUMBER() OVER(PARTITION BY ops_brand_id ORDER BY brand_hist_id) AS seq ,
lead(updated_at- interval '1 microseconds',1,'3000-01-01')  OVER(PARTITION BY ops_brand_id ORDER BY brand_hist_id) AS to_time_cal,
lag(brand_id,1,'-1')  OVER(PARTITION BY ops_brand_id ORDER BY brand_hist_id) AS brand_id_cal,
-- lag(updated_at,1,'1900-08-01')  OVER(PARTITION BY id ORDER BY dw_id) AS from_time, updated_at as to_time,
s.*
from
(
(select 'u'::varchar type,
brand_hist_id, brand_id, ops_brand_id, brand_name, ups_account_number, deleted_at, short_code,
round_msrp,show_brand_code,ops_admin_vendor_id,from_time,to_time,dw_batch_id,created_at,updated_at,archived_at
from dim_brands_hist_change dbh
)
union
(
select
'n'::varchar type,
brand_hist_id, brand_id, ops_brand_id, brand_name, ups_account_number, deleted_at, short_code,
round_msrp,show_brand_code,ops_admin_vendor_id,from_time,to_time,dw_batch_id,created_at,updated_at,archived_at
from dim_brands_hist_new dbh
)
) s
) x
;

-- Update the last log to be marked end
update audit_transformation_log set status='end', errors=0
where
        transname='{transname_target}'
        and status='start'
        and id_batch='{dw_batch_id}'
        and startdate = '{starttime}';
END;


BEGIN;

-- Update the last log with an enddate
update audit_transformation_log set enddate=now()
where
        transname='{transname_target}'
        and status='end'
        and id_batch='{dw_batch_id}'
        and startdate = '{starttime}';
END;