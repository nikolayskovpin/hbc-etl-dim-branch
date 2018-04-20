BEGIN;
-- Start log
INSERT INTO audit_transformation_log (transname, id_batch, startdate, status) VALUES ('{transname_target}', '{max_hist_batch_id}' , '{starttime}', 'start');
END;

BEGIN;
delete from dw.dim_brands
using dw.dim_brands_hist dbh
where dw.dim_brands.brand_id=dbh.brand_id
and dbh.dw_batch_id > '{dw_batch_id}'
and dbh.to_time='3000-01-01 00:00:00+00'
;

insert into dw.dim_brands
select
brand_id,
ops_brand_id,
brand_name,
ups_account_number,
deleted_at,
short_code,
round_msrp,
show_brand_code,
ops_admin_vendor_id,
dbh.dw_batch_id,
created_at,
updated_at,
archived_at
from dw.dim_brands_hist dbh
where dbh.dw_batch_id>'{dw_batch_id}'
and dbh.to_time='3000-01-01'
;

-- Update the last log to be marked end
update audit_transformation_log set status='end', errors=0
where
        transname='{transname_target}'
        and status='start'
        and id_batch='{max_hist_batch_id}'
        and startdate = '{starttime}';
END;


BEGIN;

-- Update the last log with an enddate
update audit_transformation_log set enddate=now()
where
        transname='{transname_target}'
        and status='end'
        and id_batch='{max_hist_batch_id}'
        and startdate = '{starttime}';
END;
