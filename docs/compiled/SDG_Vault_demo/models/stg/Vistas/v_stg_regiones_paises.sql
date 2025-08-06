

select a.*, b.*
from SDGVAULTMART.DBT_SDGVAULT.STG_REGIONES a
join SDGVAULTMART.DBT_SDGVAULT.STG_PAISES b on a.r_regionkey = b.n_regionkey