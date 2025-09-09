select DISTINCT(STATE) from CUSTOMERS order by STATE;

select * from US_INCOME_WIDE uiw inner join CUSTOMERS c on uiw.STATE_CLEAN = c.state order by STATE_CLEAN;


select STATE_CLEAN from US_INCOME_WIDE;



