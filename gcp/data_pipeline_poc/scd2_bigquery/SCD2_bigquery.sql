--create source table

CREATE TABLE IF NOT EXISTS `gls-customer-poc.ds2.source`(
	id int,
	customer struct<id int, name string, location string>,
	orders array<struct<id int, product string, quantity int>>,
	created_time datetime,
	modified_time datetime
);


--insert into source table
insert into gls-customer-poc.ds2.source values (001, (91, 'john', 'noida'), [(2001, 'mango', 15)], datetime(CURRENT_DATETIME()), datetime(CURRENT_DATETIME()));
insert into gls-customer-poc.ds2.source values (002, (82, 'kevi', 'mumbai'), [(1001, 'mango', 10), (1002, 'lichi', 30)],
datetime(CURRENT_DATETIME()), datetime(CURRENT_DATETIME()));
insert into gls-customer-poc.ds2.source values (003, (93, 'raj', 'pune'), [(1001, 'banana', 10), (1002, 'cheery', 30)],
datetime(CURRENT_DATETIME()), datetime(CURRENT_DATETIME()));


--fetch data from source table
Select * from `gls-customer-poc.ds2.source`;

--filter and fetch data from source table using unnest
select s.id, s.customer.name,
	orders
	from `gls-customer-poc.ds2.source` s, unnest(s.orders) as orders
	where orders.product = 'mango';

--filter and fetch data from source table using cross join
select s.id, s.customer.name,
	orders.product,
	orders.quantity,
	from `gls-customer-poc.ds2.source` s cross join s.orders;

-- create data warehouse table for scd type 2
CREATE TABLE IF NOT EXISTS `gls-customer-poc.ds2.cust_ordar_details`(
	id int,
	staging_id int,
	customer struct<id int, name string, location string>,
	orders array<struct<id int, product string, quantity int>>,
	modified_time datetime,
	end_time datetime
);

-- statement to insert/update the final table to implement scd type 2
merge `gls-customer-poc.ds2.cust_ordar_details` as t
using (
	--all records from source
	select s3.id as join_key, s3.*
	from (
		select s4.id, s4.customer, orders, s4.created_time, s4.modified_time
		from `gls-customer-poc.ds2.source` s4,
		unnest(s4.orders) orders
	) s3
	union all
	--only updated rows
	select null, s1.id, s1.customer, s1.orders, s1.created_time, s1.modified_time
	from ( 
		select s2.1d, s2.customer, orders, s2.created_time, s2.modified_time
		from `gls-customer-poc.ds2.source` s2, 
		unnest(s2.orders) orders
	) s1
	inner join (
		select t2.staging_id, t2.customer, orders, t2.end_time 
		from `gls-customer-poc.ds2.cust_ordar_details` t2
		,unnest(t2.orders) orders
	) t1
	on s1.id = t1.staging_id
	where ( s1.customer.id <> t1.customer.id
			or s1.customer.name <> t1.customer.name
			or s1.customer location <> t1.customer location
			or s1.orders.id<> t1.orders.id
			or s1.orders.product<> t1.orders.product
			or s1.orders.quantity <> t1.orders.quantity 
	) and t1.end time is NULL
)as s
on s.join_key = t.staging_id
when matched then
update set end_time = current_datetime())
when not matched by target then
insert (staging_id, customer, orders, modified_time, end_time)
values (s.join_key, (s.customer.id, s.customer.name, s.customer.location),
	[(s.orders.id, s.orders.product, s.orders.quantity)], current_datetime, null)
	
	
--fetch data from target table
Select * from `gls-customer-poc.ds2.cust_ordar_details`;