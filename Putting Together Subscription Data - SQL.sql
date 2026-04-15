/* join Tables*/

select 
	s.userID as user_id
	,Auditlog.*
    ,cast(json_value(s.data, '$.subscriptionWentActive')as date) as last_activated_at
	,json_value(s.data, '$.promoCode') as promo_code
	,json_value(s.data, '$.active') as is_current_active
	,json_value(s.data, '$.created') as created_at
	,json_value(s.data, '$.cancelled') as last_cancelled_at
    ,json_value(s.data, '$.braintreeID') as current_braintree_id
	,json_value(s.data, '$.isPayingSubscriber') as is_paying_subscriber
	,json_value(s.data, '$.isZombie') as is_zombie
	,json_value(s.data, '$.lastReconcile') as last_reconcile
	,json_value(s.data, '$.braintreeStatus') as braintree_status
	,json_value(s.data, '$.paymentToken') as payment_token
	,BT.*  
from csidb.csi.Subscription as s                                                                  /* subscription most recent record*/ 
full outer join                                                                                        
    (
	select 
	    a.id as audit_id
        ,json_value(a.previousContent, '$.subID') as subscription_id
        ,json_value(a.previousContent, '$.previousSubscriptionData.braintreeID') as braintree_id
        ,json_value(a.previousContent, '$.previousSubscriptionData.userID') as user_id
        ,json_value(a.previousContent, '$.previousSubscriptionData.lastRenewal') as activated_at
        ,right(
		(select value as Cancelled 
		from openjson(json_query(previousContent, '$.changes')) 
		where value like '%Cancelled: %'), 21) as cancelled_at
		,json_value(previousContent, '$.previousSubscriptionData.data.kickback') as kickback
	    ,json_value(previousContent, '$.previousSubscriptionData.data.kickbackPriceThreshold') as kickback_price_threshold
	    ,json_value(previousContent, '$.previousSubscriptionData.data.price') as Price
	    ,json_value(previousContent, '$.previousSubscriptionData.data.lastKickbackPercentage') as last_kickback_percentage

    from csidb.csi.AuditLog as a                                                                   /* timeseries subscription actions*/
	    join                                                                 
          (select
          json_value(previousContent, '$.previousSubscriptionData.braintreeID') as braintree_id
	      ,max(id) as max_id
          from 
	      csidb.csi.AuditLog where json_value(previousContent, '$.previousSubscriptionData.lastRenewal') is not null
          group by 
	      json_value(previousContent, '$.previousSubscriptionData.braintreeID')) as suba
          on 
	      json_value(a.previousContent, '$.previousSubscriptionData.braintreeID')=suba.braintree_id 
	      and a.id=suba.max_id

    where
    a.id > 16208 and json_value(a.previousContent, '$.previousSubscriptionData.braintreeID') !=' ' ) as Auditlog                                                   
on s.userID= Auditlog.user_id 
full outer join 
    (
    select distinct                                                                                              
        timestamp
	    ,json_value(bt.data, '$.subscriptionID') as braintree_id
	    ,json_value(bt.data, '$.userID') as user_id
	    ,cast(json_value(bt.data, '$.firstBillingDate')as date) as first_billing_date
	    ,json_value(bt.data, '$.nextBillingPeriodAmount') as next_billing_amount
	    ,cast(json_value(bt.data, '$.paidThroughDate') as date) as paid_through_date
        ,json_value(bt.data, '$.price') as price
	    ,cast(json_value(bt.data, '$.billingPeriodStartDate')as date) as billing_start_date
	    ,cast(json_value(bt.data, '$.billingPeriodEndDate') as date) as billing_end_date
	    ,json_value(bt.data, '$.email') as email
	    ,json_value(bt.data, '$.paymentStatus') as payment_status
	    ,json_value(bt.data, '$.paymentToken') as payment_token
	    ,json_value(bt.data, '$.processorResponseText') as processor_response_text
	    ,json_value(bt.data, '$.processorResponseType') as processor_response_type
	    ,json_value(bt.data, '$.numberOfTransactions') as number_of_transactions
	    ,json_value(bt.data, '$.numberOfAddOns') as number_of_addons
	    ,json_value(bt.data, '$.numberOfDiscounts') as number_of_discounts
	    ,json_value(bt.data, '$.isPaymentValid') as if_payment_valid
	    ,json_value(bt.data, '$.zombie') as if_zombie
	from 
	csidb.csi.WebhookLog as bt                                                         /*Braintree Subscription Data*/
	    join                                                                 
          (select 
	      json_value(data, '$.userID') as user_id
	      ,json_value(data, '$.subscriptionID') as sub_id
	      ,max(timestamp) as max_time
          from 
	      csidb.csi.WebhookLog as w
          group by 
	      json_value(data, '$.userID')
	      ,json_value(data, '$.subscriptionID')) as sub
          on 
	      json_value(bt.data, '$.subscriptionID')=sub.sub_id 
	      and json_value(bt.data, '$.userID')=sub.user_id 
	      and bt.timestamp=sub.max_time
	  ) as BT
    on s.userID = BT.user_id 
	   and BT.Braintree_ID= (case when Auditlog.audit_id is null then json_value(s.data, '$.braintreeID') else Auditlog.braintree_id end) 
order by s.userID



select s.userID,
	json_value(s.data, '$.active') as [Active?] 
from csidb.csi.Subscription as s
