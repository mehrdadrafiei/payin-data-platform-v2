# Metric definitions (used by the baseline on `core` and by the optimized gold layer)

* **Day** = calendar day in Asia/Tehran. **Month** = Gregorian month in Asia/Tehran. No Jalali calendar.
* **Successful transaction** = `status = 'SUCCESS'`. REVERSED means a payment that was completed and then
  cancelled, so it does not count as successful.
* **Success rate** = SUCCESS / (SUCCESS + FAILED + REVERSED). PENDING is excluded from the denominator
  because its outcome is not known yet. If the denominator is 0 the rate is empty (NULL), not 0.
* **Card 1**: successful amount and count per day, last 90 days including today.
* **Card 2**: success rate per day and channel, last 30 days including today.
* **Card 3**: top 10 merchants by successful amount in the current month.
* **Card 4**: sum of `fee_amount` of successful transactions by merchant category (MCC) and province, last 90 days.
  Baseline on `core` can only use the merchant's CURRENT province. The optimized version uses the province the
  merchant had AT TRANSACTION TIME; the difference is explained in the README.
* **Card 5**: successful transaction count and amount per month and merchant risk level. Baseline uses the
  CURRENT risk level; the optimized version uses the risk level AT TRANSACTION TIME.
* Amounts are in rials.
