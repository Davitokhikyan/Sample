<?php

namespace Jobs;

use Carbon\Carbon;
use Illuminate\Bus\Queueable;
use Illuminate\Queue\SerializesModels;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Support\Facades\Cache;
use Core\SmartMemberships\Models\Customer;
use Core\SmartMemberships\Models\Member;
use Core\SmartProducts\Models\CheckoutAbandoned;
use Core\SmartProducts\Models\IpnRawLog;
use Core\SmartProducts\Models\Product;
use Core\SmartProducts\Models\ProductDelivery;
use Core\SmartProducts\Models\ProductOrder;
use Core\SmartProducts\Models\ProductPricing;
use Core\SmartProducts\Models\ProductSetting;
use Core\SmartProducts\Models\ProductType;
use Core\SmartProducts\Models\Transaction;
use Core\Utils\ProductActionHelper;

class PaypalWebhookHandler extends BasePaymentProcessorHandler implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    /**
     * @var IpnRawLog
     */
    private IpnRawLog $ipnLog;
    /**
     * @var array
     */
    private array $params;
    /**
     * One time product events list
     * @var array
     */
    private array $oneTimeEvents = [
        "CHECKOUT.ORDER.APPROVED",
        "PAYMENTS.PAYMENT.CREATED",
        "PAYMENT.CAPTURE.REFUNDED",
        "PAYMENT.CAPTURE.REVERSED"
    ];
    /**
     * Subscription product events list
     * @var array
     */
    private array $subscriptionEvents = [
        "BILLING.SUBSCRIPTION.ACTIVATED",
        "BILLING.SUBSCRIPTION.CANCELLED",
        "BILLING.SUBSCRIPTION.PAYMENT.FAILED",
        "BILLING.SUBSCRIPTION.UPDATED",
        "PAYMENT.SALE.COMPLETED",
        "PAYMENT.SALE.REFUNDED"
    ];

    /**
     * PaypalWebhookHandler constructor.
     * @param IpnRawLog $ipnRawLog
     * @param array $params
     */
    public function __construct(IpnRawLog $ipnRawLog, $params = [])
    {
        $this->ipnLog = $ipnRawLog;
        $this->params  = $params;
    }

    /**
     * @return IpnRawLog
     */
    public function getIpnRawLog(){
        return $this->ipnLog;
    }

    /**
     * @return array
     */
    public function getParams(){
        return $this->params;
    }

    /**
     * @return bool
     */
    public function handle()
    {
        // get transaction type of IPN
        $ipnTransactionType = $this->ipnLog["transaction_type"];

        $dbData = $this->ipnLog;
        $ipnData = json_decode($dbData->ipn_data , true);
        $ipnDataHash = hash("sha256", $dbData->ipn_data);

        $productId = $this->params["product_id"];
        $productPricingId = $this->params["product_pricing_id"];

        // get product
        $product = Product::find($productId);
        // if product not found in our db
        if (is_null($product)){
            logError("critical", "Product does not exist", __FILE__, __LINE__, ["product" => $productId]);
            return false;
        }

        // get product pricing
        $productPricing = $product->product_pricing()->find($productPricingId);
        // Initially set to 0
        $isTest = 0;
        // handle product orders, customers and transactions
        switch ($ipnTransactionType){
            // onetime product was purchased
            case "CHECKOUT.ORDER.APPROVED":
                // decode the custom ipn data
                $ipnCustomData = json_decode($ipnData["resource"]["purchase_units"][0]["custom_id"], true);

                // get capture id
                $captureId = $ipnData["resource"]["purchase_units"][0]["payments"]["captures"][0]["id"] ?? $ipnData["resource"]["id"];
                // get amount and currency
                $paymentAmount = $ipnData["resource"]["purchase_units"][0]["amount"]["value"];
                $paymentCurrency = $ipnData["resource"]["purchase_units"][0]["amount"]["currency_code"];

                // get customer payer id
                $customerPayerId = $ipnData["resource"]["payer"]["payer_id"];
                // get customer personal info
                $customerPersonalInfo = $ipnData["resource"]["payer"];
                // get customer shipping info
                $customerShippingInfo = $ipnData["resource"]["purchase_units"][0]["shipping"]["address"];

                // collect customer data
                $customerData = [
                    "cust_pay_email"  => $customerPersonalInfo["email_address"],
                    "cust_email"      => $customerPersonalInfo["email_address"],
                    "cust_fname"      => $customerPersonalInfo["name"]["given_name"],
                    "cust_lname"      => $customerPersonalInfo["name"]["surname"],
                    "cust_address1"   => $customerShippingInfo["address_line_1"],
                    "cust_city"       => $customerShippingInfo["admin_area_2"],
                    "cust_country"    => $customerShippingInfo["country_code"],
                    "cust_zipcode"    => $customerShippingInfo["postal_code"],
                    "paypal_payer_id" => $customerPayerId
                ];

                if (empty($ipnCustomData["kti"]) == false){
                    $customerData["_track_id"] = $ipnCustomData["kti"];
                }

                if(!empty($ipnData['test_ipn']) || in_array($this->params['sandbox_mode'], ['on', 1])){
                    // mark this as a test transaction
                    $isTest = 2;
                    // send notification to product owner for test tarnsaction
                    $this->sendTestTransactionNotificationToProductOwner("Paypal", $product->user_id, $ipnData);
                }
                elseif( $paymentAmount < 5 ){
                    $isTest = 1;
                }
                // if product pricing was not found
                if (is_null($productPricing)) {
                    // send notification to product owner that pricing was not found for product
                    $this->sendMissingProductPricingNotificationToProductOwner("Paypal", $product, $paymentAmount, $paymentCurrency, $customerData['cust_email'], $customerData['cust_fname']);

                    logError("critical", "Product's pricing does not exist.", __FILE__, __LINE__, ["ipn" => $ipnData, "product" => $product->id]);
                    return false;
                }

                // get customer in our db
                $customer = Customer::where("cust_pay_email", $customerPersonalInfo["email_address"])->first();
                // if customer does not exist create a new one
                if (is_null($customer)) {
                    $customer = Customer::create($customerData);
                }
                // otherwise update the existing customer's data
                else{
                    $customer->update($customerData);
                }
                // invalidate customers cache
                Cache::tags("{$customer->cacheKey}_{$product->user_id}")->flush();

                $couponCode = null;
                // if coupon is used
                if (isset($ipnCustomData["coupon"]) && empty($ipnCustomData["coupon"]) == false){
                    $couponCode = $ipnCustomData["coupon"];
                    // update `used` value of product coupon
                    $this->updateCouponRemainingCount($couponCode);
                }

                // create a product order
                $productOrder = ProductOrder::create([
                    "customer_id"             => $customer->id,
                    "product_id"              => $product->id,
                    "product_pricing_id"      => $productPricing->id,
                    "product_pricing_type_id" => $productPricing->product_pricing_type_id,
                    "status"                  => ProductOrder::STATUS_COMPLETED,
                    "payment_processor"       => "paypal",
                    "amount"                  => $paymentAmount,
                    "currency"                => $productPricing->price_currency,
                    "subscription_id"         => $captureId,
                    "coupon_code"             => $couponCode,
                    "is_test"                 => $isTest
                ]);

                // checks if the order is the first one for user and log activity in ES
                $this->firstProductOrderActivityLog($product->user_id, $productOrder);

                // create a transaction for order
                $transaction = $productOrder->transactions()->create([
                    "txn_id"            => $captureId,
                    "trans_amount"      => $paymentAmount,
                    "trans_currency"    => $productPricing->price_currency,
                    "trans_date"        => date('Y-m-d H:i:s', strtotime($ipnData["resource"]["create_time"])),
                    "trans_gateway"     => "paypal",
                    "trans_type"        => "purchase",
                    "is_rebill"         => 0,
                    "buyer_email"       => $customer->cust_pay_email,
                    "ipn_hash"          => $ipnDataHash,
                    "is_test"           => $isTest
                ]);
                break;
            // onetime upsell product was purchased
            case "PAYMENTS.PAYMENT.CREATED":
                // get capture id
                $captureId = $ipnData["resource"]["id"];
                // get amount and currency
                $paymentAmount = $ipnData["resource"]["transactions"][0]["amount"]["total"];
                $paymentCurrency = $ipnData["resource"]["transactions"][0]["amount"]["currency"];

                // get customer personal info
                $customerPersonalInfo = $ipnData["resource"]["payer"]["payer_info"];

                if(!empty($ipnData['test_ipn']) || in_array($this->params['sandbox_mode'], ['on', 1])){
                    // mark this as a test transaction
                    $isTest = 2;
                    // send notification to product owner for test tarnsaction
                    $this->sendTestTransactionNotificationToProductOwner("Paypal", $product->user_id, $ipnData);
                }
                elseif( $paymentAmount < 5 ){
                    $isTest = 1;
                }

                // get customer in our db
                $customer = Customer::where("cust_pay_email", $customerPersonalInfo["email"])->first();

                // create a product order
                $productOrder = ProductOrder::create([
                    "customer_id"             => $customer->id,
                    "product_id"              => $product->id,
                    "product_pricing_id"      => $productPricing->id,
                    "product_pricing_type_id" => $productPricing->product_pricing_type_id,
                    "status"                  => ProductOrder::STATUS_COMPLETED,
                    "payment_processor"       => "paypal",
                    "amount"                  => $paymentAmount,
                    "currency"                => $paymentCurrency,
                    "subscription_id"         => $captureId,
                    "coupon_code"             => null,
                    "is_test"                 => $isTest
                ]);

                // checks if the order is the first one for user and log activity in ES
                $this->firstProductOrderActivityLog($product->user_id, $productOrder);

                // create a transaction for order
                $transaction = $productOrder->transactions()->create([
                    "txn_id"            => $captureId,
                    "trans_amount"      => $paymentAmount,
                    "trans_currency"    => $productPricing->price_currency,
                    "trans_date"        => date('Y-m-d H:i:s', strtotime($ipnData["resource"]["create_time"])),
                    "trans_gateway"     => "paypal",
                    "trans_type"        => "purchase",
                    "is_rebill"         => 0,
                    "buyer_email"       => $customer->cust_pay_email,
                    "ipn_hash"          => $ipnDataHash,
                    "is_test"           => $isTest
                ]);
                break;
            // subscription created for recurring product
            case "BILLING.SUBSCRIPTION.ACTIVATED":
                // if product pricing was not found
                if (is_null($productPricing)) {
                    logError("critical", "Product's pricing does not exist.", __FILE__, __LINE__, ["ipn" => $ipnData, "product_pricing" => $productPricingId]);
                    return false;
                }

                // decode the custom ipn data
                $ipnCustomData = json_decode($ipnData["resource"]["custom_id"], true);

                // get subscription id
                $subscriptionId = $ipnData["resource"]["id"];

                // get customer payer id
                $customerPayerId = $ipnData["resource"]["subscriber"]["payer_id"];
                // get customer personal info
                $customerPersonalInfo = $ipnData["resource"]["subscriber"];
                // get customer shipping info
                $customerShippingInfo = $ipnData["resource"]["subscriber"]["address"];
                if(!empty($ipnData['test_ipn']) || in_array($this->params['sandbox_mode'], ['on', 1])){
                    // mark this as a test transaction
                    $isTest = 2;
                    // send notification to product owner for test tarnsaction
                    $this->sendTestTransactionNotificationToProductOwner("Paypal", $product->user_id, $ipnData);
                }
                // collect customer data
                $customerData = [
                    "cust_pay_email"  => $customerPersonalInfo["email_address"],
                    "cust_email"      => $customerPersonalInfo["email_address"],
                    "cust_fname"      => $customerPersonalInfo["name"]["given_name"],
                    "cust_lname"      => $customerPersonalInfo["name"]["surname"],
                    "cust_address1"   => $customerShippingInfo["address_line_1"],
                    "cust_city"       => $customerShippingInfo["admin_area_2"],
                    "cust_country"    => $customerShippingInfo["country_code"],
                    "cust_zipcode"    => $customerShippingInfo["postal_code"],
                    "paypal_payer_id" => $customerPayerId
                ];

                if (empty($ipnCustomData["kti"]) == false){
                    $customerData["_track_id"] = $ipnCustomData["kti"];
                }

                // get customer in our db
                $customer = Customer::where("cust_pay_email", $customerPersonalInfo["email_address"])->first();
                // if customer does not exist create a new one
                if (is_null($customer)) {
                    $customer = Customer::create($customerData);
                }
                // otherwise update the existing customer's data
                else{
                    $customer->update($customerData);
                }
                // invalidate customers cache
                Cache::tags("{$customer->cacheKey}_{$product->user_id}")->flush();

                $couponCode = null;
                // if coupon is used
                if (isset($ipnCustomData["coupon"]) && empty($ipnCustomData["coupon"]) == false){
                    $couponCode = $ipnCustomData["coupon"];
                    // update `used` value of product coupon
                    $productCoupon = $this->updateCouponRemainingCount($couponCode);

                    if ($productCoupon){
                        // apply coupon on pricing
                        $productPricing->applyCoupon($productCoupon->id);
                    }
                }

                // get product order via subscription
                $productOrder = $product->product_orders()->where("subscription_id", $subscriptionId)->first();

                // if order is missing means the PAYMENT.SALE.COMPLETED event was not sent and handled yet
                if (is_null($productOrder)){
                    // create a product order
                    $productOrder = ProductOrder::create([
                        "customer_id"             => $customer->id,
                        "product_id"              => $product->id,
                        "product_pricing_id"      => $productPricing->id,
                        "product_pricing_type_id" => $productPricing->product_pricing_type_id,
                        "status"                  => ProductOrder::STATUS_COMPLETED,
                        "payment_processor"       => "paypal",
                        "amount"                  => $productPricing->has_trial ? $productPricing->trial_price : $productPricing->price,
                        "currency"                => $productPricing->price_currency,
                        "subscription_id"         => $subscriptionId,
                        "coupon_code"             => $couponCode,
                        "is_test"                 => $isTest
                    ]);
                }
                else{
                    // set the customer for the order as PAYMENT.SALE.COMPLETED event does not contain any customer info
                    $productOrder->update([
                        "customer_id"   => $customer->id,
                        "amount"        => $productPricing->has_trial ? $productPricing->trial_price : $productPricing->price,
                        "coupon_code"   => $couponCode
                    ]);
                }

                // checks if the order is the first one for user and log activity in ES
                $this->firstProductOrderActivityLog($product->user_id, $productOrder);

                $transaction = null;
                break;
            // subscription upgrade/downgrade
            case "BILLING.SUBSCRIPTION.UPDATED":
                // if product pricing does not exist
                if (is_null($productPricing)){
                    logError("critical", "Product's pricing does not exist", __FILE__, __LINE__, ["product_pricing" => $productPricingId]);
                    return false;
                }

                // get subscription id
                $subscriptionId = $ipnData["resource"]["id"];
                // get upgraded product plan id
                $paypalPlanId = $ipnData["resource"]["plan_id"];

                // keep old pricing to send it along with KPDN data
                $downgradedProductPricing = $productPricing;

                // get upgraded product pricing id based on plan
                $upgradedProductPricingId = ProductSetting::where("key", "paypal_plan_id")
                                                        ->where("value", $paypalPlanId)
                                                        ->value("product_pricing_id");

                // get upgraded product pricing
                $productPricing = ProductPricing::find($upgradedProductPricingId);
                // if product pricing does not exist
                if (is_null($productPricing)){
                    logError("critical", "Product pricing does not exist", __FILE__, __LINE__, ["product_pricing" => $upgradedProductPricingId]);
                    return false;
                }

                // get product order by subscription id
                $productOrder = ProductOrder::with("customer")
                                            ->where("product_pricing_id", $downgradedProductPricing->id)
                                            ->where("subscription_id", $subscriptionId)
                                            ->latest("created_at")
                                            ->first();
                // if product order does not exist
                if (is_null($productOrder)){
                    logError("critical", "Product's order does not exist", __FILE__, __LINE__, ["product_pricing" => $downgradedProductPricing->id, "subscription" => $subscriptionId]);
                    return false;
                }

                // update order
                $productOrder->update([
                    "status"                => ProductOrder::STATUS_COMPLETED,
                    "amount"                => $productPricing->price,
                    "product_id"            => $product->id,
                    "product_pricing_id"    => $productPricing->id
                ]);

                // create a transaction for order
                $transaction = $productOrder->transactions()->create([
                    "txn_id"          => $subscriptionId,
                    "trans_amount"    => $productPricing->price,
                    "trans_currency"  => $productPricing->price_currency,
                    "trans_date"      => date("Y-m-d H:i:s", strtotime($ipnData["resource"]["create_time"])),
                    "trans_gateway"   => "paypal",
                    "trans_type"      => $downgradedProductPricing->price < $productPricing->price ? "upgrade" : "downgrade",
                    "buyer_email"     => $productOrder->customer->cust_pay_email,
                    "ipn_hash"        => $ipnDataHash
                ]);
                break;
            // subscription payment was made (initial or rebill)
            case "PAYMENT.SALE.COMPLETED":
                // if product pricing not found in our db
                if (is_null($productPricing)){
                    logError("critical", "Product setting's relation to product pricing table not found for plan from webhook does not exist.", __FILE__, __LINE__, ["ipn" => $ipnData, "product_pricing" => $productPricingId]);
                    return false;
                }

                // get subscription id
                $subscriptionId = $ipnData["resource"]["billing_agreement_id"];

                // get product order via subscription
                $productOrder = $product->product_orders()->where("subscription_id", $subscriptionId)->first();
                if(!empty($ipnData['test_ipn']) || in_array($this->params['sandbox_mode'], ['on', 1])){
                    // mark this as a test transaction
                    $isTest = 2;
                    // send notification to product owner for test tarnsaction
                    $this->sendTestTransactionNotificationToProductOwner("Paypal", $product->user_id, $ipnData);
                }
                // if product order not found in our db
                if (is_null($productOrder)){
                    // create a product order
                    $productOrder = ProductOrder::create([
                        "customer_id"             => null,
                        "product_id"              => $product->id,
                        "product_pricing_id"      => $productPricing->id,
                        "product_pricing_type_id" => $productPricing->product_pricing_type_id,
                        "status"                  => ProductOrder::STATUS_COMPLETED,
                        "payment_processor"       => "paypal",
                        "amount"                  => $productPricing->has_trial ? $productPricing->trial_price : $productPricing->price,
                        "currency"                => $productPricing->price_currency,
                        "subscription_id"         => $subscriptionId,
                        "is_test"                 => $isTest
                    ]);
                }

                $transactions = $productOrder->transactions;

                $isRebill = true;
                // if order has no transactions means this is not a rebill
                if (count($transactions) == 0){
                    $isRebill = false;
                }
                // if order has 1 transaction which was created the same day as the order means this is not a rebill
                elseif (count($transactions) == 1 && date("Y-m-d", strtotime($transactions[0]->created_at)) == date("Y-m-d", strtotime(strtotime($ipnData["resource"]["create_time"])))){
                    $isRebill = false;
                }

                if ($isRebill){
                    // update order status and amount because in case if pricing had trial the order amount is set to trial price
                    $productOrder->update(["status" => ProductOrder::STATUS_COMPLETED, "amount" => $productPricing->price]);
                    // send subscription establishment email to customer
                    $this->sendSubscriptionEstablishmentEmailToCustomer($productOrder);
                }

                // create a transaction for order
                $transaction = $productOrder->transactions()->create([
                    "txn_id"          => $ipnData["resource"]["id"],
                    "trans_amount"    => $productOrder->amount,
                    "trans_currency"  => $productPricing->price_currency,
                    "trans_date"      => date('Y-m-d H:i:s', strtotime($ipnData["resource"]["create_time"])),
                    "trans_gateway"   => "paypal",
                    "trans_type"      => $isRebill ? "rebill" : "purchase",
                    "is_rebill"       => $isRebill ? 1 : 0,
                    "ipn_hash"        => $ipnDataHash,
                    "is_test"         => $isTest
                ]);
                break;
            // onetime payment refund was made
            case "PAYMENT.CAPTURE.REFUNDED":
                // get charge id
                $chargeId = null;
                foreach ($ipnData["resource"]["links"] as $link){
                    if (strpos($link["href"], "captures")){
                        $chargeId = explode("captures/", $link["href"])[1];
                        break;
                    }
                }

                // get product order via charge id
                $productOrder = $product->product_orders()->where("subscription_id", $chargeId)->first();
                // if product order not found
                if (is_null($productOrder)){
                    logError("CRITICAL", "Product's order does not exist.", __FILE__, __LINE__, ["ipn" => $ipnData, "product" => $productId, "charge" => $chargeId]);
                    return false;
                }

                // if product pricing not found in our db
                if (is_null($productPricing)){
                    logError("critical", "Product setting's relation to product pricing table not found for plan from webhook does not exist.", __FILE__, __LINE__, ["ipn" => $ipnData, "product_pricing" => $productPricingId]);
                    return false;
                }

                // if order status is not cancelled
                if ($productOrder->status != ProductOrder::STATUS_CANCELLED){
                    // update order status
                    $productOrder->update(["status" => ProductOrder::STATUS_REFUNDED]);
                }

                // update transaction status
                $productOrder->transactions()
                            ->where("txn_id", $chargeId)
                            ->update([
                                "is_refunded" => true
                            ]);

                // delete member access for this product
                $this->deleteMemberProductAccess($productOrder);
                break;
            // onetime payment chargeback was made
            case "PAYMENT.CAPTURE.REVERSED":
                // get charge id
                $chargeId = $ipnData["resource"]["id"];

                // get product order via charge id
                $productOrder = $product->product_orders()->where("subscription_id", $chargeId)->first();
                // if product order not found
                if (is_null($productOrder)){
                    logError("CRITICAL", "Product's order does not exist.", __FILE__, __LINE__, ["ipn" => $ipnData, "product" => $productId, "charge" => $chargeId]);
                    return false;
                }

                // if product pricing not found in our db
                if (is_null($productPricing)){
                    logError("critical", "Product setting's relation to product pricing table not found for plan from webhook does not exist.", __FILE__, __LINE__, ["ipn" => $ipnData, "product_pricing" => $productPricingId]);
                    return false;
                }

                // update order status
                $productOrder->update(["status" => ProductOrder::STATUS_CHARGEBACK]);

                // create a transaction for order
                $transaction = $productOrder->transactions()->create([
                    "txn_id"          => $chargeId,
                    "trans_amount"    => $productPricing->price,
                    "trans_currency"  => $productPricing->price_currency,
                    "trans_date"      => date('Y-m-d H:i:s', strtotime($ipnData["resource"]["create_time"])),
                    "trans_gateway"   => "paypal",
                    "trans_type"      => "chargeback",
                    "is_rebill"       => 0,
                    "buyer_email"     => $productOrder->customer->cust_pay_email,
                    "ipn_hash"        => $ipnDataHash
                ]);

                // delete member access for this product
                $this->deleteMemberProductAccess($productOrder);
                break;
            // subscription cancelled or payment failed
            case "BILLING.SUBSCRIPTION.CANCELLED":
            case "BILLING.SUBSCRIPTION.PAYMENT.FAILED":
                // get transaction id
                $subscriptionId = $ipnData["resource"]["id"];

                // get product order via subscription
                $productOrder = $product->product_orders()->where("subscription_id", $subscriptionId)->first();
                // if product order not found
                if (is_null($productOrder)){
                    logError("CRITICAL", "Product's order does not exist.", __FILE__, __LINE__, ["ipn" => $ipnData, "product" => $productId, "subscription" => $subscriptionId]);
                    return false;
                }

                // if product setting not found in our db
                if (is_null($productPricing)){
                    logError("critical", "Product setting's relation to product pricing table not found for plan from webhook does not exist.", __FILE__, __LINE__, ["ipn" => $ipnData, "product_pricing" => $productPricingId]);
                    return false;
                }

                // update order status
                $productOrder->update(["status" => ProductOrder::STATUS_CANCELLED]);

                // create a transaction for order
                $transaction = $productOrder->transactions()->create([
                    "txn_id"          => $subscriptionId,
                    "trans_amount"    => $productPricing->price,
                    "trans_currency"  => $productPricing->price_currency,
                    "trans_date"      => date('Y-m-d H:i:s', strtotime($ipnData["resource"]["create_time"])),
                    "trans_gateway"   => "paypal",
                    "trans_type"      => "cancellation",
                    "is_rebill"       => 0,
                    "buyer_email"     => $productOrder->customer->cust_pay_email,
                    "ipn_hash"        => $ipnDataHash
                ]);

                // delete member access for this product
                $this->deleteMemberProductAccess($productOrder);
                break;
            // subscription payment refunded
            case "PAYMENT.SALE.REFUNDED":
                // if product pricing not found in our db
                if (is_null($productPricing)){
                    logError("critical", "Product setting's relation to product pricing table not found for plan from webhook does not exist.", __FILE__, __LINE__, ["ipn" => $ipnData, "product_pricing" => $productPricingId]);
                    return false;
                }

                // get charge id
                $chargeId = $ipnData["resource"]["sale_id"];

                // get transaction by charge id
                $transaction = Transaction::with("product_order")
                                        ->where("trans_gateway", "paypal")
                                        ->where("txn_id", $chargeId)
                                        ->where("is_refunded", 0)
                                        ->first();
                // if transaction not found
                if (is_null($transaction)){
                    logError("critical", "Product's order transaction does not exist", __FILE__, __LINE__, ["ipn" => $ipnData, "product" => $product->id, "charge" => $chargeId]);
                    return false;
                }

                // get product order via charge id
                $productOrder = $transaction->product_order;
                // if product order not found
                if (is_null($productOrder)){
                    logError("CRITICAL", "Product's order does not exist.", __FILE__, __LINE__, ["ipn" => $ipnData, "product" => $productId, "charge" => $chargeId]);
                    return false;
                }

                // update transaction status
                $transaction->update([
                    "is_refunded" => true
                ]);

                // delete member access for this product
                $this->deleteMemberProductAccess($productOrder);
                break;
            default:
                return false;
        }

        // check if user has over severity score for the product
        if (in_array($ipnTransactionType, ["PAYMENT.CAPTURE.REFUNDED", "PAYMENT.CAPTURE.REVERSED", "PAYMENT.SALE.REFUNDED"])) {
            // add BlacklistIncident job to queue
            dispatch(new BlacklistIncident(
                $ipnTransactionType == "PAYMENT.CAPTURE.REVERSED" ? "chargeback" : "refund",
                $productOrder->customer_id,
                $productOrder->amount)
            )->onQueue("blacklistIncidents");
        }

        // get product order's customer
        $productOrderCustomer = $productOrder->customer;
        // get product pricing's delivery
        $productDelivery = $productPricing->product_deliveries()->first();

        // if product's delivery for pricing not found
        if (is_null($productDelivery)){
            // send delivery missing method notification to product owner
            $this->sendDeliveryMethodMissingWarningNotification($product, $productOrderCustomer->cust_email);

            logError("CRITICAL", "Product pricing's delivery does not exist.", __FILE__, __LINE__, ["ipn" => $ipnData, "pricing" => $productPricing->id]);
            return false;
        }

        // handle purchase
        if (in_array($ipnTransactionType, ["CHECKOUT.ORDER.APPROVED", "PAYMENTS.PAYMENT.CREATED", "BILLING.SUBSCRIPTION.ACTIVATED", "BILLING.SUBSCRIPTION.UPDATED"])) {

            // get severity max score for the product
            $productSeverity = $product->getSetting("severity");
            // get blacklist incident for customer
            $blacklist = \Core\SmartProducts\Models\BlacklistIncident::where("_track_id", $productOrderCustomer->_track_id)->first();

            // if customer severity score is higher than max score for product , refuse to deliver product!
            if (is_null($blacklist) == false && is_null($productSeverity) == false && $blacklist->severity > $productSeverity->severity){
                // send email to customer about delivery refuse
                $this->sendDeliveryRefuseEmailToCustomer($productOrderCustomer->cust_email, $product);
                // send email to owner about delivery refuse
                $this->sendDeliveryRefuseEmailToProductOwner($productOrder, $product, $productOrderCustomer);
                // send notification to owner
                $this->sendDeliveryRefuseNotificationToProductOwner($product, $productOrderCustomer);

                logDebug("high", "Product pricing's delivery refused due to high severity score.", __FILE__, __LINE__, ["customer_score" => $blacklist->severity, "max_score" => $productSeverity->severity]);
                return false;
            }

            // update status for customer checkout session
            CheckoutAbandoned::where("customer_email", $productOrderCustomer->cust_email)
                            ->where("product_pricing_id", $productPricing->id)
                            ->update(["purchased" => 1]);

            // if pricing is only available to members
            if ($productPricing->availability == ProductPricing::AVAILABILITY_MEMBER){
                // check if customer is also member for product
                $checkIfCustomerIsMember = Member::select("members.id")
                                                ->leftJoin("membership_site", "membership_site.id", "=", "members.membership_site_id")
                                                ->leftJoin("membership_site_product", "membership_site_product.membership_site_id", "=", "membership_site.id")
                                                ->where("membership_site_product.product_id", $product->id)
                                                ->where("members.email", $productOrderCustomer->cust_email)
                                                ->first();

                if (is_null($checkIfCustomerIsMember)){
                    // send non member purchase email to Kyvian and in app notification
                    $this->sendNonMemberProductPurchaseEmailToKyvian($productOrder);
                    // send non member purchase email to customer
                    $this->sendNonMemberProductPurchaseEmailToMember($productOrder);

                    return false;
                }
            }

            // store payment provider customer used for payment in cache for 6 hours and use that provider on next payment attempts
            Cache::put("customer_latest_payment_provider_{$productOrderCustomer->id}", "paypal", Carbon::now()->addHours(6));

            // check if product is used as membership product
            $productMembershipSites = $product->membership_site_product;
            // if product does not have any relation to membership site
            if (count($productMembershipSites) && $product->product_type->product_type != ProductType::TYPE_MEMBERSHIP){
                $this->handleMembershipDelivery($productOrder);
            }

            // send in app notifications to product owner when stock left is less than 10
            if ($productPricing->stock_left == 9 || $productPricing->stock_left == 4 || $productPricing->stock_left == 1){
                // send stock left reminder to product owner
                $this->sendProductStockLeftReminderNotificationToProductOwner($product, $productPricing->stock_left);
            }
            elseif($productPricing->stock_left == 0){
                // send extra purchase notification to product owner
                $this->sendProductOutOfStockExtraPurchaseNotificationToProductOwner($productOrder);
            }

            // add conversion activity to ES
            $this->productOrderPurchaseActivityLog($productOrder, 2);

            // add delivery access id to order
            $productOrder->update(["deliv_accessid" => md5(date("Y-m-d H:i:s"))]);

            switch ($productDelivery->delivery_method){
                case ProductDelivery::DELIVERY_METHOD_MEMBERSHIP:
                    $this->handleMembershipDelivery($productOrder);
                    break;
                case ProductDelivery::DELIVERY_METHOD_EMAIL:
                    $this->sendProductDeliveryEmail($productDelivery, $productOrderCustomer, $productOrder);
                    break;
                case ProductDelivery::DELIVERY_METHOD_REDIRECT_URL:
                    $this->sendProductRedirectUrl($productDelivery, $productOrderCustomer);
                    break;
                case ProductDelivery::DELIVERY_METHOD_FILE_UPLOAD:
                    $this->sendProductDownloadUrl($productOrder);
                    break;
                case ProductDelivery::DELIVERY_METHOD_POST_NOTIFICATION:
                    // The KPDN is handled below on line 446 as KPDN is always send no matter if transaction type is SALE or anything else
                    break;
            }

            // if product's owner sale notification is enabled
            if($product->owner_sale_notification) {
                // send successful notification
                $this->sendOwnerSaleNotification($product);
            }

            // if purchase is for subscription product send receipt and subscription establishment emails to customer
            if ($ipnData["event_type"] == "BILLING.SUBSCRIPTION.ACTIVATED"){
                // send subscription receipt email to customer
                $this->sendSubscriptionReceiptEmailToCustomer($productOrder);
            }
        }

        // execute product actions
        $productActionHelper = new ProductActionHelper();
        $productActionHelper->executeProductAction($productOrder->customer_id, $productOrder->product_id, $productOrder->product_pricing_id, $transaction->trans_type ?? null);

        if ($productDelivery->delivery_method == ProductDelivery::DELIVERY_METHOD_POST_NOTIFICATION){
            // get the whole ipn data
            $kpdnData = json_decode($this->ipnLog["ipn_data"], true);

            // add product info to kpdn data
            if (isset($downgradedProductPricing)){
                $kpdnData["_prod_data"] = [
                    [
                        "product_order_id" => $productOrder->id,
                        "prod_name" => $product->name,
                        "old_price_variant_id" => $downgradedProductPricing->id,
                        "price_variant_id" => $productPricing->id
                    ]
                ];
            }
            else{
                $kpdnData["_prod_data"] = [
                    [
                        "product_order_id" => $productOrder->id,
                        "prod_name" => $product->name,
                        "price_variant_id" => $productPricing->id
                    ]
                ];
            }

            $kpdnData["headers"] = $this->params["request_headers"] ?? [];

            // handle KPDN as well for any type of transaction
            $this->sendKPDNEvent($kpdnData, $this->ipnLog->processor);
        }

        // invalidate product orders cache
        Cache::tags("{$productOrder->cacheKey}_{$product->user_id}")->flush();

        return true;
    }
}
