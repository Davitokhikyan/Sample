<?php

namespace Core\Jobs;

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
use Core\SmartProducts\Models\ProductPricingType;
use Core\SmartProducts\Models\ProductType;
use Core\SmartProducts\Models\Transaction;
use Core\Traits\APIClient;
use Core\Utils\ProductActionHelper;

/**
 * Class StripeIpnHandler
 * @package Core\Jobs
 */
class StripeIpnHandler extends BasePaymentProcessorHandler implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels, APIClient;

    /**
     * @var IpnRawLog
     */
    private $ipnLog;
    /**
     * @var array
     */
    private $params;


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
     * StripeIpnHandler constructor.
     * @param IpnRawLog $ipnRawLog
     * @param array $params
     */
    public function __construct(IpnRawLog $ipnRawLog , $params = [])
    {
        $this->ipnLog = $ipnRawLog;
        $this->params  = $params;
    }

    /**
     * Execute a job
     *
     * @return bool
     */
    public function handle()
    {
        // get transaction type of IPN
        $ipnTransactionType = $this->ipnLog["transaction_type"];

        $ipnDataHash = hash("sha256", $this->ipnLog["ipn_data"]);
        // decode IPN json data
        $ipnData = json_decode($this->ipnLog["ipn_data"], true);
        $ipnData = $ipnData["data"]["object"];

        // get custom IPN custom data
        $ipnCustomData = $this->params["metadata"];

        // get marketing consent and  track id values from custom IPN metadata
        $marketingConsent = $ipnCustomData["marketing_consent"] ?? null;
        $TrackId = $ipnCustomData["kti"] ?? null;

        $productId = $ipnCustomData["product_id"];
        $productPricingId = $ipnCustomData["product_pricing_id"];

        // get product in our db
        $product = Product::find($productId);
        // if product is not found stop the process
        if (is_null($product)) {
            logError(
                "critical",
                "IPN does not contain valid product id",
                __FILE__,
                __LINE__,
                ["ipn" => $ipnData, "product" => $productId]
            );
            return false;
        }

        // get product pricing for product based on currency and IPN transaction amount
        $productPricing = $product->product_pricing()
                                ->with("product_deliveries")
                                ->where("id", $productPricingId)
                                ->first();
        // if product has not pricing with IPN data"s transaction amount and currency stop the process
        if (is_null($productPricing)) {
            $customerEmail =  $ipnData["customer_email"] ?? $ipnData["billing_details"]["email"];
            $customerName = $ipnData["customer_name"] ?? $ipnData["billing_details"]["name"];
            $amount = $ipnData["amount"] ? $ipnData["amount"] / 100 : $ipnData["amount_paid"] / 100;
            // send notification to product owner that pricing was not found for product
            $this->sendMissingProductPricingNotificationToProductOwner("Stripe", $product, $amount, $ipnData["currency"], $customerEmail, $customerName);
            logError(
                "critical",
                "IPN does not contain valid product pricing id",
                __FILE__,
                __LINE__,
                ["ipn" => $ipnData, "product" => $product->id, "pricing" => $productPricingId, "amount" => $ipnData["amount"], "currency" => $ipnData["currency"]]
            );
            return false;
        }
        // Initially set to 0
        $isTest = 0;
        switch ($ipnTransactionType) {
            case "payment_intent.succeeded":
                $charge = $ipnData["charges"]["data"][0];
                // charge id from ipn
                $transactionId = $charge["id"];
                // amount is in cents so we need to convert it (100 cents = $1.00)
                $ipnData["amount"] = $charge["amount"] / 100;

                if(isset($ipnData['livemode']) && $ipnData['livemode'] == false){
                    // mark this as a test transaction
                    $isTest = 2;
                    // send notification to product owner for test tarnsaction
                    $this->sendTestTransactionNotificationToProductOwner("Stripe", $product->user_id, $ipnData);
                }
                elseif( !empty($ipnData['amount']) && $ipnData["amount"] < 5 ){
                    $isTest = 1;
                }
                // get customer details from IPN data
                $customerData = $charge["billing_details"];
                // extract customer's first and last names
                if (empty($customerData["name"])){
                    $customerFirstName = "Unknown";
                    $customerLastName = "";
                }
                else{
                    $customerNameParts = explode(" ", $customerData["name"]);
                    $customerFirstName = ucwords(strtolower($customerNameParts[0]));
                    $customerLastName = ucwords(strtolower($customerNameParts[1]));
                }

                // get customer by email
                $customer = Customer::where("cust_pay_email", $customerData["email"])->first();
                // if customer is not found create a new one
                if (is_null($customer)) {
                    // create a new customer
                    $customer = Customer::create([
                        "_track_id"        => $TrackId,
                        "cust_pay_email"        => $customerData["email"],
                        "cust_email"            => $customerData["email"],
                        "cust_fname"            => $customerFirstName,
                        "cust_lname"            => $customerLastName,
                        "cust_address1"         => $customerData["address"]["line1"],
                        "cust_city"             => $customerData["address"]["city"],
                        "cust_country"          => $customerData["address"]["country"],
                        "cust_zipcode"          => $customerData["address"]["postal_code"],
                        "stripe_customer_id"    => $ipnData["customer"]
                    ]);
                }
                else{
                    // update existing customer data
                    $customer->update([
                        "_track_id"        => $TrackId ?? $customer->_track_id,
                        "cust_email"            => $customerData["email"],
                        "cust_fname"            => $customerFirstName,
                        "cust_lname"            => $customerLastName,
                        "stripe_customer_id"    => $ipnData["customer"]
                    ]);
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
                    "payment_processor"       => $this->params["is_stripe_connect"] ? "stripe connect" : "stripe",
                    "amount"                  => $ipnData["amount"],
                    "currency"                => strtoupper($ipnData["currency"]),
                    "subscription_id"         => $transactionId,
                    "coupon_code"             => $couponCode,
                    "marketing_consent"       => $marketingConsent,
                    "is_test"                 => $isTest
                ]);

                // create a transaction for order
                $transaction = $productOrder->transactions()->create([
                    "txn_id"            => $transactionId,
                    "trans_amount"      => $ipnData["amount"],
                    "trans_currency"    => strtoupper($ipnData["currency"]),
                    "trans_date"        => gmdate("Y-m-d H:i:s", $ipnData["created"]),
                    "trans_gateway"     => $this->params["is_stripe_connect"] ? "stripe connect" : "stripe",
                    "trans_type"        => "purchase",
                    "is_rebill"         => 0,
                    "buyer_email"       => $customer->cust_pay_email,
                    "ipn_hash"          => $ipnDataHash,
                    "is_test"           => $isTest
                ]);

                // checks if the order is the first one for user and log activity in ES
                $this->firstProductOrderActivityLog($product->user_id, $productOrder);
                break;
            case "invoice.payment_succeeded";
                if(isset($ipnData['livemode']) && $ipnData['livemode'] == false){
                    // mark this as a test transaction
                    $isTest = 2;
                    // send notification to product owner for test tarnsaction
                    $this->sendTestTransactionNotificationToProductOwner("Stripe", $product->user_id, $ipnData);
                }
                elseif( !empty($ipnData['amount']) && $ipnData["amount"] < 5 ){
                    $isTest = 1;
                }
                // subscription id from ipn
                $subscriptionId = $ipnData["subscription"];
                // charge id from ipn
                $transactionId = $ipnData["charge"] ?? $ipnData["id"];
                // amount is in cents so we need to convert it (100 cents = $1.00)
                $ipnData["amount"] = $ipnData["amount_paid"] / 100;
                // get customer details from IPN data
                $customerData = [
                    "email" => $ipnData["customer_email"],
                    "name"  => $ipnData["customer_name"],
                    "address" => [
                        "line1" => $ipnData["customer_address"],
                        "city" => null,
                        "country" => null,
                        "postal_code" => null,
                    ]
                ];

                // set a flag to see if we need to handle the delivery
                $isRebill = false;
                // set flag to define if event is for subscription upgrade or downgrade
                $isUpgradeOrDowngrade = false;

                // if event is for rebill payment
                if ($ipnData["billing_reason"] == "subscription_cycle"){
                    $isRebill = true;
                }
                // if event is for upgrade or downgrade payment
                elseif ($ipnData["billing_reason"] == "subscription_update"){
                    $isUpgradeOrDowngrade = true;
                }

                // if event is for rebill payment
                if ($isRebill){
                    // get product order by subscription id
                    $productOrder = ProductOrder::with("customer", "product", "product_pricing")
                                                ->where("subscription_id", $subscriptionId)
                                                ->latest("created_at")
                                                ->first();
                    // if product order not found
                    if (is_null($productOrder)){
                        logError(
                            "critical",
                            "Product's order does not exist",
                            __FILE__,
                            __LINE__,
                            ["ipn" => $ipnData, "product" => $product->id, "subscription" => $subscriptionId]
                        );
                        return false;
                    }

                    // update order
                    $productOrder->update([
                        "status" => ProductOrder::STATUS_COMPLETED,
                        "amount" => $ipnData["amount"]
                    ]);

                    // create a transaction for order
                    $transaction = $productOrder->transactions()->create([
                        "txn_id"            => $transactionId,
                        "trans_amount"      => $ipnData["amount"],
                        "trans_currency"    => strtoupper($ipnData["currency"]),
                        "trans_date"        => gmdate("Y-m-d H:i:s", $ipnData["created"]),
                        "trans_gateway"     => $this->params["is_stripe_connect"] ? "stripe connect" : "stripe",
                        "trans_type"        => "rebill",
                        "is_rebill"         => 1,
                        "buyer_email"       => $productOrder->customer->cust_pay_email,
                        "ipn_hash"          => $ipnDataHash,
                        "is_test"           => $isTest
                    ]);

                    // send subscription receipt to customer with subscription info
                    $this->sendSubscriptionEstablishmentEmailToCustomer($productOrder);
                }
                else{
                    // if payment is for upgrade or downgrade event
                    if ($isUpgradeOrDowngrade){
                        // get product order by subscription id
                        $productOrder = ProductOrder::with("customer", "product_pricing")
                                                    ->where("subscription_id", $subscriptionId)
                                                    ->latest("created_at")
                                                    ->first();
                        // if product order not found
                        if (is_null($productOrder)){
                            logError(
                                "critical",
                                "Product's order does not exist",
                                __FILE__,
                                __LINE__,
                                ["ipn" => $ipnData, "product" => $product->id, "subscription" => $subscriptionId]
                            );
                            return false;
                        }

                        // keep old pricing id to send it along with KPDN data
                        $oldProductPricingId = $productOrder->product_pricing_id;

                        // update order
                        $productOrder->update([
                            "status"                => ProductOrder::STATUS_COMPLETED,
                            "amount"                => $ipnData["amount"],
                            "product_id"            => $product->id,
                            "product_pricing_id"    => $productPricing->id
                        ]);

                        // create a transaction for order
                        $transaction = $productOrder->transactions()->create([
                            "txn_id"            => $transactionId,
                            "trans_amount"      => $ipnData["amount"],
                            "trans_currency"    => strtoupper($ipnData["currency"]),
                            "trans_date"        => gmdate("Y-m-d H:i:s", $ipnData["created"]),
                            "trans_gateway"     => $this->params["is_stripe_connect"] ? "stripe connect" : "stripe",
                            "trans_type"        => $productOrder->product_pricing->price < $productPricing->price ? "upgrade" : "downgrade",
                            "is_rebill"         => 0,
                            "buyer_email"       => $productOrder->customer->cust_pay_email,
                            "ipn_hash"          => $ipnDataHash,
                            "is_test"           => $isTest
                        ]);
                    }
                    else{
                        // extract customer's first and last names
                        if (empty($customerData["name"])){
                            $customerFirstName = "Unknown";
                            $customerLastName = "";
                        }
                        else{
                            $customerNameParts = explode(" ", $customerData["name"]);
                            $customerFirstName = ucwords(strtolower($customerNameParts[0]));
                            $customerLastName = ucwords(strtolower($customerNameParts[1]));
                        }

                        // get customer by email
                        $customer = Customer::where("cust_pay_email", $customerData["email"])->first();
                        // if customer is not found create a new one
                        if (is_null($customer)) {
                            // create a new customer
                            $customer = Customer::create([
                                "_track_id"        => $TrackId,
                                "cust_pay_email"        => $customerData["email"],
                                "cust_email"            => $customerData["email"],
                                "cust_fname"            => $customerFirstName,
                                "cust_lname"            => $customerLastName,
                                "cust_address1"         => $customerData["address"]["line1"],
                                "cust_city"             => $customerData["address"]["city"],
                                "cust_country"          => $customerData["address"]["country"],
                                "cust_zipcode"          => $customerData["address"]["postal_code"],
                                "stripe_customer_id"    => $ipnData["customer"]
                            ]);
                        }
                        else{
                            // update existing customer data
                            $customer->update([
                                "_track_id"        => $TrackId ?? $customer->_track_id,
                                "cust_email"            => $customerData["email"],
                                "cust_fname"            => $customerFirstName,
                                "cust_lname"            => $customerLastName,
                                "stripe_customer_id"    => $ipnData["customer"]
                            ]);
                        }

                        // invalidate customers cache
                        Cache::tags("{$customer->cacheKey}_{$product->user_id}")->flush();

                        $couponCode = null;
                        // if coupon is used
                        if (isset($ipnData["discount"]["coupon"]["id"])){
                            $couponCode = $ipnData["discount"]["coupon"]["id"];
                            // update `used` value of product coupon
                            $productCoupon = $this->updateCouponRemainingCount($couponCode);
                            // if coupon is valid
                            if ($productCoupon){
                                // apply coupon on pricing
                                $productPricing->applyCoupon($productCoupon->id);
                            }
                        }

                        // create a product order
                        $productOrder = ProductOrder::create([
                            "customer_id"             => $customer->id,
                            "product_id"              => $product->id,
                            "product_pricing_id"      => $productPricing->id,
                            "product_pricing_type_id" => $productPricing->product_pricing_type_id,
                            "status"                  => ProductOrder::STATUS_COMPLETED,
                            "payment_processor"       => $this->params["is_stripe_connect"] ? "stripe connect" : "stripe",
                            "amount"                  => $productPricing->trial_price > 0 ? $productPricing->trial_price : $ipnData["amount"],
                            "currency"                => strtoupper($ipnData["currency"]),
                            "subscription_id"         => $subscriptionId,
                            "coupon_code"             => $couponCode,
                            "marketing_consent"       => $marketingConsent,
                            "is_test"                 => $isTest
                        ]);

                        // create a transaction for order
                        $transaction = $productOrder->transactions()->create([
                            "txn_id"            => $transactionId,
                            "trans_amount"      => $productPricing->trial_price > 0 ? $productPricing->trial_price : $ipnData["amount"],
                            "trans_currency"    => strtoupper($ipnData["currency"]),
                            "trans_date"        => gmdate("Y-m-d H:i:s", $ipnData["created"]),
                            "trans_gateway"     => $this->params["is_stripe_connect"] ? "stripe connect" : "stripe",
                            "trans_type"        => "purchase",
                            "is_rebill"         => 0,
                            "buyer_email"       => $productOrder->customer->cust_pay_email,
                            "ipn_hash"          => $ipnDataHash,
                            "is_test"           => $isTest
                        ]);

                        // checks if the order is the first one for user and log activity in ES
                        $this->firstProductOrderActivityLog($product->user_id, $productOrder);
                    }
                }

                // This will be a stripe connect ipn
                if($this->params["is_stripe_connect"]){
                    $aaData = [
                        "user_id" => $product->user_id,
                        "trans_date" => Carbon::now()->toDateString(),
                        "trans_type" => "payment_succeeded",
                        "trans_id" => $ipnData["id"],
                        "trans_currency" => strtoupper($ipnData["currency"]),
                        "trans_amount" =>  number_format((float)($ipnData["application_fee_amount"]/100), 2, ".", ""),
                        "buyer_email" => $productOrder->customer->cust_email,
                        "buyer_first_name" => $productOrder->customer->cust_fname,
                        "buyer_last_name" => $productOrder->customer->cust_lname
                    ];
                    $aaRequest = $this->makeJsonPostRequest(config("config.app.aa_api_url") . "transactions/stripe-connect-split-payment" , null , $aaData);
                    if(empty($aaRequest["data"]) && empty($aaRequest["data"]["id"])){
                        logError("CRITICAL", "Issue in updating stripe connect amount to AA.", __FILE__, __LINE__, ["response" => $aaRequest, "data" => $aaData]);
                    }
                }
                break;
            case "charge.refunded";
                // charge id from ipn
                $transactionId = $ipnData["id"];
                // amount is in cents so we need to convert it (100 cents = $1.00)
                $ipnData["amount"] = $ipnData["amount"] / 100;
                $ipnData["amount_refunded"] = $ipnData["amount_refunded"] / 100;

                // get transaction by charge id
                $transaction = Transaction::where("trans_gateway", "stripe")
                                        ->where("txn_id", $transactionId)
                                        ->where("is_refunded", 0)
                                        ->first();
                // if transaction not found
                if (is_null($transaction)){
                    logError(
                        "critical",
                        "Product's order transaction does not exist",
                        __FILE__,
                        __LINE__,
                        ["ipn" => $ipnData, "product" => $product->id, "charge" => $transactionId]
                    );
                    return false;
                }

                // get product order
                $productOrder = $transaction->product_order;
                // if product order not found
                if (is_null($productOrder)){
                    logError(
                        "critical",
                        "Product's order does not exist",
                        __FILE__,
                        __LINE__,
                        ["ipn" => $ipnData, "product" => $product->id, "charge" => $transactionId]
                    );
                    return false;
                }

                if ($transaction->trans_type == "purchase"){
                    // if it gets refunded and order amount is not equal to refund amount means partial refund
                    if ($productOrder->amount == $ipnData["amount_refunded"]){
                        // update product order status
                        $productOrder->update([
                            "status" => ProductOrder::STATUS_REFUNDED
                        ]);
                    }
                    else {
                        // update product order status
                        $productOrder->update([
                            "status" => ProductOrder::STATUS_PARTIAL_REFUND
                        ]);
                    }
                }

                // update transaction status
                $transaction->update([
                    "is_refunded" => true
                ]);

                // delete member access for this product
                $this->deleteMemberProductAccess($productOrder);
                break;
            case "customer.subscription.deleted";
                // subscription id from ipn
                $subscriptionId = $ipnData["id"];
                // amount is in cents so we need to convert it (100 cents = $1.00)
                $ipnData["amount"] = $ipnData["plan"]["amount"] / 100;
                $ipnData["currency"] = $ipnData["plan"]["currency"];

                // get product order by subscription id
                $productOrder = ProductOrder::with("customer")
                                            ->where("subscription_id", $subscriptionId)
                                            ->latest("created_at")
                                            ->first();
                // if product order  not found
                if (is_null($productOrder)){
                    logError(
                        "critical",
                        "Product's order does not exist",
                        __FILE__,
                        __LINE__,
                        ["ipn" => $ipnData, "product" => $product->id, "subscription" => $subscriptionId]
                    );
                    return false;
                }

                // update order status
                $productOrder->update([
                    "status" => ProductOrder::STATUS_CANCELLED
                ]);

                // create a transaction for order
                $transaction = $productOrder->transactions()->create([
                    "txn_id"            => $subscriptionId,
                    "trans_amount"      => $ipnData["amount"],
                    "trans_currency"    => strtoupper($ipnData["currency"]),
                    "trans_date"        => gmdate("Y-m-d H:i:s", $ipnData["created"]),
                    "trans_gateway"     => $this->params["is_stripe_connect"] ? "stripe connect" : "stripe",
                    "trans_type"        => "cancellation",
                    "is_rebill"         => 0,
                    "buyer_email"       => $productOrder->customer->cust_pay_email,
                    "ipn_hash"          => $ipnDataHash
                ]);

                // delete member access for this product
                $this->deleteMemberProductAccess($productOrder);
                break;
            default:
                return false;
                break;
        }

        // check if user has over severity score for the product
        if ($ipnTransactionType == "charge.refunded") {
            // add BlacklistIncident job to queue
            dispatch(new BlacklistIncident("refund", $productOrder->customer_id, $ipnData["amount_refunded"]))->onQueue("blacklistIncidents");
        }

        // get product's pricing delivery method
        $productDelivery = $productPricing->product_deliveries()->first();
        // if product"s delivery for pricing not found
        if (is_null($productDelivery)){
            $this->sendDeliveryMethodMissingWarningNotification($product , $customerData["email"]);
            logError(
                "critical",
                "Product pricing's delivery does not exist",
                __FILE__,
                __LINE__,
                ["ipn" => $ipnData, "pricing" => $productPricing->id]
            );
            return false;
        }

        // handle delivery for sale (all methods except KPDN)
        if ($ipnTransactionType == "payment_intent.succeeded" || ( $ipnTransactionType == "invoice.payment_succeeded" && $isRebill == false )){

            // get severity max score for the product
            $productSeverity = $product->getSetting("severity");
            // get product order"s customer"s  track id
            $productOrderCustomer = $productOrder->customer;
            // get blacklist incident for customer
            $blacklist = \Core\SmartProducts\Models\BlacklistIncident::where("_track_id" , $productOrderCustomer->_track_id)->first();

            // if customer severity score is higher than max score for product , refuse to deliver product!
            if (is_null($blacklist) == false && is_null($productSeverity) == false && $blacklist->severity > $productSeverity->severity){
                // send email to customer about delivery refuse
                $this->sendDeliveryRefuseEmailToCustomer($productOrderCustomer->cust_email , $product);
                // send email to owner about delivery refuse
                $this->sendDeliveryRefuseEmailToProductOwner($productOrder, $product, $productOrderCustomer);
                // send notification to owner
                $this->sendDeliveryRefuseNotificationToProductOwner($product, $productOrderCustomer);

                logDebug(
                    "high",
                    "Product pricing's delivery refused due to high severity score",
                    __FILE__,
                    __LINE__,
                    ["customer_score" => $blacklist->severity, "max_score" => $productSeverity->severity]
                );
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
            Cache::put("customer_latest_payment_provider_{$productOrderCustomer->id}", "stripe", Carbon::now()->addHours(6));

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
            $productOrder->update([
                "deliv_accessid" => md5(date("Y-m-d H:i:s"))
            ]);

            switch ($productDelivery->delivery_method) {
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
                    // The KPDN is handled below on line 354 as KPDN is always send no matter if transaction type is SALE or anything else
                    break;
            }

            // if products owner sale notification is enabled send notification to owner
            if ($product->owner_sale_notification) {
                // send successful notification
                $this->sendOwnerSaleNotification($product);
            }

            // if purchase is for subscription product send receipt email to customer
            if ($ipnTransactionType == "invoice.payment_succeeded"){
                $this->sendSubscriptionReceiptEmailToCustomer($productOrder);
            }

            // if cross sell pricings are included
            if (isset($ipnCustomData["cross_sell_pricing_id"])) {
                // get cross sell pricing id values
                $appliedCrossSellPricingIdValues = json_decode($ipnCustomData["cross_sell_pricing_id"], true);

                if (empty($appliedCrossSellPricingIdValues) == false) {
                    // get cross sell pricings
                    $productAppliedCrossSellPricings = $product->product_pricing()
                        ->with("product_deliveries")
                        ->whereIn("id", $appliedCrossSellPricingIdValues)
                        ->get();

                    foreach ($productAppliedCrossSellPricings as $crossSellPricing) {
                        // get pricing delivery
                        $crossSellPricingDelivery = $crossSellPricing->product_deliveries()->first();
                        // if delivery is not set for pricing
                        if (is_null($crossSellPricingDelivery)) {
                            $this->sendDeliveryMethodMissingWarningNotification($product, $productOrderCustomer->cust_email);
                            logError(
                                "critical",
                                "Product pricing's delivery does not exist",
                                __FILE__,
                                __LINE__,
                                ["ipn" => $ipnData, "pricing" => $crossSellPricing->id]
                            );
                            continue;
                        }

                        // handle delivery
                        switch ($crossSellPricingDelivery->delivery_method) {
                            case ProductDelivery::DELIVERY_METHOD_MEMBERSHIP:
                                $this->handleMembershipDelivery($productOrder);
                                break;
                            case ProductDelivery::DELIVERY_METHOD_EMAIL:
                                $this->sendProductDeliveryEmail($crossSellPricingDelivery, $productOrderCustomer, $productOrder);
                                break;
                            case ProductDelivery::DELIVERY_METHOD_REDIRECT_URL:
                                $this->sendProductRedirectUrl($crossSellPricingDelivery, $productOrderCustomer);
                                break;
                            case ProductDelivery::DELIVERY_METHOD_FILE_UPLOAD:
                                $this->sendProductDownloadUrl($productOrder);
                                break;
                        }
                    }
                }
            }
        }

        // execute product actions
        $productActionHelper = new ProductActionHelper();
        $productActionHelper->executeProductAction($productOrder->customer_id, $productOrder->product_id, $productOrder->product_pricing_id, $transaction->trans_type);

        // if delivery method is KPDN
        if ($productDelivery->delivery_method == ProductDelivery::DELIVERY_METHOD_POST_NOTIFICATION) {
            // get the whole ipn data
            $kpdnData = json_decode($this->ipnLog["ipn_data"], true);

            // add product info to kpdn data
            if (isset($isUpgradeOrDowngrade) && $isUpgradeOrDowngrade == true && isset($oldProductPricingId)){
                $kpdnData["_prod_data"] = [
                    [
                        "product_order_id" => $productOrder->id,
                        "prod_name" => $product->name,
                        "old_price_variant_id" => $oldProductPricingId,
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

            // handle KPDN as well for any type of transaction
            $this->sendKPDNEvent($kpdnData, $this->ipnLog->processor);
        }

        // invalidate product orders cache
        Cache::tags("{$productOrder->cacheKey}_{$product->user_id}")->flush();

        return true;
    }
}