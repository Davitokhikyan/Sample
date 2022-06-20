<?php

namespace Core\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Queue\SerializesModels;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Support\Facades\Log;
use Core\Payments\Paddle;
use Core\Payments\PayPalSubscription;
use Core\Payments\Stripe;
use Core\Payments\StripeConnect;
use Core\SmartProducts\Models\ProductPricing;
use Core\Traits\ActiveIntegrationsTrait;
use Core\Traits\APIClient;

/**
 * This job will be called after creating a new product via CreateProductMutation.
 * It's responsible for creating a product/billing plan/billing agreement in Stripe,PayPal and Paddle via API
 * in case if the product owner has those integration's setup for the account.
 * If account does not have any of those integrations nothing will happen here.
 *
 * Class CreateProductForPP
 * @package Core\Jobs
 */
class CreateProductForPP implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels, APIClient, ActiveIntegrationsTrait;

    private $product;

    /**
     * CreateProductForPP constructor.
     * @param $product
     *
     */
    public function __construct($product)
    {
        $this->product = $product;
    }

    /**
     * @return mixed
     */
    public function getProduct(){
        return $this->product;
    }

    /**
     * Execute the job.
     *
     * @return bool
     */
    public function handle()
    {
        $product = $this->product;
        $productPricings = $product->product_pricing_subscription_type;

        // if there are no proper pricing stop the process
        if (count($productPricings) == 0){
            logDebug("high", "Product does not have subscription pricing.", __FILE__, __LINE__, ["product" => $product->id]);
            return false;
        }

        // get product's active payment integrations
        $productPaymentMethodActiveIntegrations = $this->getProductPaymentActiveIntegrations($product);

        if (is_null($productPaymentMethodActiveIntegrations["message"]) == false){
            logError("high", "No product payment method active integration.", __FILE__, __LINE__, ["product" => $product->id, "error" => $productPaymentMethodActiveIntegrations["message"]]);
            return false;
        }

        // get user's Stripe active payment integration data based on integration id
        $stripeActiveIntegrationData = $this->getActiveIntegrationDataByIntegrationName($productPaymentMethodActiveIntegrations, "Stripe");
        // if user has Stripe setup for product
        if ($stripeActiveIntegrationData){
            // create products in Stripe
            $this->createProductsInStripe($product, $productPricings, $stripeActiveIntegrationData);
        }

        // get user's Paypal active payment integration data based on integration id
        $paypalActiveIntegrationData = $this->getActiveIntegrationDataByIntegrationName($productPaymentMethodActiveIntegrations, "Paypal");
        // if user has Paypal setup for product
        if ($paypalActiveIntegrationData){
            // create products in Paypal
            $this->createProductsInPaypal($product, $productPricings, $paypalActiveIntegrationData);
        }

        // get user's Paddle active payment integration data based on integration id
        $paddleActiveIntegrationData = $this->getActiveIntegrationDataByIntegrationName($productPaymentMethodActiveIntegrations, "Paddle");
        // if user has Paddle setup for product
        if ($paddleActiveIntegrationData){
            // create products in Paddle
            $this->createProductsInPaddle($product, $productPricings, $paddleActiveIntegrationData);
        }

        // get user's Stripe Connect active payment integration data based on integration id
        $stripeConnectActiveIntegrationData = $this->getActiveIntegrationDataByIntegrationName($productPaymentMethodActiveIntegrations, "Stripe Connect");
        // if user has Stripe Connect setup for product
        if ($stripeConnectActiveIntegrationData){
            // create products in Stripe Connect
            $this->createProductsInStripeConnect($product, $productPricings, $stripeConnectActiveIntegrationData);
        }

        return true;
    }

    /**
     * Function to create product plans in Stripe
     *
     * @param $product
     * @param $productPricings
     * @param $stripeActiveIntegrationData
     *
     * @return bool
     */
    private function createProductsInStripe($product, $productPricings, $stripeActiveIntegrationData)
    {
        $stripeSecretKey = $this->getActiveIntegrationDataValuesByKey($stripeActiveIntegrationData, "SECRET_KEY");

        if (is_null($stripeSecretKey)){
            logError("high", "User Stripe integration does not have secret key credentials set for the product.", __FILE__, __LINE__, ["product" => $product->id, "integrationData" => $stripeActiveIntegrationData]);
            // create in app notification
            $this->notifyUser(
                __("notification.productIntegrationDataIsMissingTitle", ["integrationName" => "Stripe", "productName" => $product->name]),
                __("notification.productIntegrationDataIsMissingContent", ["integrationName" => "Stripe", "productName" => $product->name])
            );
            return false;
        }

        $processorClient = new Stripe($stripeSecretKey);

        try {
            // create a product in stripe
            $stripeProduct = $processorClient->createProduct([
                "name" => $product->name,
                "type" => "service",
                "active" => true,
                "description" => $product->description,
            ]);
        }
        catch (\Exception $e){
            logError("high", "Error while creating product in Stripe via API." , __FILE__, __LINE__, ["product" => $product->id, "error" => $e->getMessage()]);
            // create a flag to indicate the setup failed
            $product->product_settings()->create(["key" => "stripe_setup_failed", "value" => 1]);
            // create in app notification
            $this->notifyUser(
                __("notification.productCreationViaApiFailedTitle", ["paymentProvider" => "Stripe"]),
                __("notification.productCreationViaApiFailedContent", ["paymentProvider" => "Stripe", "productName" => $product->name])
            );
            return false;
        }

        // if creating product failed
        if (isset($stripeProduct["id"]) == false){
            logError("high", "Error while creating product in Stripe via API." , __FILE__, __LINE__, ["product" => $product->id, "error" => $stripeProduct]);
            // create a flag to indicate the setup failed
            $product->product_settings()->create(["key" => "stripe_setup_failed", "value" => 1]);
            // create in app notification
            $this->notifyUser(
                __("notification.productCreationViaApiFailedTitle", ["paymentProvider" => "Stripe"]),
                __("notification.productCreationViaApiFailedContent", ["paymentProvider" => "Stripe", "productName" => $product->name])
            );
            return false;
        }

        // store stripe product id in our side
        $product->product_settings()->create(["key" => "stripe_product_id", "value" => $stripeProduct["id"]]);

        foreach ($productPricings as $key => $productPricing){
            // prepare plan data
            $planData = [
                "amount" => $productPricing->price,
                "currency" => $productPricing->price_currency,
                "interval" => $productPricing->billing_period_frequency,
                "trial_period_days" =>  $productPricing->has_trial ? $productPricing->trial_duration_days : 0,
                "product"  => $stripeProduct["id"],
                "metadata" => [
                    "product_id" => $product->id,
                    "product_pricing_id" => $productPricing->id
                ]
            ];

            if ($productPricing->billing_period == ProductPricing::BILLING_PERIOD_QUARTERLY){
                $planData["interval_count"] = 3;
            }
            elseif ($productPricing->billing_period == ProductPricing::BILLING_PERIOD_BI_YEARLY){
                $planData["interval_count"] = 2;
            }

            try {
                // create a plan in stripe
                $stripePlan = $processorClient->createPlan($planData);
            }
            catch (\Exception $e){
                logError("high", "Error while creating plan in Stripe via API." , __FILE__, __LINE__, ["product" => $product->id, "error" => $e->getMessage()]);
                // create a flag to indicate the setup failed
                $product->product_settings()->create(["key" => "stripe_setup_failed", "value" => 1]);
                // create in app notification
                $this->notifyUser(
                    __("notification.pricingPlanCreationViaApiFailedTitle", ["paymentProvider" => "Stripe", "productName" => $product->name]),
                    __("notification.pricingPlanCreationViaApiFailedContent", ["paymentProvider" => "Stripe", "productName" => $product->name])
                );
                continue;
            }

            // if creating plan failed
            if (isset($stripePlan["id"]) == false){
                logError("high", "Error while creating plan in Stripe via API." , __FILE__, __LINE__, ["product" => $product->id, "error" => $stripePlan]);
                // create a flag to indicate the setup failed
                $product->product_settings()->create(["key" => "stripe_setup_failed", "value" => 1]);
                // create in app notification
                $this->notifyUser(
                    __("notification.pricingPlanCreationViaApiFailedTitle", ["paymentProvider" => "Stripe", "productName" => $product->name]),
                    __("notification.pricingPlanCreationViaApiFailedContent", ["paymentProvider" => "Stripe", "productName" => $product->name])
                );
                continue;
            }

            // store stripe plan id in our side
            $product->product_settings()->create(["product_pricing_id" => $productPricing->id, "key" => "stripe_plan_id", "value" => $stripePlan["id"]]);
        }

        return true;
    }

    /**
     * Function to create product plans in Paypal
     *
     * @param $product
     * @param $productPricings
     * @param $paypalActiveIntegrationData
     *
     * @return bool
     */
    private function createProductsInPaypal($product, $productPricings, $paypalActiveIntegrationData)
    {
        $paypalClientId = $this->getActiveIntegrationDataValuesByKey($paypalActiveIntegrationData, "CLIENT_ID");
        $paypalSecret = $this->getActiveIntegrationDataValuesByKey($paypalActiveIntegrationData, "SECRET");
        $sandboxMode = $this->getActiveIntegrationDataValuesByKey($paypalActiveIntegrationData, "SANDBOX_MODE");

        if (is_null($paypalClientId) || is_null($paypalSecret)){
            logError("high", "User Paypal integration does not have secret key credentials set for the product.", __FILE__, __LINE__, ["product" => $product->id, "integrationData" => $paypalActiveIntegrationData]);
            // create in app notification
            $this->notifyUser(
                __("notification.productIntegrationDataIsMissingTitle", ["integrationName" => "Paypal", "productName" => $product->name]),
                __("notification.productIntegrationDataIsMissingContent", ["integrationName" => "Paypal", "productName" => $product->name])
            );
            return false;
        }

        $processorClient = new PayPalSubscription($paypalClientId, $paypalSecret, $sandboxMode);

        try{
            $paypalProduct = $processorClient->createProduct([
                "name" => $product->name,
                "description" => strlen($product->description) > 50 ? substr($product->description, 0, 50) . "..." : $product->description,
                "type" => "DIGITAL"
            ]);
        }
        catch (\Exception $e){
            logError("high", "Error while creating product in Paypal via API." , __FILE__, __LINE__, ["product" => $product->id, "error" => $e->getMessage()]);
            // create a flag to indicate the setup failed
            $product->product_settings()->create(["key" => "paypal_setup_failed", "value" => 1]);
            // create in app notification
            $this->notifyUser(
                __("notification.productCreationViaApiFailedTitle", ["paymentProvider" => "Paypal"]),
                __("notification.productCreationViaApiFailedContent", ["paymentProvider" => "Paypal", "productName" => $product->name])
            );
            return false;
        }

        // if creating product failed
        if (isset($paypalProduct["id"]) == false){
            logError("high", "Error while creating product in Paypal via API." , __FILE__, __LINE__, ["product" => $product->id, "error" => $paypalProduct]);
            // create a flag to indicate the setup failed
            $product->product_settings()->create(["key" => "paypal_setup_failed", "value" => 1]);
            // create in app notification
            $this->notifyUser(
                __("notification.productCreationViaApiFailedTitle", ["paymentProvider" => "Paypal"]),
                __("notification.productCreationViaApiFailedContent", ["paymentProvider" => "Paypal", "productName" => $product->name])
            );
            return false;
        }

        // store paypal product id in our side
        $product->product_settings()->create(["key" => "paypal_product_id", "value" => $paypalProduct["id"]]);

        foreach ($productPricings as $key => $productPricing) {
            $billingCycles = [];

            if ($productPricing->has_trial) {
                $billingCycles[] = [
                    "tenure_type" => "TRIAL",
                    "sequence" => 1,
                    "frequency" => [
                        "interval_unit" => "DAY",
                        "interval_count" => $productPricing->trial_duration_days,
                    ],
                    "pricing_scheme" => [
                        "fixed_price" => [
                            "value" => $productPricing->trial_price,
                            "currency_code" => $productPricing->price_currency
                        ]
                    ]
                ];
            }

            $billingCycles[] = [
                "tenure_type" => "REGULAR",
                "sequence" => $productPricing->has_trial ? 2 : 1,
                "frequency" => [
                    "interval_unit" => strtoupper($productPricing->billing_period_frequency),
                    "interval_count" => $productPricing->billing_period_frequency_interval,
                ],
                "total_cycles" => $productPricing->rebill_times,
                "pricing_scheme" => [
                    "fixed_price" => [
                        "value" => $productPricing->price,
                        "currency_code" => $productPricing->price_currency
                    ]
                ]
            ];

            try {
                $paypalPlan = $processorClient->createPlan([
                    "product_id" => $paypalProduct["id"],
                    "name" => $product->name,
                    "description" => strlen($product->description) > 50 ? substr($product->description, 0, 50) . "..." : $product->description,
                    "status" => "ACTIVE",
                    "billing_cycles" => $billingCycles,
                    "payment_preferences" => [
                        "setup_fee" => [
                            "value" => 0,
                            "currency_code" => $productPricing->price_currency
                        ]
                    ]
                ]);
            }
            catch (\Exception $e) {
                logError("high", "Error while creating plan in Paypal via API.", __FILE__, __LINE__, ["product" => $product->id, "error" => $e->getMessage()]);
                // create a flag to indicate the setup failed
                $product->product_settings()->create(["key" => "paypal_setup_failed", "value" => 1]);
                // create in app notification
                $this->notifyUser(
                    __("notification.pricingPlanCreationViaApiFailedTitle", ["paymentProvider" => "Paypal", "productName" => $product->name]),
                    __("notification.pricingPlanCreationViaApiFailedContent", ["paymentProvider" => "Paypal", "productName" => $product->name])
                );
                continue;
            }

            // if creating plan failed
            if (isset($paypalPlan["id"]) == false){
                logError("high", "Error while creating plan in Paypal via API.", __FILE__, __LINE__, ["product" => $product->id, "error" => $paypalPlan]);
                // create a flag to indicate the setup failed
                $product->product_settings()->create(["key" => "paypal_setup_failed", "value" => 1]);
                // create in app notification
                $this->notifyUser(
                    __("notification.pricingPlanCreationViaApiFailedTitle", ["paymentProvider" => "Paypal", "productName" => $product->name]),
                    __("notification.pricingPlanCreationViaApiFailedContent", ["paymentProvider" => "Paypal", "productName" => $product->name])
                );
                continue;
            }

            // store paypal plan id in our side
            $product->product_settings()->create(["product_pricing_id" => $productPricing->id, "key" => "paypal_plan_id", "value" => $paypalPlan["id"]]);
        }

        return true;
    }

    /**
     * Function to create product plans in Paddle
     *
     * @param $product
     * @param $productPricings
     * @param $paddleActiveIntegrationData
     *
     * @return bool
     */
    private function createProductsInPaddle($product, $productPricings, $paddleActiveIntegrationData)
    {
        $paddleVendorId = $this->getActiveIntegrationDataValuesByKey($paddleActiveIntegrationData, "VENDOR_ID");
        $paddleVendorAuth = $this->getActiveIntegrationDataValuesByKey($paddleActiveIntegrationData, "VENDOR_CODE");

        if (is_null($paddleVendorId) || is_null($paddleVendorAuth)){
            logError("high", "User Paddle integration does not have secret key credentials set for the product.", __FILE__, __LINE__, ["product" => $product->id, "integrationData" => $paddleActiveIntegrationData]);
            // create in app notification
            $this->notifyUser(
                __("notification.productIntegrationDataIsMissingTitle", ["integrationName" => "Paddle", "productName" => $product->name]),
                __("notification.productIntegrationDataIsMissingContent", ["integrationName" => "Paddle", "productName" => $product->name])
            );
            return false;
        }

        $processorClient = new Paddle($paddleVendorId, $paddleVendorAuth);

        foreach ($productPricings as $key => $productPricing){
            try{
                // create plan in Paddle
                $paddlePlan = $processorClient->createPlan([
                    "plan_name" => $productPricing->price_name,
                    "plan_type" => $productPricing->billing_period_frequency,
                    "plan_length" => $productPricing->billing_period_frequency_interval,
                    "main_currency_code" => $productPricing->price_currency,
                    "recurring_price_{$productPricing->price_currency}" => $productPricing->price,
                    "initial_price_{$productPricing->price_currency}" => $productPricing->has_trial ? $productPricing->trial_price : $productPricing->price,
                    "plan_trial_days" => $productPricing->has_trial ? $productPricing->trial_duration_days : 0
                ]);
            }
            catch (\Exception $e){
                logError("high", "Error while creating plan in Paddle via API." , __FILE__, __LINE__, ["product" => $product->id, "error" => $e->getMessage()]);
                // create a flag to indicate the setup failed
                $product->product_settings()->create(["key" => "paddle_setup_failed", "value" => 1]);
                // create in app notification
                $this->notifyUser(
                    __("notification.pricingPlanCreationViaApiFailedTitle", ["paymentProvider" => "Paddle", "productName" => $product->name]),
                    __("notification.pricingPlanCreationViaApiFailedContent", ["paymentProvider" => "Paddle", "productName" => $product->name])
                );
                continue;
            }

            // if creating product failed
            if (isset($paddlePlan["product_id"]) == false){
                logError("high", "Error while creating plan in Paddle via API." , __FILE__, __LINE__, ["product" => $product->id, "error" => $paddlePlan]);
                // create a flag to indicate the setup failed
                $product->product_settings()->create(["key" => "paddle_setup_failed", "value" => 1]);
                // create in app notification
                $this->notifyUser(
                    __("notification.pricingPlanCreationViaApiFailedTitle", ["paymentProvider" => "Paddle", "productName" => $product->name]),
                    __("notification.pricingPlanCreationViaApiFailedContent", ["paymentProvider" => "Paddle", "productName" => $product->name])
                );
                continue;
            }

            // store paddle plan id in our side
            $product->product_settings()->create(["product_pricing_id" => $productPricing->id, "key" => "paddle_plan_id", "value" => $paddlePlan["product_id"]]);
        }

        return true;
    }

    /**
     * Function to create product plans in Stripe Connect
     *
     * @param $product
     * @param $productPricings
     * @param $stripeConnectActiveIntegrationData
     *
     * @return bool
     */
    private function createProductsInStripeConnect($product, $productPricings, $stripeConnectActiveIntegrationData)
    {
        $stripeAccountId = $this->getActiveIntegrationDataValuesByKey($stripeConnectActiveIntegrationData, "ACCOUNT_ID");

        if (is_null($stripeAccountId)){
            logError("high", "User Stripe Connect integration does not have account id credentials set for the product.", __FILE__, __LINE__, ["product" => $product->id, "integrationData" => $stripeConnectActiveIntegrationData]);
            // create in app notification
            $this->notifyUser(
                __("notification.productIntegrationDataIsMissingTitle", ["integrationName" => "Stripe Connect", "productName" => $product->name]),
                __("notification.productIntegrationDataIsMissingContent", ["integrationName" => "Stripe Connect", "productName" => $product->name])
            );
            return false;
        }

        $processorClient = new StripeConnect($stripeAccountId);

        try {
            // create a product in stripe
            $stripeProduct = $processorClient->createProduct([
                'name' => $product->name,
                'type' => "service",
                'active' => true,
                'description' => $product->description
            ]);
        }
        catch (\Exception $e){
            logError("high", "Error while creating product in Stripe Connect via API." , __FILE__, __LINE__, ["product" => $product->id, "error" => $e->getMessage()]);
            // create a flag to indicate the setup failed
            $product->product_settings()->create(["key" => "stripe_connect_setup_failed", "value" => 1]);
            // create in app notification
            $this->notifyUser(
                __("notification.productCreationViaApiFailedTitle", ["paymentProvider" => "Stripe Connect"]),
                __("notification.productCreationViaApiFailedContent", ["paymentProvider" => "Stripe Connect", "productName" => $product->name])
            );
            return false;
        }

        // if creating product failed
        if (isset($stripeProduct["id"]) == false){
            logError("high", "Error while creating product in Stripe Connect via API." , __FILE__, __LINE__, ["product" => $product->id, "error" => $stripeProduct]);
            // create a flag to indicate the setup failed
            $product->product_settings()->create(["key" => "stripe_connect_setup_failed", "value" => 1]);
            // create in app notification
            $this->notifyUser(
                __("notification.productCreationViaApiFailedTitle", ["paymentProvider" => "Stripe Connect"]),
                __("notification.productCreationViaApiFailedContent", ["paymentProvider" => "Stripe Connect", "productName" => $product->name])
            );
            return false;
        }

        // store stripe product id in our side
        $product->product_settings()->create(["key" => "stripe_connect_product_id", "value" => $stripeProduct["id"]]);

        foreach ($productPricings as $key => $productPricing){
            // prepare plan data
            $planData = [
                'amount' => (int) $productPricing->price * 100,
                'currency' => $productPricing->price_currency,
                'interval' => $productPricing->billing_period_frequency,
                'trial_period_days' =>  $productPricing->has_trial ? $productPricing->trial_duration_days : 0,
                'product'  => $stripeProduct["id"],
                'metadata' => [
                    'product_id' => $product->id,
                    'product_pricing_id' => $productPricing->id
                ]
            ];

            if ($productPricing->billing_period == ProductPricing::BILLING_PERIOD_QUARTERLY){
                $planData['interval_count'] = 3;
            }
            elseif ($productPricing->billing_period == ProductPricing::BILLING_PERIOD_BI_YEARLY){
                $planData['interval_count'] = 2;
            }

            try {
                // create a plan in stripe
                $stripePlan = $processorClient->createPlan($planData);
            }
            catch (\Exception $e){
                logError("high", "Error while creating plan in Stripe Connect via API." , __FILE__, __LINE__, ["product" => $product->id, "error" => $e->getMessage()]);
                // create a flag to indicate the setup failed
                $product->product_settings()->create(["key" => "stripe_connect_setup_failed", "value" => 1]);
                // create in app notification
                $this->notifyUser(
                    __("notification.pricingPlanCreationViaApiFailedTitle", ["paymentProvider" => "Stripe Connect", "productName" => $product->name]),
                    __("notification.pricingPlanCreationViaApiFailedContent", ["paymentProvider" => "Stripe Connect", "productName" => $product->name])
                );
                continue;
            }

            // if creating plan failed
            if (isset($stripePlan["id"]) == false){
                logError("high", "Error while creating plan in Stripe Connect via API." , __FILE__, __LINE__, ["product" => $product->id, "error" => $stripePlan]);
                // create a flag to indicate the setup failed
                $product->product_settings()->create(["key" => "stripe_connect_setup_failed", "value" => 1]);
                // create in app notification
                $this->notifyUser(
                    __("notification.pricingPlanCreationViaApiFailedTitle", ["paymentProvider" => "Stripe Connect", "productName" => $product->name]),
                    __("notification.pricingPlanCreationViaApiFailedContent", ["paymentProvider" => "Stripe Connect", "productName" => $product->name])
                );
                continue;
            }

            // store stripe plan id in our side
            $product->product_settings()->create(["product_pricing_id" => $productPricing->id, "key" => "stripe_connect_plan_id", "value" => $stripePlan["id"]]);
        }

        return true;
    }

    /**
     * Create in app notification for user
     *
     * @param $subject
     * @param $message
     */
    private function notifyUser($subject, $message)
    {
        $notificationData = [
            "user_id"   => $this->product->user_id,
            "type"      => 3,
            'module_id' => config("config.module_ids.SP"),
            'title'     => $subject,
            'message'   => $message
        ];
        // put in queue
        dispatch(new InAppNotification($notificationData))->onConnection("sync");
    }
}