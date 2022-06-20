<?php
namespace Core\Traits;

use Core\Jobs\InAppNotification;
use Core\Payments\Paddle;
use Core\Payments\Stripe;
use Core\Payments\StripeConnect;
use Core\SmartFunnels\Models\PageType;
use Core\SmartProducts\Models\ProductPricingType;

trait CheckoutTrait
{
    use ActiveIntegrationsTrait, GoogleRecaptchaTrait;

    /**
     * Generates script for Free product checkout
     *
     * @param $product
     * @param $productPricing
     *
     * @return string
     */
    public function getFreeProductCheckoutScript($product, $productPricing)
    {
        $freeProductScript = view("payments.free_checkout_payment", [
            "productId"         => $product->id,
            "productPricingId"  => $productPricing->id,
            "thankYouPage"      => $productPricing->thank_you_link,
            "googleRecaptchaV3" => $this->generateGoogleRecaptchaV3JsScript($product->user_id, "-free-button", "window.handleFreeProductPurchase(event);")
        ])->render();

        return $freeProductScript;
    }

    /**
     * Generates script for Stripe checkout
     *
     * @param $product
     * @param $productPricing
     * @param $productPaymentMethodActiveIntegrations
     *
     * @return string
     */
    public function getStripeCheckoutScript($product, $productPricing, $productPaymentMethodActiveIntegrations)
    {
        // get the product's Stripe active integration data
        $activeIntegrationData = $this->getActiveIntegrationDataByIntegrationName($productPaymentMethodActiveIntegrations, "stripe");
        // if product does not have any Stripe active integration
        if (is_null($activeIntegrationData)) return null;

        // if pricing stock is expired
        if ($productPricing->stock_left == 0){

            $supportLink = $product->support_link;
            // if product support link is not set
            if (is_null($supportLink)){
                // get product owner user's data
                $productOwnerRequest = $this->makeGetRequest(config("config.app.aa_api_url") . "aa/users/{$product->user_id}");

                // if request to AA was failed
                if (isset($productOwnerRequest["data"]) == false){
                    $supportLink =  $productOwnerRequest["data"]["email"];
                }
            }

            $stripeOutOfStock = view("templates.checkout_payment_out_of_stock", [
                "supportLink" => $supportLink,
                "provider" => "stripe"
            ])->render();

            return $stripeOutOfStock;
        }

        // get publishable key for stripe
        $publishableKey =  $this->getActiveIntegrationDataValuesByKey($activeIntegrationData, "PUBLISHABLE_KEY");
        $cvcValidation =  $this->getActiveIntegrationDataValuesByKey($activeIntegrationData, "CVC_VALIDATION");

        if (is_null($publishableKey)) return null;

        $stripeScript = view("payments.stripe_checkout", [
            "productType"       => $productPricing->product_pricing_type->type_name,
            "productId"         => $product->id,
            "productPricingId"  => $productPricing->id,
            "publishableKey"    => $publishableKey,
            "cvcValidation"     => $cvcValidation,
            "thankYouPage"      => $productPricing->thank_you_link,
            "googleRecaptchaV3" => $this->generateGoogleRecaptchaV3JsScript($product->user_id,"-stripe-button", "window.handleStripeProductPurchase(event)")
        ])->render();

        $checkoutContent =  view("payments.checkout_payment", [
            "stripeScript" => $stripeScript,
            "provider"     => "stripe"
        ])->render();

        return $checkoutContent;
    }

    /**
     * Generates script for Stripe Connect checkout
     *
     * @param $product
     * @param $productPricing
     * @param $productPaymentMethodActiveIntegrations
     *
     * @return array|string|null
     */
    public function getStripeConnectCheckoutScript($product, $productPricing, $productPaymentMethodActiveIntegrations)
    {
        // get the product's active payment integration data based on integration name
        $activeIntegrationData = $this->getActiveIntegrationDataByIntegrationName($productPaymentMethodActiveIntegrations, "stripe connect");

        if (is_null($activeIntegrationData)) return null;

        // if pricing stock is expired
        if ($productPricing->stock_left == 0){

            $supportLink = $product->support_link;
            // if product support link is not set
            if (is_null($supportLink)){
                // get product owner user's data
                $productOwnerRequest = $this->makeGetRequest(config("config.app.aa_api_url") . "aa/users/{$product->user_id}");

                // if request to AA was failed
                if (isset($productOwnerRequest["data"])){
                    $supportLink =  $productOwnerRequest["data"]["email"];
                }
            }

            $stripeConnectOutOfStock = view("payments.checkout_payment_out_of_stock", [
                "supportLink" => $supportLink,
                "provider" => "stripe_connect"
            ])->render();

            return $stripeConnectOutOfStock;
        }

        // get the account id and cvc key for stripe connect
        $accountId = $this->getActiveIntegrationDataValuesByKey($activeIntegrationData, "ACCOUNT_ID");
        $cvcValidation = $this->getActiveIntegrationDataValuesByKey($activeIntegrationData, "CVC_VALIDATION");

        $secretKey = config("config.app.pp_secrets._stripe_connect_secret");
        $publishableKey = config("config.app.pp_secrets._stripe_connect_publishable");

        // if account id or publishable key were not found
        if (is_null($accountId) || is_null($secretKey) || is_null($publishableKey)) return null;

        // if product is onetime, free or cross sell
        if ($productPricing->product_pricing_type->type == ProductPricingType::PRICING_TYPE_SUBSCRIPTION){
            $stripeConnectScript = view("payments.stripe_connect_checkout", [
                "productType"      => $productPricing->product_pricing_type->type_name,
                "productId"        => $product->id,
                "productPricingId" => $productPricing->id,
                "accountId"        => $accountId,
                "publishableKey"   => $publishableKey,
                "cvcValidation"    => $cvcValidation,
                "thankYouPage"     => $productPricing->thank_you_link
            ])->render();
        }
        else{
            // init Stripe Connect client
            $processorClient = new StripeConnect($accountId);
            // create payment intent
            $paymentIntent = $processorClient->createPaymentIntent([
                "amount" => (float) $productPricing->price,
                "currency" => $productPricing->price_currency,
                "description" => $product->name,
                "payment_method_types" => ["card"],
                "metadata" => [
                    "product_id" => $product->id,
                    "product_pricing_id" => $productPricing->id,
                    "kti" => $_COOKIE[""] ?? null
                ]
            ]);

            // if there was error creating payment intent via API
            if (isset($paymentIntent["client_secret"]) == false){
                logDebug("high", "There was an error while creating Stripe connect payment intent.", __FILE__, __LINE__, ["error" => $paymentIntent]);
                return null;
            }

            $stripeConnectScript = view("payments.stripe_connect_checkout", [
                "productType"      => $productPricing->product_pricing_type->type_name,
                "productId"        => $product->id,
                "accountId"        => $accountId,
                "publishableKey"   => $publishableKey,
                "cvcValidation"    => $cvcValidation,
                "paymentIntent"    => $paymentIntent,
                "thankYouPage"     => $productPricing->thank_you_link
            ])->render();
        }

        $checkoutContent =  view("payments.checkout_payment", [
            "stripeConnectScript" => $stripeConnectScript,
            "provider"            => "stripe_connect"
        ])->render();

        return $checkoutContent;
    }

    /**
     * Generate script for Stripe Bank Payments checkout
     *
     * @param $product
     * @param $productPricing
     * @param $productPaymentMethodActiveIntegrations
     *
     * @return array|string|null
     */
    public function getStripeBankPaymentsScript($product, $productPricing, $productPaymentMethodActiveIntegrations)
    {
        // get the product"s active payment integration data based on integration name
        $activeIntegrationData = $this->getActiveIntegrationDataByIntegrationName($productPaymentMethodActiveIntegrations, "stripe");

        if (is_null($activeIntegrationData)) return null;

        // if pricing stock is expired
        if ($productPricing->stock_left == 0){

            $supportLink = $product->support_link;
            // if product support link is not set
            if (is_null($supportLink)){
                // get product owner user"s data
                $productOwnerRequest = $this->makeGetRequest(config("config.app.aa_api_url") . "aa/users/{$product->user_id}");

                // if request to AA was failed
                if (isset($productOwnerRequest["data"])){
                    $supportLink =  $productOwnerRequest["data"]["email"];
                }
            }

            $stripeBankOutOfStock = view("payments.checkout_payment_out_of_stock", [
                "supportLink" => $supportLink,
                "provider" => "stripe_bank"
            ])->render();

            return $stripeBankOutOfStock;
        }

        // get the secret key and publishable key for stripe
        $secretKey = $this->getActiveIntegrationDataValuesByKey($activeIntegrationData, "SECRET_KEY");
        $publishableKey =  $this->getActiveIntegrationDataValuesByKey($activeIntegrationData, "PUBLISHABLE_KEY");

        if (is_null($secretKey) || is_null($publishableKey)) return null;

        // if pricing currency is not EUR
        if ($productPricing->price_currency != "EUR") return null;

        // if product is subscription
        if ($productPricing->product_pricing_type->type == ProductPricingType::PRICING_TYPE_SUBSCRIPTION) return null;

        // init Stripe client
        $processorClient = new Stripe($secretKey);

        // create checkout session intent
        $checkoutSession = $processorClient->createCheckoutSession([
            "success_url" => $productPricing->thank_you_link ?? config("config.app._fe_main_url"),
            "cancel_url" => $productPricing->purchase_link ?? config("config.app._fe_main_url"),
            "mode" => "payment",
            "payment_method_types" => ["bancontact", "eps", "giropay", "ideal", "p24", "sofort"],
            "payment_intent_data" => [
                "metadata" => [
                    "product_id" => $product->id,
                    "product_pricing_id" => $productPricing->id,
                    "kti" => $_COOKIE["ID"] ?? null
                ]
            ],
            "line_items" => [
                [
                    "amount" => (int) $productPricing->price * 100,
                    "currency" => $productPricing->price_currency,
                    "name" => $product->name,
                    "quantity" => 1
                ]
            ],
            "discounts" => [
                [
                    "coupon" => $productPricing->applied_coupon
                ]
            ],
            "metadata" => [
                "product_id" => $product->id,
                "product_pricing_id" => $productPricing->id,
                "kti" => $_COOKIE["ID"] ?? null
            ]
        ]);

        // if there was error creating checkout session via API
        if (isset($checkoutSession["id"]) == false){
            logDebug("high", "There was an error while creating Stripe checkout session.", __FILE__, __LINE__, ["error" => $checkoutSession]);
            return null;
        }

        $stripeBankPaymentsScript = view("payments.stripe_bank_payments_checkout", [
            "productType"      => $productPricing->product_pricing_type->type_name,
            "sessionId"        => $checkoutSession["id"],
            "publishableKey"   => $publishableKey
        ])->render();

        $checkoutContent =  view("payments.checkout_payment", [
            "stripeBankPaymentsScript"  => $stripeBankPaymentsScript,
            "provider"                  => "stripe_bank"
        ])->render();

        return $checkoutContent;
    }

    /**
     * Generates script for paypal checkout
     *
     * @param $product
     * @param $productPricing
     * @param $userIntegrations
     * @return string
     */
    public function getPaypalCheckoutScript($product, $productPricing, $userIntegrations)
    {
        // get the product's active payment integration data based on integration name
        $activeIntegrationData = $this->getActiveIntegrationDataByIntegrationName($userIntegrations, "paypal");

        if (is_null($activeIntegrationData)) return null;

        // if pricing stock is expired
        if ($productPricing->stock_left == 0){

            $supportLink = $product->support_link;
            // if product support link is not set
            if (is_null($supportLink)){
                // get product owner user"s data
                $productOwnerRequest = $this->makeGetRequest(config("config.app.aa_api_url") . "aa/users/{$product->user_id}");

                // if request to AA was failed
                if (isset($productOwnerRequest["data"])){
                    $supportLink =  $productOwnerRequest["data"]["email"];
                }
            }

            $paypalBankOutOfStock = view("payments.checkout_payment_out_of_stock", [
                "supportLink" => $supportLink,
                "provider" => "paypal"
            ])->render();

            return $paypalBankOutOfStock;
        }

        // get the client key for paypal
        $clientKey = $this->getActiveIntegrationDataValuesByKey($activeIntegrationData, "CLIENT_ID");
        $sandboxMode = $this->getActiveIntegrationDataValuesByKey($activeIntegrationData, "SANDBOX_MODE");

        if (is_null($clientKey)) return null;

        // specify the environment
        if (is_null($sandboxMode) || $sandboxMode == "off"){
            $env = "production";
        }
        else{
            $env = "sandbox";
        }

        // if product is subscription
        if ($productPricing->product_pricing_type->type == ProductPricingType::PRICING_TYPE_SUBSCRIPTION){
            if (is_null($productPricing->applied_coupon_id)){
                // get the paypal plan id
                $paypalPlanId = $product->product_settings()
                    ->where("product_pricing_id", $productPricing->id)
                    ->where("key", "paypal_plan_id")
                    ->value("value");
            }
            else {
                // get the paypal plan id for coupon
                $paypalPlanId = $product->product_settings()
                    ->where("product_pricing_id", $productPricing->id)
                    ->where("key", "paypal_coupon_{$productPricing->applied_coupon_id}_plan_id")
                    ->value("value");
            }

            // if paypal plan id was not found
            if (is_null($paypalPlanId)) return null;

            $paypalScript = view("payments.paypal_checkout", [
                "productType"      => $productPricing->product_pricing_type->type_name,
                "product"          => $product,
                "paypalPlanId"     => $paypalPlanId,
                "productPricing"   => $productPricing,
                "appliedCoupon"    => $productPricing->applied_coupon,
                "clientKey"        => $clientKey,
                "thankYouPage"     => $productPricing->thank_you_link,
                "environment"      => $env
            ])->render();
        }
        else{
            $paypalScript = view("payments.paypal_checkout", [
                "productType"      => $productPricing->product_pricing_type->type_name,
                "product"          => $product,
                "productPricing"   => $productPricing,
                "appliedCoupon"    => $productPricing->applied_coupon,
                "clientKey"        => $clientKey,
                "thankYouPage"     => $productPricing->thank_you_link,
                "environment"      => $env
            ])->render();
        }

        $checkoutContent =  view("payments.checkout_payment", [
            "paypalScript" => $paypalScript,
            "provider"     => "paypal"
        ])->render();

        return $checkoutContent;
    }

    /**
     * Generates script for paddle checkout
     *
     * @param $product
     * @param $productPricing
     * @param $userIntegrations
     * @return string
     */
    public function getPaddleCheckoutScript($product, $productPricing, $userIntegrations)
    {
        // get the product's active payment integration data based on integration name
        $activeIntegrationData = $this->getActiveIntegrationDataByIntegrationName($userIntegrations, "paddle");

        if (is_null($activeIntegrationData)) return null;

        // if pricing stock is expired
        if ($productPricing->stock_left == 0){

            $supportLink = $product->support_link;
            // if product support link is not set
            if (is_null($supportLink)){
                // get product owner user's data
                $productOwnerRequest = $this->makeGetRequest(config("config.app.aa_api_url") . "aa/users/{$product->user_id}");

                // if request to AA was failed
                if (isset($productOwnerRequest["data"])){
                    $supportLink =  $productOwnerRequest["data"]["email"];
                }
            }

            $paddleBankOutOfStock = view("payments.checkout_payment_out_of_stock", [
                "supportLink" => $supportLink,
                "provider" => "paddle"
            ])->render();

            return $paddleBankOutOfStock;
        }

        $paddleVendorId = $this->getActiveIntegrationDataValuesByKey($activeIntegrationData, "VENDOR_ID");
        $paddleVendorAuth = $this->getActiveIntegrationDataValuesByKey($activeIntegrationData, "VENDOR_CODE");

        if (is_null($paddleVendorId) || is_null($paddleVendorAuth)) return null;

        $processorClient = new Paddle($paddleVendorId, $paddleVendorAuth);

        // if product is subscription
        if ($productPricing->product_pricing_type->type == ProductPricingType::PRICING_TYPE_SUBSCRIPTION){

            // get the paddle plan id for the purchasing product
            $paddlePlanId = $product->product_settings()
                                ->where("product_pricing_id", $productPricing->id)
                                ->where("key", "paddle_plan_id")
                                ->value("value");

            // if paddle plan id was not found throw an error
            if (is_null($paddlePlanId)) return null;

            $paddleScript = view("payments.paddle_checkout", [
                "productType"      => $productPricing->product_pricing_type->type_name,
                "paddleVendorId"   => $paddleVendorId,
                "paddlePlanId"     => $paddlePlanId,
                "appliedCoupon"    => $productPricing->applied_coupon,
                "thankYouPage"     => $productPricing->thank_you_link
            ])->render();
        }
        else {
            // prepare data to create a pay link
            $payLinkData = [
                "discountable" => false,
                "title" => $productPricing->price_name,
                "prices" => ["{$productPricing->price_currency}:{$productPricing->price}"],
                "passthrough" => json_encode(["product_id" => $product->id, "product_pricing_id" => $productPricing->id, "kti" => $_COOKIE["ID"] ?? "", "coupon" => $productPricing->applied_coupon]),
                "webhook_url" => config("config.app.url") . "ipn/paddle",
                "return_url" => $productPricing->thank_you_link
            ];

            // generate pay link
            $payLinkRequest = $processorClient->generatePayLink($payLinkData);

            // if there was error creating pay link via API
            if (isset($payLinkRequest["url"]) == false){
                logDebug("high", "There was an error while creating Paddle pay link.", __FILE__, __LINE__, ["error" => $payLinkRequest]);
                return null;
            }

            $paddleScript = view("payments.paddle_checkout", [
                "productType"      => $productPricing->product_pricing_type->type_name,
                "paddleVendorId"   => $paddleVendorId,
                "payLink"          => $payLinkRequest["url"],
                "thankYouPage"     => $productPricing->thank_you_link
            ])->render();
        }

        $checkoutContent =  view("payments.checkout_payment", [
            "paddleScript" => $paddleScript,
            "provider"     => "paddle"
        ])->render();

        return $checkoutContent;
    }

    /**
     * Function to generate checkout page link for product
     *
     * @param $funnel
     * @param $product
     * @param $productPricing
     *
     * @return mixed|string|null
     */
    public function generateCheckoutLinkForProduct($funnel, $product, $productPricing)
    {
        // get site url
        $siteUrl = $this->getSiteUrlById($funnel->site_id, $funnel->user_id);
        // if something went wrong while getting site from AA
        if (is_null($siteUrl)){
            return null;
        }

        // if product is  checkout or free product
        if ($product->_checkout == 1 || $productPricing->product_pricing_type->type == ProductPricingType::PRICING_TYPE_FREE){

            // get funnel's checkout page
            $checkoutPage = $funnel->pages()->whereHas("page_type", function ($q){
                $q->where("page_type", PageType::getPageTypeKey("checkout-page"));
            })->first();

            if (is_null($checkoutPage)){
                logError("critical", "Error while generating checkout link.Funnel does not have checkout page.",__FILE__, __LINE__, ["funnel" => $funnel->id]);

                // create an in app notification for funnel owner
                dispatch(new InAppNotification([
                    "user_id"   => $funnel->user_id,
                    "type"      => 3,
                    "module_id" => config("config.module_ids.SP"),
                    "title"     => __("notification.funnelCheckoutPageIsMissingTitle"),
                    "message"   => __("notification.funnelCheckoutPageIsMissingContent", ["funnelName" => $funnel->funnel_name, "productName" => $product->name])
                ]))->onConnection("sync");

                return null;
            }

            // generate checkout link
            $checkoutPage = "https://{$siteUrl}/{$funnel->slug}/{$checkoutPage->slug}?pid={$product->id}&pvid={$productPricing->id}";

            return $checkoutPage;
        }
        else {

            // get payment providers settings for product
            $productPaymentSettings = $productPricing->product_settings()
                                                    ->where("value", "<>", "")
                                                    ->whereIn("key", [
                                                        "jvzoo_product_id",
                                                        "tc_checkout_page_url",
                                                        "zaxaa_checkout_url",
                                                        "wspro_offer_button_code",
                                                        "clickbank_product_id",
                                                        "clickbank_username",
                                                        "digistore_checkout_url",
                                                        "paykickstart_checkout_url"
                                                    ])
                                                    ->get()
                                                    ->keyBy("key");

            if (count($productPaymentSettings) == 0){
                // create an in app notification for product owner
                dispatch(new InAppNotification([
                    "user_id"   => $product->user_id,
                    "type"      => 3,
                    "module_id" => config("config.module_ids.SP"),
                    "title"     => __("notification.productPaymentSettingsMissingTitle"),
                    "message"   => __("notification.productPaymentSettingsMissingContent", ["productName" => $product->name])
                ]))->onConnection("sync");

                return null;
            }

            if (isset($productPaymentSettings["jvzoo_product_id"])){
                $jvzooProductId = $productPaymentSettings["jvzoo_product_id"]->value;

                $checkoutUrl = "https://www.jvzoo.com/b/0/{$jvzooProductId}/99";

                return $checkoutUrl;
            }
            elseif (isset($productPaymentSettings["clickbank_product_id"]) && isset($productPaymentSettings["clickbank_username"])){
                $cbProductId = $productPaymentSettings["clickbank_product_id"]->value;
                $cbAccountName = $productPaymentSettings["clickbank_username"]->value;

                $checkoutUrl = "https://{$cbAccountName}.pay.clickbank.net/?cbitems={$cbProductId}&v1={$product->id}";

                return $checkoutUrl;
            }
            elseif (isset($productPaymentSettings["wspro_offer_button_code"])){
                $wsoproButtonCode = $productPaymentSettings["wspro_offer_button_code"]->value;

                preg_match("'\<a.*?href=\"(.*?)\".*?\>(.*?)\<\/a\>'si", $wsoproButtonCode, $match);
                $checkoutUrl = $match[1] ?? null;

                if (is_null($checkoutUrl) == false){
                    return $checkoutUrl;
                }
            }
            elseif (isset($productPaymentSettings["tc_checkout_page_url"])){
                $checkoutUrl = $productPaymentSettings["tc_checkout_page_url"]->value;

                return $checkoutUrl;
            }
            elseif (isset($productPaymentSettings["zaxaa_checkout_url"])){
                $checkoutUrl = $productPaymentSettings["zaxaa_checkout_url"]->value;

                return $checkoutUrl;
            }
            elseif (isset($productPaymentSettings["digistore_checkout_url"])){
                $checkoutUrl = $productPaymentSettings["digistore_checkout_url"]->value;

                return $checkoutUrl;
            }
            elseif (isset($productPaymentSettings["paykickstart_checkout_url"])){
                $checkoutUrl = $productPaymentSettings["paykickstart_checkout_url"]->value;

                return $checkoutUrl;
            }

            return null;
        }
    }
}
