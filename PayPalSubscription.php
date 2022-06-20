<?php
namespace Core\Payments;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\ClientException;
use Illuminate\Support\Facades\Validator;

/**
 * This class will handle PayPal subscriptions.
 * Ref https://developer.paypal.com/docs/subscriptions &&
 * https://developer.paypal.com/docs/api/subscriptions/v1
 *
 * Class PayPalSubscription
 * @package Core\Payments
 */
class PayPalSubscription
{
    private $apiUrl = "https://api.paypal.com/v1/";
    private $sandboxPpiUrl = "https://api.sandbox.paypal.com/v1/";
    private $clientId;
    private $secretKey;
    private $accessToken;
    private $sandbox;

    /**
     * PayPalSubscription constructor.
     * @param $clientId
     * @param $secretKey
     * @param $sandbox
     */
    public function __construct($clientId, $secretKey, $sandbox)
    {
        $this->clientId = $clientId;
        $this->secretKey = $secretKey;
        $this->sandbox = in_array($sandbox, ["on", 1])  ? true : false;
        $this->accessToken = $this->generateAccessToken();
    }

    /**
     * Function to generate the access token based on client id and secret key
     *
     * @return null|string
     */
    private function generateAccessToken(){

        $accessTokenRequest = $this->request("oauth2/token", "POST", [
            "form_params" => [
                "grant_type" => "client_credentials"
            ]
        ], [
            "auth" => [$this->clientId, $this->secretKey]
        ]);

        return $accessTokenRequest["access_token"] ?? null;
    }

    /**
     * Merchants can use the Catalog Products API to create products, which are goods and services.
     *
     * @param array $attributes
     *
     * @return mixed|string
     * @throws \Exception
     */
    public function createProduct(array $attributes){

        // validate input
        $validator = Validator::make($attributes, [
            "name"          => "required|string",
            "description"   => "nullable|string",
            "type"          => "required|string|in:PHYSICAL,DIGITAL,SERVICE",
            "category"      => "string",
            "image_url"     => "string|url",
            "home_url"      => "string|url"
        ]);

        // if validation fails return errors array
        if ($validator->fails()){
            throw new \Exception($validator->errors());
        }

        $response = $this->request(
            "catalogs/products",
            "POST",
            ["json" => $attributes]
        );

        return $response;
    }

    /**
     * Updates a product, by ID. You can patch these attributes and objects:
     *
     * @param string $productId
     * @param array $attributes
     * @return mixed|string
     * @throws \Exception
     */
    public function updateProduct(string $productId, array $attributes){

        // validate input
        $validator = Validator::make($attributes, [
            "description"   => "string",
            "image_url"     => "string|url",
            "home_url"      => "string|url",
        ]);

        // if validation fails return errors array
        if ($validator->fails()){
            throw new \Exception($validator->errors());
        }

        $body = [];
        foreach ($attributes as $attributeKey => $attributeValue){
            array_push($body, [
                "op"    => "replace",
                "path"  => "/{$attributeKey}",
                "value" => $attributeValue
            ]);
        }

        $response = $this->request(
            "catalogs/products/{$productId}",
            "PATCH",
            ["json" => $body]
        );

        return $response;
    }

    /**
     * Shows details for a product, by ID.
     *
     * @param string $productId
     * @return mixed|string
     */
    public function retrieveProduct(string $productId){

        $response = $this->request(
            "catalogs/products/{$productId}",
            "GET"
        );

        return $response;
    }

    /**
     * Lists products.
     *
     * @return mixed|string
     */
    public function retrieveProducts(){

        $response = $this->request(
            "catalogs/products/",
            "GET"
        );

        return $response["products"] ?? [];
    }

    /**
     * Creates a plan that defines pricing and billing cycle details for subscriptions.
     *
     * @param array $attributes
     * @return mixed|string
     * @throws \Exception
     */
    public function createPlan(array $attributes){

        // validate input
        $validator = Validator::make($attributes, [
            "product_id"                                                => "required|string",
            "name"                                                      => "required|string",
            "status"                                                    => "string|in:CREATED,INACTIVE,ACTIVE",
            "billing_cycles.*.tenure_type"                              => "required|string|in:REGULAR,TRIAL",
            "billing_cycles.*.sequence"                                 => "required|integer",
            "billing_cycles.*.frequency.interval_unit"                  => "required|string|in:DAY,WEEK,MONTH,YEAR",
            "billing_cycles.*.frequency.interval_count"                 => "integer",
            "billing_cycles.*.pricing_scheme.fixed_price.value"         => "required",
            "billing_cycles.*.pricing_scheme.fixed_price.currency_code" => "required|string",
            "payment_preferences.setup_fee.value"                       => "required|integer",
            "payment_preferences.setup_fee.currency_code"               => "required|string",
        ]);

        // if validation fails return errors array
        if ($validator->fails()){
            throw new \Exception($validator->errors());
        }

        $response = $this->request(
            "billing/plans",
            "POST",
            ["json" => $attributes]
        );

        return $response;
    }

    /**
     * Updates a plan with the CREATED or ACTIVE status. For an INACTIVE plan, you can make only status updates.
     *
     * @param string $planId
     * @param array $attributes
     * @return mixed|string
     * @throws \Exception
     */
    public function updatePlan(string $planId, array $attributes){

        // validate input
        $validator = Validator::make($attributes, [
            "description"   => "string",
            "status"        => "string|in:CREATED,INACTIVE,ACTIVE"
        ]);

        // if validation fails return errors array
        if ($validator->fails()){
            throw new \Exception($validator->errors());
        }

        $body = [];
        foreach ($attributes as $attributeKey => $attributeValue){
            array_push($body, [
                "op"    => "replace",
                "path"  => "/{$attributeKey}",
                "value" => $attributeValue
            ]);
        }

        $response = $this->request(
            "billing/plans/{$planId}",
            "PATCH",
            ["json" => $body]
        );

        return $response;
    }

    /**
     * Shows details for a plan, by ID.
     *
     * @param string $planId
     * @return mixed|string
     */
    public function retrievePlan(string $planId){

        $response = $this->request(
            "billing/plans/{$planId}",
            "GET"
        );

        return $response;
    }

    /**
     * Lists billing plans.
     *
     * @return mixed|string
     */
    public function retrievePlans(){

        $response = $this->request(
            "billing/plans",
            "GET"
        );

        return $response["plans"] ?? [];
    }

    /**
     * Activates a plan, by ID.
     *
     * @param string $planId
     * @return array
     */
    public function activatePlan(string $planId){

        $response = $this->request(
            "billing/plans/{$planId}/activate",
            "POST"
        );

        return $response;
    }

    /**
     * Deactivates  a plan, by ID.
     *
     * @param string $planId
     * @return array
     */
    public function deactivatePlan(string $planId){

        $response = $this->request(
            "billing/plans/{$planId}/deactivate",
            "POST"
        );

        return $response;
    }

    /**
     * Updates pricing for a plan. For example, you can update a regular billing cycle from $5 per month to $7 per month.
     *
     * @param string $planId
     * @param array $attributes
     * @return mixed|string
     * @throws \Exception
     */
    public function updatePlanPricing(string $planId, array $attributes){

        // validate input
        $validator = Validator::make($attributes, [
            "pricing_schemes.*.billing_cycle_sequence"                    => "required|integer",
            "pricing_schemes.*.pricing_scheme.fixed_price.value"          => "required",
            "pricing_schemes.*.pricing_scheme.fixed_price.currency_code"  => "required|string"
        ]);

        // if validation fails return errors array
        if ($validator->fails()){
            throw new \Exception($validator->errors());
        }

        $response = $this->request(
            "billing/plans/$planId/update-pricing-schemes",
            "POST",
            ["json" => $attributes]
        );

        return $response;
    }

    /**
     * Creates a subscription.
     *
     * @param array $attributes
     * @return mixed|string
     * @throws \Exception
     */
    public function createSubscription(array $attributes){

        // validate input
        $validator = Validator::make($attributes, [
            "plan_id"                      => "required|string",
            "start_time"                   => "string",
            "subscriber.name"              => "string",
            "subscriber.email_address"     => "string|email"
        ]);

        // if validation fails return errors array
        if ($validator->fails()){
            throw new \Exception($validator->errors());
        }

        $response = $this->request(
            "billing/subscriptions",
            "POST",
            ["json" => $attributes]
        );

        return $response;
    }

    /**
     * Shows details for a subscription, by ID.
     *
     * @param string $subscriptionId
     * @return mixed|string
     */
    public function retrieveSubscription(string $subscriptionId){

        $response = $this->request(
            "billing/subscriptions/{$subscriptionId}",
            "GET"
        );

        return $response;
    }

    /**
     * Activates the subscription.
     *
     * @param string $subscriptionId
     * @return mixed|string
     */
    public function activateSubscription(string $subscriptionId){

        $response = $this->request(
            "billing/subscriptions/{$subscriptionId}/activate",
            "POST",
            [
                "json" => ["reason" => "Activate subscription"]
            ]
        );

        return $response;
    }

    /**
     * Suspends the subscription.
     *
     * @param string $subscriptionId
     * @return mixed|string
     */
    public function suspendSubscription(string $subscriptionId){

        $response = $this->request(
            "billing/subscriptions/{$subscriptionId}/suspend",
            "POST",
            [
                "json" => ["reason" => "Suspend subscription"]
            ]
        );

        return $response;
    }

    /**
     * Lists transactions for a subscription.
     *
     * @param string $subscriptionId
     * @param string $startTime
     * @param string $endTime
     * @return mixed|string
     */
    public function retrieveSubscriptionTransactions(string $subscriptionId, string $startTime, string $endTime){

        $response = $this->request(
            "billing/subscriptions/{$subscriptionId}/transactions",
            "GET",
            [
                "query" => [
                    "start_time" => $startTime,
                    "end_time" => $endTime
                ]
            ]
        );

        return $response;
    }

    /**
     * Cancels the subscription.
     *
     * @param string $subscriptionId
     * @return mixed|string
     */
    public function cancelSubscription(string $subscriptionId){

        $response = $this->request(
            "billing/subscriptions/{$subscriptionId}/cancel",
            "POST",
            [
                "json" => ["reason" => "Cancel subscription"]
            ]
        );

        return $response;
    }

    /**
     * Updates the quantity of the product or service in a subscription.
     * You can also use this method to switch the plan and update the shipping_amount, shipping_address values for the subscription.
     * This type of update requires the buyer's consent.
     * If the buyer originally subscribed using a credit or debit card, the revision does not require the buyer's consent.
     *
     * @param string $subscriptionId
     * @param string $planId
     * @return mixed|string
     */
    public function reviseSubscription(string $subscriptionId, string $planId){

        $response = $this->request(
            "billing/subscriptions/{$subscriptionId}/revise",
            "POST",
            [
                "json" => [
                    "plan_id" => $planId,
                    "application_context" => [
                        "return_url" => config("config.app._fe_main_url"),
                        "cancel_url" => config("config.app._fe_main_url")
                    ]
                ]
            ]
        );

        return $response;
    }

    /**
     * Function to make GET/POST requests
     *
     * @param $url
     * @param string $method
     * @param array $body
     * @param array $options
     * @return mixed|string
     */
    private function request($url, $method = "POST", array $body = [], array $options = [])
    {
        $client = new Client([
            "headers" => [
                "Authorization" => "Bearer {$this->accessToken}"
            ]
        ]);

        // pick api base url based on env (sandbox or live)
        if ($this->sandbox){
            $apiUrl = $this->sandboxPpiUrl;
        }
        else{
            $apiUrl = $this->apiUrl;
        }

        try {
            switch ($method){
                case "GET":
                    $response = $client->get($apiUrl . $url, array_merge($body, $options));
                    break;
                case "POST":
                    $response = $client->post($apiUrl . $url, array_merge($body, $options));
                    break;
                case "PATCH":
                    $response = $client->patch($apiUrl . $url, array_merge($body, $options));
                    break;
            }
        } catch (ClientException $e){
            return json_decode($e->getResponse()->getBody()->getContents(), true);
        }
        return json_decode($response->getBody()->getContents(), true);
    }
}
