<?php

namespace Core\AutoResponders;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\ClientException;
use Illuminate\Support\Facades\Validator;

/**
 * Class to communicate with ActiveCampaign API
 *
 * @package Core\AutoResponders
 * @author Davit Okhikyan <davit@kvsocial.com>
 */
class ActiveCampaign
{
    private $apiUrl;
    private $apiKey;
    private $apiRequestUrl;

    /**
     * ActiveCampaign constructor
     *
     * @param string $apiUrl Api url for making requests
     * @param string $apiKey Api key to use for Authorization
     */
    public function __construct($apiUrl, $apiKey)
    {
        $this->apiUrl = $apiUrl;
        $this->apiKey = $apiKey;
        $this->apiRequestUrl = $apiUrl . "/api/3/";
    }

    /**
     * Subscribe email to a list
     *
     * @param array $attributes Array with subscriber data
     * @param array $customFields Array with subscriber custom data
     *
     * @return array
     */
    public function subscribe(array $attributes, array $customFields = [])
    {
        // validate input
        $validator = Validator::make($attributes, [
            "email"        => "required|email",
            "list"         => "required",
            "first_name"   => "string",
            "last_name"    => "string",
            "phone"        => "string"
        ]);

        // if validation fails
        if ($validator->fails()){
            return ["success" => false, "errors" => $validator->errors()->messages()];
        }

        $body = [
            "contact" => [
                "email" => $attributes["email"]
            ]
        ];

        if (isset($attributes["first_name"])){
            $body["contact"]["firstName"] = $attributes["first_name"];
        }

        if (isset($attributes["last_name"])){
            $body["contact"]["lastName"] = $attributes["last_name"];
        }

        if (isset($attributes["phone"])){
            $body["contact"]["phone"] = $attributes["phone"];
        }

        if (count($customFields)) {
            // get available custom fields
            $existingCustomFieldsRequest = $this->getCustomFields($attributes["list"]);

            // if something was wrong during getting the custom fields for the list
            if ($existingCustomFieldsRequest["success"] == false){
                return ["success" => false, "errors" => $existingCustomFieldsRequest["errors"]];
            }

            $existingCustomFields = array_map(function ($customField){
                return $customField["name"];
            }, $existingCustomFieldsRequest["response"]);

            foreach ($customFields as $field => $value){
                if (in_array($field, $existingCustomFields) == false || is_null($value)) continue;

                $body["contact"]["fieldValues"][] = [
                    "field"     => $field,
                    "value"     => $value
                ];
            }
        }

        // create a contact
        $createContactRequest = $this->request($this->apiRequestUrl . "contacts", "POST", $body);

        // if something was wrong during creating a contact
        if ($createContactRequest["success"] == false || isset($createContactRequest["response"]["contact"]) == false){
            return ["success" => false, "errors" => $createContactRequest["errors"]];
        }

        // get contact data from response
        $contact = $createContactRequest["response"]["contact"];

        // add subscriber/contact to list
        if (isset($attributes["list"]) == true){

            $body = [
                "contactList" => [
                    "list"      => $attributes["list"],
                    "contact"   => $contact["id"],
                    "status"    => 1
                ]
            ];

            // add contact to list
            $addContactToListRequest = $this->request($this->apiRequestUrl . "contactLists", "POST", $body);

            // if something went wrong while adding the contact to list
            if ($addContactToListRequest["success"] == false){
                return ["success" => false, "errors" => $addContactToListRequest["errors"]];
            }
        }

        return ["success" => true, "response" => $contact];
    }

    /**
     * Get AR lists
     *
     * @return array Array with lists
     */
    public function getLists()
    {
        $getListsRequest = $this->request($this->apiRequestUrl . "lists" , "GET");

        if ($getListsRequest["success"] == false){
            return ["success" => false, "errors" => $getListsRequest["errors"]];
        }

        $lists = [];
        foreach ($getListsRequest["response"]["lists"] as $list) {
            $lists[$list["id"]] = $list["name"];
        }

        return ["success" => true, "response" => $lists];
    }

    /**
    * Get AR tags
    *
    * @return array Array with tags
    */
    public function getTags(){
        $getTagsRequest = $this->request($this->apiRequestUrl . "tags" , "GET");

        if ($getTagsRequest["success"] == false){
            return ["success" => false, "errors" => $getTagsRequest["errors"]];
        }

        $tags = [];
        foreach ($getTagsRequest["response"]["tags"] as $tag) {
            $tags[$tag["id"]] = $tag["tag"];
        }

        return ["success" => true, "response" => $tags];
    }

    /**
     * Get all custom fields
     *
     * @param $listId
     *
     * @return array Array with custom fields
     */
    public function getCustomFields($listId = null){

        $getCustomFieldsRequest = $this->request($this->apiRequestUrl . "fields" , "GET");

        $defaultFields = [
            ["name" => "email", "type" => "text", "obligatory" => true, "values" => [],"desc" => "Email"],
            ["name" => "first_name", "type" => "text", "obligatory" => false, "values" => [],"desc" => "First Name"],
            ["name" => "last_name", "type" => "text", "obligatory" => false, "values" => [],"desc" => "Last Name"],
            ["name" => "phone", "type" => "text", "obligatory" => false, "values" => [],"desc" => "Phone"]
        ];

        if ($getCustomFieldsRequest["success"] == false){
            return ["success" => true, "response" => $defaultFields];
        }

        $customFieldNames = [];
        foreach ($getCustomFieldsRequest["response"]["fields"] as $field){
            $customFieldNames[] = [
                "name" =>$field["id"],
                "desc" => $field["title"],
                "values" => [],
                "type" => $field["type"] ?? "text",
                "obligatory" => false
            ];
        }

        return ["success" => true, "response" => array_merge($defaultFields, $customFieldNames)];
    }

    /**
     * Function to make requests to ActiveCampaign API
     *
     * @param string $url    The url to use for request
     * @param string $method The method to use for request ( GET/POST )
     * @param array $data    The body to use for request
     *
     * @return array Response of the request
     */
    private function request($url, $method = "POST", array $data = [])
    {
        // initialize the Http client
        $client = new Client(["headers" => ["Api-Token" => $this->apiKey]]);

        try {
            switch ($method){
                case "GET":
                    $response = $client->get($url , ["query" => $data]);
                    break;
                case "POST":
                    $response = $client->post($url , ["body" => json_encode($data)]);
                    break;
            }
        } catch (ClientException $e){
            $errors = [];

            if ($e->getResponse()->getStatusCode() == 422){
                $responseErrors = json_decode($e->getResponse()->getBody()->getContents(), true);
                if (isset($responseErrors["errors"])){
                    foreach ($responseErrors["errors"] as $errorData){
                        $errors[] = $errorData["title"];
                    }
                }
                else{
                    $errors[] = $e->getResponse()->getBody()->getContents();
                }
            }
            else{
                $errors[] = $e->getResponse()->getBody()->getContents();
            }

            return ["success" => false, "errors" => $errors];
        }

        $decodedResponse = json_decode($response->getBody()->getContents(), true);

        return ["success" => true, "response" => $decodedResponse];
    }
}
