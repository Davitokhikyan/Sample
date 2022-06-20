<?php
namespace Core\AutoResponders;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\ClientException;
use Illuminate\Support\Facades\Validator;

/**
 * Class to communicate with SendInBlue API
 *
 * @package Core\AutoResponders
 * @author Davit Okhikyan <davit@kvsocial.com>
 */
class SendInBlue
{
    private $authKey;
    private $apiRequestUrl = "https://api.sendinblue.com/v3/";

    /**
     * SendInBlue constructor
     * 
     * @param $authKey
     */
    public function __construct($authKey)
    {
        $this->authKey = $authKey;
    }

    /**
     * Send a transaction email with SIB
     *
     * @param array $attributes
     *
     * @throws \Exception
     *
     * @return array|bool
     */
    public function sendTransactionalEmail(array $attributes)
    {
        // validate input
        $validator = Validator::make($attributes, [
            "sender.email"        => "required|email",
            "sender.name"         => "string",
            "to"                  => "required|email",
            "htmlContent"         => "required|string",
            "textContent"         => "required|string",
            "subject"             => "required|string",

        ]);

        // if validation fails return errors array
        if ($validator->fails()){
            throw new \Exception($validator->errors());
        }

        $attributes["replyTo"] = ["email" => $attributes["sender"]["email"]];

        $response = $this->request($this->apiRequestUrl . "smtp/email", "POST", $attributes);

        return $response;
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
            "email"       => "required|email",
            "list"        => "required"
        ]);

        // if validation fails return errors array
        if ($validator->fails()){
            return ["success" => false, "errors" => $validator->errors()->messages()];
        }

        $accountListsRequest = $this->getLists();

        if ($accountListsRequest["success"] == false){
            return ["success" => false, "errors" => $accountListsRequest];
        }

        $accountLists = $accountListsRequest["response"];

        // check list id to exist in SIB
        if (array_key_exists($attributes["list"], $accountLists) == false){
            return ["success" => false, "errors" => ["Account does not contain given list"]];
        }

        // check if contact exists
        $checkIfContactExistsRequest = $this->getContact($attributes["email"]);

        if ($checkIfContactExistsRequest["success"] == true){
            return ["success" => true, "response" => $checkIfContactExistsRequest["response"]];
        }

        // create a contact
        $body = [
            "email" => $attributes["email"],
            "listIds" => [(int) $attributes["list"]]
        ];

        // if custom fields are passed
        if (count($customFields)){
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

                $body["attributes"][$field] = $value;
            }
        }

        if (isset($attributes["first_name"])){
            $body["attributes"]["FIRSTNAME"] = $attributes["first_name"];
        }

        if (isset($attributes["last_name"])){
            $body["attributes"]["LASTNAME"] = $attributes["last_name"];
        }

        // create a contact
        $createContactRequest = $this->request($this->apiRequestUrl . "contacts" , "POST" , $body);

        // if something was wrong during creating a contact
        if ($createContactRequest["success"] == false){
            return ["success" => false, "errors" => $createContactRequest["errors"]];
        }

        return ["success" => true, "response" => $createContactRequest["response"]];
    }

    /**
     * Create a tag
     *
     * @param $tag
     * @return array|bool
     */
    public function createTag($tag)
    {
        $getAllTagsRequest = $this->getTags();

        if ($getAllTagsRequest["success"] == false){
            return ["success" => false, "errors" => $getAllTagsRequest["errors"]];
        }

        $tags = $getAllTagsRequest["response"];

        // if tag exists
        if (in_array($tag, $tags)){
            return ["success" => true, "response" => $tag];
        }

        $body = ["type" => "text"];

        $response = $this->request($this->apiRequestUrl . "contacts/attributes/normal/{$tag}" , "POST", $body);

        return $response;
    }

    /**
     * Remove a tag
     *
     * @param $tag
     * @return array|bool
     */
    public function removeTag($tag)
    {
        $getAllTagsRequest = $this->getTags();

        if ($getAllTagsRequest["success"] == false){
            return ["success" => false, "errors" => $getAllTagsRequest["errors"]];
        }

        $tags = $getAllTagsRequest["response"];

        // if tag does not exist
        if (in_array($tag , $tags) == false){
            return ["success" => true, "response" => null];
        }

        $response = $this->request($this->apiRequestUrl . "contacts/attributes/normal/{$tag}" , "DELETE");

        return $response;
    }

    /**
     * Get all tags (attributes)
     *
     * @return array
     */
    public function getTags()
    {
        $getTagsRequest = $this->request($this->apiRequestUrl . "contacts/attributes");

        if ($getTagsRequest["success"] == false){
            return ["success" => false, "errors" => $getTagsRequest["errors"]];
        }

        $tags = [];
        foreach ($getTagsRequest["response"]["attributes"] as $entry) {
            $tags[] = strtolower($entry["name"]);
        }

        return ["success" => true, "response" => $tags];
    }

    /**
     * Get AR lists
     *
     * @return array Array with lists
     */
    public function getLists()
    {
        $getListsRequest = $this->request($this->apiRequestUrl . "contacts/lists");

        if ($getListsRequest["success"] == false){
            return ["success" => false, "errors" => $getListsRequest["errors"]];
        }

        $lists = [];
        foreach ($getListsRequest["response"]["lists"] as $entry) {
            $lists[$entry["id"]] = strtolower($entry["name"]);
        }

        return ["success" => true, "response" => $lists];
    }

    /**
     * Get list"s contacts by list id
     *
     * @param $list
     * @return array
     */
    public function getListContacts($list)
    {
        $getListContactsRequest = $this->request($this->apiRequestUrl . "contacts/lists/{$list}/contacts");

        if ($getListContactsRequest["success"] == false){
            return ["success" => false, "errors" => $getListContactsRequest["errors"]];
        }

        $contacts = [];
        foreach ($getListContactsRequest["response"]["contacts"] as $entry) {
            $contacts[$entry["id"]] = $entry["email"];
        }

        return ["success" => true, "response" => $contacts];
    }

    /**
     * Get SIB contacts
     *
     * @return array
     */
    public function getContacts()
    {
        $getContactRequest = $this->request($this->apiRequestUrl . "contacts");

        return $getContactRequest;
    }

    /**
     * Get SIB contact by email
     *
     * @param $email
     * @return array
     */
    public function getContact($email)
    {
        $getContactRequest = $this->request($this->apiRequestUrl . "contacts/{$email}");

        return $getContactRequest;
    }

    /**
     * Get SIB custom fields
     *
     * @param $listId
     *
     * @return array
     */
    public function getCustomFields($listId = null)
    {
        $defaultFields = [["name"   => "email", "type" => "text", "obligatory" => true, "values" => [],"desc" => "Email"]];

        $getAttrRequest = $this->request($this->apiRequestUrl . "contacts/attributes");

        if ($getAttrRequest["success"] == false){
            return ["success" => true, "response" => $defaultFields];
        }

        $attributes = $getAttrRequest["response"]["attributes"] ?? [];
        $customFieldNames = [];
        foreach ($attributes as $attribute) {
            if ($attribute["type"] == "text"){
                $customFieldNames[] =[
                    "name" => $attribute["name"],
                    "type" => $attribute["type"],
                    "values" => [],
                    "desc" => $attribute["name"],
                    "obligatory" => false
                ];
            }
        }

        return ["success" => true, "response" => array_merge($defaultFields, $customFieldNames)];
    }

    /**
     * Function to make requests to SendInBlue API
     *
     * @param string $url    The url to use for request
     * @param string $method The method to use for request ( GET/POST )
     * @param array $data    The body to use for request
     *
     * @return array Response of the request
     */
    private function request($url, $method = "GET", array $data = [])
    {
        $client = new Client([
            "headers" => [
                "api-key" => $this->authKey,
            ]
        ]);

        try {
            switch ($method){
                case "GET":
                    $response = $client->get($url);
                    break;
                case "POST":
                    $response = $client->post($url , ["json" => $data]);
                    break;
                case "DELETE":
                    $response = $client->delete($url);
                    break;
            }
        } catch (ClientException $e){
            $responseErrors = json_decode($e->getResponse()->getBody()->getContents(), true);

            $errors = [];

            if (isset($responseErrors["message"])){
                $errors[] = $responseErrors["message"];
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