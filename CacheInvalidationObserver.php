<?php

namespace Core\Observers;

use Illuminate\Support\Facades\Cache;

/**
 * Class CacheInvalidationObserver
 * @package Core\Observers
 * @author Davit Okhikyan <davit@kvsocial.com>
 */
class CacheInvalidationObserver
{
    /**
     * Listen for create event and flush cache for the cache key
     *
     * @param $createdModel
     */
    public function created($createdModel)
    {
        $userId = request('userId');
        // if request does not contain user id try to get that value from model
        if (is_null($userId)){
            $userId = $createdModel->user_id ?? null;
        }

        $cacheKey = $createdModel->cacheKey;
        // clear cache for given key
        Cache::tags("{$cacheKey}_{$userId}")->flush();

        // check if model has related cache keys
        if (isset($createdModel->relatedCacheKeys)){
            // loop over related cache keys and clear those as well
            foreach ($createdModel->relatedCacheKeys as $cacheKey){
                Cache::tags("{$cacheKey}_{$userId}")->flush();
            }
        }
    }

    /**
     * Listen for update event and flush cache for the cache key
     *
     * @param $updatedModel
     */
    public function updated($updatedModel)
    {
        $userId = request('userId');
        // if request does not contain user id try to get that value from model
        if (is_null($userId)){
            $userId = $updatedModel->user_id ?? null;
        }

        $cacheKey = $updatedModel->cacheKey;

        // clear cache for given key
        Cache::tags("{$cacheKey}_{$userId}")->flush();
        // sometimes user id header is missing and we could have cached data with tag {cache_key}_ instead of {cache_key}_{user_id}
        Cache::tags("{$cacheKey}_")->flush();

        // check if model has related cache keys
        if (isset($updatedModel->relatedCacheKeys)){
            // loop over related cache keys and clear those as well
            foreach ($updatedModel->relatedCacheKeys as $cacheKey){
                Cache::tags("{$cacheKey}_{$userId}")->flush();
            }
        }
    }

    /**
     * Listen for delete event and flush cache for the cache key
     *
     * @param $deletedModel
     */
    public function deleted($deletedModel)
    {
        $userId = request('userId');
        // if request does not contain user id try to get that value from model
        if (is_null($userId)){
            $userId = $deletedModel->user_id ?? null;
        }

        $cacheKey = $deletedModel->cacheKey;

        // clear cache for given key
        Cache::tags("{$cacheKey}_{$userId}")->flush();

        // check if model has related cache keys
        if (isset($deletedModel->relatedCacheKeys)){
            // loop over related cache keys and clear those as well
            foreach ($deletedModel->relatedCacheKeys as $cacheKey){
                Cache::tags("{$cacheKey}_{$userId}")->flush();
            }
        }
    }
}
