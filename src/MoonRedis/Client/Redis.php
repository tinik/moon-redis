<?php

namespace MoonRedis\Client;

use RedisClient\ClientFactory;


class Redis extends \Zend_Cache_Backend implements \Zend_Cache_Backend_ExtendedInterface
{

    const SET_IDS = 'zc:ids';
    const SET_TAGS = 'zc:tags';

    const PREFIX_KEY = 'zc:k:';
    const PREFIX_TAG_IDS = 'zc:ti:';

    const FIELD_DATA = 'd';
    const FIELD_MTIME = 'm';
    const FIELD_TAGS = 't';
    const FIELD_INF = 'i';

    const MAX_LIFETIME = 2592000; /* Redis backend limit */
    const DEFAULT_CONNECT_TIMEOUT = 5;
    const DEFAULT_CONNECT_RETRIES = 1;

    private $_redis;

    /** @var bool */
    protected $_notMatchingTags = false;

    public function __construct(array $options = [])
    {
        parent::__construct($options);

        if (isset($options['notMatchingTags'])) {
            $this->_notMatchingTags = (bool)$options['notMatchingTags'];
        }

        $this->_options['automatic_cleaning_factor'] = 0;
        if (isset($options['automatic_cleaning_factor'])) {
            $this->_options['automatic_cleaning_factor'] = (int)$options['automatic_cleaning_factor'];
        }

        $this->_redis = ClientFactory::create($options);
    }

    public function getIds()
    {
        if ($this->_notMatchingTags) {
            return (array)$this->_redis->smembers(self::SET_IDS);
        }

        $prefixLen = strlen(self::PREFIX_KEY);

        $keys = $this->_redis->keys(self::PREFIX_KEY . '*');
        foreach ($keys as $index => $key) {
            $keys[$index] = substr($key, $prefixLen);
        }

        return $keys;
    }

    public function getTags()
    {
        return (array)$this->_redis->smembers(self::SET_TAGS);
    }

    public function getIdsMatchingTags($tags = [])
    {
        if ($tags) {
            $tagIds = $this->_preprocessTagIds($tags);
            return (array)$this->_redis->sinter($tagIds);
        }

        return [];
    }

    public function getIdsNotMatchingTags($tags = [])
    {
        if (!$this->_notMatchingTags) {
            \Zend_Cache::throwException("notMatchingTags is currently disabled.");
        }

        if ($tags) {
            $tagIds = $this->_preprocessTagIds($tags);
            return (array)$this->_redis->sdiff(self::SET_IDS, $tagIds);
        }

        return (array)$this->_redis->smembers(self::SET_IDS);
    }

    public function getIdsMatchingAnyTags($tags = [])
    {
        $result = [];
        if ($tags) {
            $chunks = array_chunk($tags, 256);
            foreach ($chunks as $chunk) {
                $tagIds = $this->_preprocessTagIds($chunk);
                $result = array_merge($result, (array)$this->_redis->sunion($tagIds));
            }

            if (count($chunks) > 1) {
                // since we are chunking requests, we must de-duplicate member names
                $result = array_unique($result);
            }
        }

        return $result;
    }

    public function getFillingPercentage()
    {
        $maxMem = $this->_redis->config('GET', 'maxmemory');
        if (0 == (int)$maxMem['maxmemory']) {
            return 1;
        }

        $info = $this->_redis->info();
        return (int)round(
            ($info['used_memory'] / $maxMem['maxmemory'] * 100),
            0,
            PHP_ROUND_HALF_UP
        );
    }

    public function getMetadatas($id)
    {
        $values = $this->_redis->hmget(self::PREFIX_KEY . $id, [self::FIELD_TAGS, self::FIELD_MTIME, self::FIELD_INF]);

        list($tags, $mtime, $inf) = array_values($values);
        if (!$mtime) {
            return false;
        }

        $tags = explode(',', $tags);
        $expire = $inf === '1' ? false : time() + $this->_redis->ttl(self::PREFIX_KEY . $id);

        return [
            'expire' => $expire,
            'mtime' => $mtime,
            'tags' => $tags,
        ];
    }

    public function touch($id, $extraLifetime)
    {
        list($inf) = $this->_redis->hget(self::PREFIX_KEY . $id, self::FIELD_INF);
        if ($inf === '0') {
            $expireAt = time() + $this->_redis->ttl(self::PREFIX_KEY . $id) + $extraLifetime;
            return (bool)$this->_redis->expireAt(self::PREFIX_KEY . $id, $expireAt);
        }

        return false;
    }

    public function getCapabilities()
    {
        return [
            'automatic_cleaning' => ($this->_options['automatic_cleaning_factor'] > 0),
            'tags' => true,
            'expired_read' => false,
            'priority' => false,
            'infinite_lifetime' => true,
            'get_list' => true,
        ];
    }

    public function test($id)
    {
        // Don't use slave for this since `test` is usually used for locking
        $mtime = $this->_redis->hget(self::PREFIX_KEY . $id, self::FIELD_MTIME);
        return ($mtime ? $mtime : false);
    }

    public function load($id, $doNotTestCacheValidity = false)
    {
        try {
            $data = $this->_redis->hget(self::PREFIX_KEY . $id, self::FIELD_DATA);
            if ($data == null) {
                return false;
            }

            $this->_redis->expire(self::PREFIX_KEY . $id, min(0, self::MAX_LIFETIME));
            $values = json_decode($data, true);
            if (isset($values['data'])) {
                return $values['data'];
            }
        } catch (\Exception $e) {
            $this->remove($id);
        }

        return null;
    }

    public function save($data, $id, $tags = [], $specificLifetime = false)
    {
        $this->_redis->multi();

        if (!is_array($tags)) {
            $tags = $tags ? [$tags] : [];
        } else {
            $tags = array_flip(array_flip($tags));
        }

        $lifetime = $this->getLifetime($specificLifetime);

        // Set the data
        $result = $this->_redis->hmset(self::PREFIX_KEY . $id, [
            self::FIELD_INF => $lifetime ? 0 : 1,
            self::FIELD_DATA => json_encode(['data' => $data]),
            self::FIELD_TAGS => implode(',', $tags),
            self::FIELD_MTIME => time(),
        ]);

        if (!$result) {
            \Zend_Cache::throwException("Could not set cache key $id");
        }

        // Set expiration if specified
        if ($lifetime) {
            $this->_redis->expire(self::PREFIX_KEY . $id, min($lifetime, self::MAX_LIFETIME));
        }

        // Process added tags
        if ($tags) {
            // Update the list with all the tags
            $this->_redis->sadd(self::SET_TAGS, $tags);

            // Update the id list for each tag
            foreach ($tags as $tag) {
                $this->_redis->sadd(self::PREFIX_TAG_IDS . $tag, $id);
            }
        }

        $this->_redis->exec();
        return true;
    }

    public function remove($id)
    {
        $this->_redis->multi();

        // Get list of tags for this id
        $tags = $this->getTagsById($id);

        // Remove data
        $this->_redis->del(self::PREFIX_KEY . $id);

        // Remove id from list of all ids
        if ($this->_notMatchingTags) {
            $this->_redis->srem(self::SET_IDS, $id);
        }

        // Update the id list for each tag
        foreach ($tags as $tag) {
            $this->_redis->srem(self::PREFIX_TAG_IDS . $tag, $id);
        }

        $this->_redis->exec();
        return true;
    }

    public function clean($mode = \Zend_Cache::CLEANING_MODE_ALL, $tags = [])
    {
        if ($mode == \Zend_Cache::CLEANING_MODE_ALL) {
            $this->_redis->flushall();
            return true;
        }

        if ($mode == \Zend_Cache::CLEANING_MODE_MATCHING_TAG) {
            $keys = $this->getIdsMatchingTags($tags);
            return $this->flushByIds($keys);
        }

        if ($mode == \Zend_Cache::CLEANING_MODE_NOT_MATCHING_TAG) {
            $keys = $this->getIdsNotMatchingTags($tags);
            return $this->flushByIds($keys);
        }

        return false;
    }

    protected function flushByIds($keys)
    {
        if ($keys) {
            $this->_redis->multi();

            // Remove data
            $this->_redis->del($this->_preprocessIds($keys));

            // Remove ids from list of all ids
            if ($this->_notMatchingTags) {
                $this->_redis->srem(self::SET_IDS, $keys);
            }

            $this->_redis->exec();
        }

        return true;
    }

    protected function getTagsById($id)
    {
        $values = $this->_redis->hget(self::PREFIX_KEY . $id, self::FIELD_TAGS);
        return explode(',', $values);
    }

    /**
     * @param $item
     * @param $index
     * @param $prefix
     */
    protected function _preprocess(&$item, $index, $prefix)
    {
        $item = $prefix . $item;
    }

    /**
     * @param $ids
     * @return array
     */
    protected function _preprocessIds($ids)
    {
        array_walk($ids, [$this, '_preprocess'], self::PREFIX_KEY);
        return $ids;
    }

    /**
     * @param $tags
     * @return array
     */
    protected function _preprocessTagIds($tags)
    {
        array_walk($tags, [$this, '_preprocess'], self::PREFIX_TAG_IDS);
        return $tags;
    }
}
