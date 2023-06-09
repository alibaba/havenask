/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <stdint.h>
#include <vector>


namespace autil {

template<class K, class V>
class Cache
{
public:
    virtual ~Cache(){}
    
public:
    virtual void setCacheSize(uint64_t cacheSize) = 0;
    virtual void warmUp(const std::vector<K>& keyList, const std::vector<V>& valueList) = 0;
    virtual bool invalidate(const K& key) = 0;
    virtual void invalidate(const std::vector<K>& keyList) = 0;
    virtual bool put(const K& key, const V& val) = 0;
    virtual bool get(const K& key, V& val) = 0;
    virtual bool update(const K& key, const V& newVal) = 0;
    virtual float getHitRatio() const = 0;
    virtual bool isInCache(const K& key) = 0;
};

}

