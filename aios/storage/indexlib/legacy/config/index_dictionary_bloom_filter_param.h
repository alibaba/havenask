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

#include <memory>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"

namespace indexlib { namespace config {

class IndexDictionaryBloomFilterParam : public autil::legacy::Jsonizable
{
public:
    IndexDictionaryBloomFilterParam() {}
    IndexDictionaryBloomFilterParam(const IndexDictionaryBloomFilterParam& other)
        : indexName(other.indexName)
        , multipleNum(other.multipleNum)
    {
    }
    ~IndexDictionaryBloomFilterParam() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("index_name", indexName, indexName);
        json.Jsonize("multiple_num", multipleNum, multipleNum);
        if (json.GetMode() == autil::legacy::Jsonizable::FROM_JSON) {
            if (multipleNum <= 1 || multipleNum > 16) {
                multipleNum = DEFAULT_BLOOM_FILTER_MULTIPLE_NUM;
            }
        }
    }

    bool operator==(const IndexDictionaryBloomFilterParam& other) const
    {
        return indexName == other.indexName && multipleNum == other.multipleNum;
    }

public:
    static constexpr uint32_t DEFAULT_BLOOM_FILTER_MULTIPLE_NUM = 5;
    std::string indexName;
    uint32_t multipleNum = DEFAULT_BLOOM_FILTER_MULTIPLE_NUM;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<IndexDictionaryBloomFilterParam> IndexDictionaryBloomFilterParamPtr;

}} // namespace indexlib::config
