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
#include "indexlib/base/Status.h"

namespace indexlib::config {

//"combine_segments=true,max_doc_count=3"
class PrimaryKeyLoadStrategyParam
{
public:
    enum PrimaryKeyLoadMode { SORTED_VECTOR = 0, HASH_TABLE, BLOCK_VECTOR };

public:
    PrimaryKeyLoadStrategyParam(PrimaryKeyLoadMode mode = SORTED_VECTOR, bool lookupReverse = false)
        : _loadMode(mode)
        , _needLookupReverse(lookupReverse)
    {
    }

    ~PrimaryKeyLoadStrategyParam() {}

public:
    Status FromString(const std::string& paramStr);
    PrimaryKeyLoadMode GetPrimaryKeyLoadMode() const { return _loadMode; }
    size_t GetMaxDocCount() const { return _maxDocCount; }
    bool NeedCombineSegments() const { return _needCombineSegments; }
    void DisableCombineSegments() { _needCombineSegments = false; }
    Status CheckEqual(const PrimaryKeyLoadStrategyParam& other) const;
    bool NeedLookupReverse() const { return _needLookupReverse; }

public:
    static Status CheckParam(const std::string& param);

private:
    bool ParseParams(const std::vector<std::vector<std::string>>& params);
    bool ParseParam(const std::string& name, const std::string& value);

private:
    static const std::string COMBINE_SEGMENTS;
    static const std::string PARAM_SEP;
    static const std::string KEY_VALUE_SEP;
    static const std::string MAX_DOC_COUNT;
    static const std::string LOOKUP_REVERSE;

private:
    size_t _maxDocCount = 0;
    PrimaryKeyLoadMode _loadMode;
    bool _needCombineSegments = false;
    bool _needLookupReverse;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::config
