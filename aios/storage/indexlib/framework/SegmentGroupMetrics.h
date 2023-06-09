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

#include "autil/legacy/jsonizable.h"
#include "indexlib/util/Exception.h"

namespace indexlib::framework {

class SegmentGroupMetrics
{
public:
    typedef autil::legacy::json::JsonMap Group;
    SegmentGroupMetrics(const Group& group) : _group(group) {}

    SegmentGroupMetrics(const SegmentGroupMetrics& other) : _group(other._group) {}

public:
    // throw InconsistentStateException if failed
    template <typename MetricsType>
    MetricsType Get(const std::string& key) const
    {
        Group::const_iterator it = _group.find(key);
        if (it == _group.end()) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "get [%s] from segment group metrics failed", key.c_str());
        }
        MetricsType retValue;
        autil::legacy::FromJson(retValue, it->second);
        return retValue;
    }

    template <typename MetricsType>
    bool Get(const std::string& key, MetricsType& value) const
    {
        auto it = _group.find(key);
        if (it == _group.end()) {
            return false;
        }
        autil::legacy::FromJson(value, it->second);
        return true;
    }

    std::string ToJsonString() const { return autil::legacy::ToJsonString(_group); }

private:
    const autil::legacy::json::JsonMap& _group;

private:
    inline AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.framework, SegmentGroupMetrics);
};
} // namespace indexlib::framework
