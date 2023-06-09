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
#include "indexlib/config/SortDescription.h"
#include "indexlib/index/common/field_format/pack_attribute/AttributeReference.h"
#include "indexlib/index/kv/config/ValueConfig.h"

namespace indexlibv2::index {
class PackAttributeFormatter;

class PackValueComparator
{
public:
    class AttributeComparator
    {
    public:
        AttributeComparator(AttributeReference* ref, bool desc) : _ref(ref), _desc(desc) {}

    public:
        bool LessThan(const autil::StringView& lfv, const autil::StringView& rhv) const
        {
            return !_desc ? _ref->LessThan(lfv.data(), rhv.data()) : _ref->LessThan(rhv.data(), lfv.data());
        }
        bool Equal(const autil::StringView& lfv, const autil::StringView& rhv) const
        {
            return _ref->Equal(lfv.data(), rhv.data());
        }

    public:
        AttributeReference* TEST_GetAttributeReference() { return _ref; }

    private:
        AttributeReference* _ref;
        bool _desc;
    };

public:
    PackValueComparator();
    ~PackValueComparator();

public:
    bool Init(const std::shared_ptr<config::ValueConfig>& valueConfig,
              const config::SortDescriptions& sortDescriptions);
    bool operator()(const autil::StringView& lhs, const autil::StringView& rhs) const { return Compare(lhs, rhs) < 0; }
    //  -1: lhs < rhs
    //   0: lhs = rhs
    //   1: lhs > rhs
    int Compare(const autil::StringView& lhs, const autil::StringView& rhs) const;

public:
    int32_t TEST_GetFixedValueLen() const { return _fixedValueLen; }
    const std::vector<std::unique_ptr<AttributeComparator>>& TEST_GetSortComps() const { return _sortComps; }

private:
    autil::StringView DecodeValue(const autil::StringView& value) const;

private:
    int32_t _fixedValueLen = -1;
    std::unique_ptr<PackAttributeFormatter> _formatter;
    std::vector<std::unique_ptr<AttributeComparator>> _sortComps;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
