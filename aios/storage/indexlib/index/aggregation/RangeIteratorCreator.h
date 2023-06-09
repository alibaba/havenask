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

#include <limits>
#include <memory>

#include "indexlib/config/SortDescription.h"
#include "indexlib/index/aggregation/IValueIterator.h"
#include "indexlib/index/common/field_format/pack_attribute/AttributeReference.h"

namespace indexlibv2::index {

class RangeIteratorCreator
{
public:
    RangeIteratorCreator(const AttributeReference* rangeFieldRef, bool dataSortedByRangeField,
                         config::SortPattern dataSortOrder);

public:
    StatusOr<std::unique_ptr<IValueIterator>> Create(std::unique_ptr<IValueIterator> raw, const std::string& from,
                                                     const std::string& to) const;

private:
    template <typename T>
    StatusOr<std::unique_ptr<IValueIterator>> CreateTyped(std::unique_ptr<IValueIterator> raw, const std::string& from,
                                                          const std::string& to) const;
    template <typename T>
    StatusOr<std::unique_ptr<IValueIterator>> CreateRangeIterator(std::unique_ptr<IValueIterator> raw,
                                                                  const std::string& from, const std::string& to) const;
    template <typename T>
    StatusOr<std::unique_ptr<IValueIterator>>
    CreateFilterIterator(std::unique_ptr<IValueIterator> raw, const std::string& from, const std::string& to) const;

    bool IsDescSort() const { return _sortOrder == config::sp_desc; }

private:
    const AttributeReference* _rangeFieldRef;
    const bool _dataSortedByRangeField;
    const config::SortPattern _sortOrder;
};

} // namespace indexlibv2::index
