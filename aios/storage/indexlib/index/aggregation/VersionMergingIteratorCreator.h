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

#include "indexlib/config/SortDescription.h"
#include "indexlib/index/aggregation/IValueIterator.h"
#include "indexlib/index/common/field_format/pack_attribute/AttributeReference.h"

namespace indexlibv2::index {

class VersionMergingIteratorCreator
{
public:
    struct DataIterator {
        std::unique_ptr<IValueIterator> data;
        const AttributeReference* uniqueFieldRef = nullptr;
        const AttributeReference* versionFieldRef = nullptr;
        config::SortPattern sortPattern = config::sp_nosort;
    };
    static StatusOr<std::unique_ptr<IValueIterator>> Create(DataIterator dataIter, DataIterator delDataIter,
                                                            autil::mem_pool::PoolBase* pool);

private:
    template <typename T>
    static StatusOr<std::unique_ptr<IValueIterator>>
    CreateWithTypedUniqueField(DataIterator dataIter, DataIterator delDataIter, autil::mem_pool::PoolBase* pool);

    template <typename T1, typename T2>
    static StatusOr<std::unique_ptr<IValueIterator>> CreateImpl(DataIterator dataIter, DataIterator delDataIter,
                                                                autil::mem_pool::PoolBase* pool);

    template <typename T1, typename T2>
    static StatusOr<std::unique_ptr<IValueIterator>> CreateSimple(DataIterator dataIter, DataIterator delDataIter,
                                                                  autil::mem_pool::PoolBase* pool);
};

} // namespace indexlibv2::index
