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

#include "autil/Log.h"
#include "indexlib/index/inverted_index/InvertedIndexMerger.h"
#include "indexlib/index/inverted_index/OnDiskIndexIteratorCreator.h"
#include "indexlib/index/inverted_index/builtin_index/pack/OnDiskPackIndexIterator.h"

namespace indexlib::index {

template <typename DictKey, InvertedIndexType indexType>
class NumberIndexMergerTyped : public InvertedIndexMerger
{
public:
    using OnDiskNumberIndexIterator = OnDiskPackIndexIteratorTyped<DictKey>;

    NumberIndexMergerTyped() = default;
    ~NumberIndexMergerTyped() = default;

    std::string GetIdentifier() const override
    {
        switch (indexType) {
        case it_number_int8:
            return "index.merger.numberInt8";
        case it_number_uint8:
            return "index.merger.numberUInt8";
        case it_number_int16:
            return "index.merger.numberInt16";
        case it_number_uint16:
            return "index.merger.numberUInt16";
        case it_number_int32:
            return "index.merger.numberInt32";
        case it_number_uint32:
            return "index.merger.numberUInt32";
        case it_number_int64:
            return "index.merger.numberInt64";
        case it_number_uint64:
            return "index.merger.numberUInt64";
        default:
            break;
        }
        return "unknown";
    }

    std::shared_ptr<OnDiskIndexIteratorCreator> CreateOnDiskIndexIteratorCreator() override;

private:
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////
template <typename DictKey, InvertedIndexType indexType>
alog::Logger* NumberIndexMergerTyped<DictKey, indexType>::_logger =
    alog::Logger::getLogger("indexlib.index.NumberIndexMergerTyped");

template <typename DictKey, InvertedIndexType indexType>
std::shared_ptr<OnDiskIndexIteratorCreator>
NumberIndexMergerTyped<DictKey, indexType>::CreateOnDiskIndexIteratorCreator()
{
    return std::shared_ptr<OnDiskIndexIteratorCreator>(
        new typename NumberIndexMergerTyped<DictKey, indexType>::OnDiskNumberIndexIterator::Creator(
            this->_indexFormatOption.GetPostingFormatOption(), this->_ioConfig, _indexConfig));
}

} // namespace indexlib::index
