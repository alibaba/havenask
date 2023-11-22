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

#include "indexlib/common_define.h"
#include "indexlib/index/normal/framework/index_merger_creator.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_merger.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger.h"
#include "indexlib/index/normal/inverted_index/builtin_index/pack/on_disk_pack_index_iterator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index { namespace legacy {

template <typename DictKey, InvertedIndexType indexType>
class NumberIndexMergerTyped : public NormalIndexMerger
{
public:
    typedef OnDiskPackIndexIteratorTyped<DictKey> OnDiskNumberIndexIterator;

public:
    NumberIndexMergerTyped() {}
    ~NumberIndexMergerTyped() {}

public:
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
        return "unknow";
    }

    class Creator : public IndexMergerCreator
    {
    public:
        InvertedIndexType GetInvertedIndexType() const { return indexType; }
        IndexMerger* Create() const { return new NumberIndexMergerTyped<DictKey, indexType>(); }
    };

public:
    OnDiskIndexIteratorCreatorPtr CreateOnDiskIndexIteratorCreator() override;

private:
    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////
template <typename DictKey, InvertedIndexType indexType>
alog::Logger* NumberIndexMergerTyped<DictKey, indexType>::_logger =
    alog::Logger::getLogger("indexlib.index.NumberIndexMergerTyped");

template <typename DictKey, InvertedIndexType indexType>
OnDiskIndexIteratorCreatorPtr NumberIndexMergerTyped<DictKey, indexType>::CreateOnDiskIndexIteratorCreator()
{
    return OnDiskIndexIteratorCreatorPtr(
        new typename NumberIndexMergerTyped<DictKey, indexType>::OnDiskNumberIndexIterator::Creator(
            this->GetPostingFormatOption(), this->mIOConfig, mIndexConfig));
}

typedef NumberIndexMergerTyped<uint8_t, it_number_int8> NumberInt8IndexMerger;
typedef NumberIndexMergerTyped<uint8_t, it_number_uint8> NumberUInt8IndexMerger;
typedef NumberIndexMergerTyped<uint16_t, it_number_int16> NumberInt16IndexMerger;
typedef NumberIndexMergerTyped<uint16_t, it_number_uint16> NumberUInt16IndexMerger;
typedef NumberIndexMergerTyped<uint32_t, it_number_int32> NumberInt32IndexMerger;
typedef NumberIndexMergerTyped<uint32_t, it_number_uint32> NumberUInt32IndexMerger;
typedef NumberIndexMergerTyped<uint64_t, it_number_int64> NumberInt64IndexMerger;
typedef NumberIndexMergerTyped<uint64_t, it_number_uint64> NumberUInt64IndexMerger;

DEFINE_SHARED_PTR(NumberInt8IndexMerger);
DEFINE_SHARED_PTR(NumberUInt8IndexMerger);
DEFINE_SHARED_PTR(NumberInt16IndexMerger);
DEFINE_SHARED_PTR(NumberUInt16IndexMerger);
DEFINE_SHARED_PTR(NumberInt32IndexMerger);
DEFINE_SHARED_PTR(NumberUInt32IndexMerger);
DEFINE_SHARED_PTR(NumberInt64IndexMerger);
DEFINE_SHARED_PTR(NumberUInt64IndexMerger);
}}} // namespace indexlib::index::legacy
