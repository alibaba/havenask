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
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/index/normal/primarykey/ordered_primary_key_iterator.h"
#include "indexlib/index/normal/primarykey/primary_key_load_plan.h"
#include "indexlib/index/normal/primarykey/primary_key_pair.h"
#include "indexlib/index/util/segment_output_mapper.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/HashMap.h"

DECLARE_REFERENCE_CLASS(file_system, FileWriter);
namespace indexlib { namespace index {

struct PKMergeOutputData {
    segmentindex_t outputIdx = 0;
    segmentindex_t targetSegmentIndex = 0;
    file_system::FileWriterPtr fileWriter;
};

template <typename Key>
class PrimaryKeyFormatter
{
public:
    typedef index::PKPair<Key> PKPair;
    typedef util::HashMap<Key, docid_t> HashMapType;
    typedef std::shared_ptr<HashMapType> HashMapTypePtr;
    typedef std::shared_ptr<PrimaryKeyIterator<Key>> PrimaryKeyIteratorPtr;
    typedef std::shared_ptr<OrderedPrimaryKeyIterator<Key>> OrderedPrimaryKeyIteratorPtr;

public:
    PrimaryKeyFormatter() {}
    virtual ~PrimaryKeyFormatter() {}

public:
    virtual void Format(const OrderedPrimaryKeyIteratorPtr& pkIter,
                        const file_system::FileWriterPtr& fileWriter) const = 0;

    virtual void Format(const OrderedPrimaryKeyIteratorPtr& pkIter,
                        SegmentOutputMapper<PKMergeOutputData>& outputMapper) const = 0;

    virtual void SerializeToFile(const HashMapTypePtr& hashMap, size_t docCount, autil::mem_pool::PoolBase* pool,
                                 const file_system::FileWriterPtr& file) const = 0;

    virtual size_t CalculatePkSliceFileLen(size_t pkCount, size_t docCount) const = 0;
    virtual void DeserializeToSliceFile(const PrimaryKeyIteratorPtr& pkIterator,
                                        const file_system::FileWriterPtr& sliceFileWriter, size_t sliceFileLen)
    {
        assert(false);
    }

    virtual size_t EstimatePkCount(size_t fileLength, uint32_t docCount) const = 0;
    virtual size_t EstimateDirectLoadSize(const PrimaryKeyLoadPlanPtr& plan) const = 0;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index
