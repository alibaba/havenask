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
#include "indexlib/partition/join_cache/join_cache_initializer.h"

#include "indexlib/common/field_format/attribute/single_value_attribute_convertor.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/partition/index_partition_reader.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::index;
using namespace indexlib::common;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, JoinCacheInitializer);

JoinCacheInitializer::JoinCacheInitializer() {}

JoinCacheInitializer::~JoinCacheInitializer() {}

void JoinCacheInitializer::Init(const AttributeReaderPtr& attrReader, const IndexPartitionReaderPtr auxReader)
{
    assert(attrReader);
    assert(auxReader);

    mJoinFieldAttrReader = attrReader;
    mAuxPartitionReader = auxReader;
    mPkIndexReader = mAuxPartitionReader->GetPrimaryKeyReader();
    assert(mPkIndexReader);
}

bool JoinCacheInitializer::GetInitValue(docid_t docId, char* buffer, size_t bufLen) const
{
    docid_t joinDocId = GetJoinDocId(docId);
    assert(bufLen >= sizeof(docid_t));
    docid_t& value = *((docid_t*)buffer);
    value = joinDocId;
    return true;
}

bool JoinCacheInitializer::GetInitValue(docid_t docId, StringView& value, Pool* memPool) const
{
    docid_t joinDocId = GetJoinDocId(docId);
    value = SingleValueAttributeConvertor<docid_t>::EncodeValue(joinDocId, memPool);
    return true;
}

docid_t JoinCacheInitializer::GetJoinDocId(docid_t docId) const
{
    string joinFieldValue;
    assert(mJoinFieldAttrReader);
    if (!mJoinFieldAttrReader->Read(docId, joinFieldValue, NULL)) {
        IE_LOG(ERROR, "Read join field value fail!");
        return INVALID_DOCID;
    }

    assert(mPkIndexReader);
    return mPkIndexReader->Lookup(joinFieldValue);
}
}} // namespace indexlib::partition
