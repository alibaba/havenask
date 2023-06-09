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
#include "indexlib/partition/remote_access/partition_seeker.h"

#include "indexlib/partition/offline_partition.h"
#include "indexlib/partition/remote_access/attribute_data_seeker.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::common;
using namespace indexlib::util;
using namespace indexlib::config;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PartitionSeeker);

bool PartitionSeeker::Init(const OfflinePartitionPtr& partition)
{
    Reset();
    mReader.reset();
    if (!partition) {
        return false;
    }

    TableType type = partition->GetSchema()->GetTableType();
    if (type == tt_kkv) {
        IE_LOG(ERROR, "not support kkv table!");
        return false;
    }

    mEnableCountedMultiValue = (type == tt_kv);
    try {
        mReader = partition->GetReader();
        return mReader.get() != NULL;
    } catch (const FileIOException& e) {
        IE_LOG(ERROR, "caught file io exception: %s", e.what());
    } catch (const ExceptionBase& e) {
        // IndexCollapsedException, UnSupportedException,
        // BadParameterException, InconsistentStateException, ...
        IE_LOG(ERROR, "caught exception: %s", e.what());
    } catch (const exception& e) {
        IE_LOG(ERROR, "caught std exception: %s", e.what());
    } catch (...) {
        IE_LOG(ERROR, "caught unknown exception");
    }
    return false;
}

AttributeDataSeeker* PartitionSeeker::GetSingleAttributeSeeker(const string& attrName)
{
    ScopedLock lock(mLock);
    SeekerMap::const_iterator iter = mSeekerMap.find(attrName);
    if (iter != mSeekerMap.end()) {
        IE_LOG(INFO, "Get AttributeDataSeeker from cache!");
        return iter->second;
    }
    AttributeDataSeeker* seeker = CreateSingleAttributeSeeker(attrName, mPool.get());
    mSeekerMap.insert(make_pair(attrName, seeker));
    return seeker;
}

AttributeDataSeeker* PartitionSeeker::CreateSingleAttributeSeeker(const string& attrName, Pool* pool)
{
    IE_LOG(INFO, "create AttributeDataSeeker for attribute [%s]", attrName.c_str());
    if (!mReader) {
        return NULL;
    }
    const auto& schema = mReader->GetSchema();
    const auto& attrSchema = schema->GetAttributeSchema();
    if (!attrSchema) {
        return NULL;
    }
    const auto& attrConf = attrSchema->GetAttributeConfig(attrName);
    if (!attrConf) {
        IE_LOG(ERROR, "[%s] not found in attribute schema!", attrName.c_str());
        return NULL;
    }

    FieldType ft = attrConf->GetFieldType();
    bool isMultiValue = attrConf->IsMultiValue();
    AttributeDataSeeker* seeker = NULL;
    switch (ft) {
#define MACRO(field_type)                                                                                              \
    case field_type: {                                                                                                 \
        if (!isMultiValue) {                                                                                           \
            typedef typename FieldTypeTraits<field_type>::AttrItemType T;                                              \
            seeker = POOL_COMPATIBLE_NEW_CLASS(pool, AttributeDataSeekerTyped<T>, pool);                               \
        } else {                                                                                                       \
            typedef typename FieldTypeTraits<field_type>::AttrItemType T;                                              \
            typedef typename autil::MultiValueType<T> MT;                                                              \
            seeker = POOL_COMPATIBLE_NEW_CLASS(pool, AttributeDataSeekerTyped<MT>, pool);                              \
        }                                                                                                              \
        break;                                                                                                         \
    }
        NUMBER_FIELD_MACRO_HELPER(MACRO);
#undef MACRO

    case ft_string:
        if (!isMultiValue) {
            seeker = POOL_COMPATIBLE_NEW_CLASS(pool, AttributeDataSeekerTyped<MultiChar>, pool);
        } else {
            seeker = POOL_COMPATIBLE_NEW_CLASS(pool, AttributeDataSeekerTyped<MultiString>, pool);
        }
        break;
    default:
        assert(false);
    }

    if (!seeker) {
        return NULL;
        ;
    }
    if (!seeker->Init(mReader, attrConf)) {
        IE_LOG(WARN, "create AttributeDataSeeker for attribute [%s] failed", attrName.c_str());
        POOL_COMPATIBLE_DELETE_CLASS(pool, seeker);
        return NULL;
    }
    return seeker;
}

void PartitionSeeker::Reset()
{
    Pool* pool = mPool.get();
    SeekerMap::const_iterator iter = mSeekerMap.begin();
    for (; iter != mSeekerMap.end(); iter++) {
        POOL_COMPATIBLE_DELETE_CLASS(pool, iter->second);
    }
    mSeekerMap.clear();
    mPool.reset(new Pool);
}
}} // namespace indexlib::partition
