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
#include "indexlib/partition/remote_access/index_data_patcher.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/document/document_rewriter/section_attribute_appender.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/document/index_field_convertor.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer_factory.h"
#include "indexlib/index_define.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/SimplePool.h"
#include "indexlib/util/memory_control/BuildResourceCalculator.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::document;
using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, IndexDataPatcher);

bool IndexDataPatcher::Init(const IndexConfigPtr& indexConfig, const DirectoryPtr& segDir, uint32_t docCount)
{
    mIndexConfig = indexConfig;
    mDocCount = docCount;
    mPatchDocCount = 0;
    mPool.reset(new autil::mem_pool::Pool);
    mIndexDoc.reset(new IndexDocument(mPool.get()));
    IndexConfig::Iterator iter = indexConfig->CreateIterator();
    while (iter.HasNext()) {
        auto fieldConfig = iter.Next();
        fieldid_t fieldId = fieldConfig->GetFieldId();
        assert(fieldId >= 0);
        mFieldConfigMap[fieldId] = fieldConfig;
    }
    mConvertor.reset(new IndexFieldConvertor(mSchema));

    if (mDocCount == 0) {
        IE_LOG(INFO, "Init IndexDataPatcher [%s] for empty segment [%s]", indexConfig->GetIndexName().c_str(),
               segDir->DebugString().c_str());
        return true;
    }

    segDir->MakeDirectory(INDEX_DIR_NAME);
    mIndexDir = segDir->GetDirectory(INDEX_DIR_NAME, false);
    if (mIndexDir == nullptr) {
        IE_LOG(ERROR, "get directory failed, path[%s/%s]!", segDir->DebugString().c_str(), INDEX_DIR_NAME.c_str());
        return false;
    }

    string targetDirPath = indexConfig->GetIndexName();
    if (mIndexDir->IsExist(targetDirPath)) {
        IE_LOG(ERROR, "target path [%s/%s] already exist, will be cleaned!", mIndexDir->DebugString().c_str(),
               targetDirPath.c_str());
        indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
        mIndexDir->RemoveDirectory(targetDirPath, removeOption /*mayNonExsit*/);
    }

    mBuildResourceMetrics.reset(new BuildResourceMetrics);
    return DoInit();
}

void IndexDataPatcher::AddField(const string& fieldValue, fieldid_t fieldId)
{
    FieldId2FieldConfigMap::const_iterator iter = mFieldConfigMap.find(fieldId);
    if (iter == mFieldConfigMap.end()) {
        IE_LOG(ERROR, "field [%d] not in index [%s]", fieldId, mIndexConfig->GetIndexName().c_str());
        return;
    }
    mConvertor->convertIndexField(mIndexDoc, iter->second, fieldValue, INDEXLIB_MULTI_VALUE_SEPARATOR_STR, mPool.get());
}

void IndexDataPatcher::EndDocument()
{
    if (mIndexWriter == nullptr) {
        return;
    }

    assert(mIndexDoc);
    FieldId2FieldConfigMap::const_iterator iter = mFieldConfigMap.begin();
    while (iter != mFieldConfigMap.end()) {
        Field* field = mIndexDoc->GetField(iter->first);
        if (field) {
            mIndexWriter->AddField(field);
        }
        iter++;
    }

    mIndexDoc->SetDocId(mPatchDocCount);
    if (mSectionAttrAppender) {
        if (!mSectionAttrAppender->AppendSectionAttribute(mIndexDoc)) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "append section attribute fail for docid [%d]!",
                                 mIndexDoc->GetDocId());
        }
    }
    if (mIndexDoc->GetDocId() >= (docid_t)mDocCount) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "docId [%d] over range!", mIndexDoc->GetDocId());
    }
    mIndexWriter->EndDocument(*mIndexDoc);
    ++mPatchDocCount;
    mIndexDoc->ClearData();
    if (mPool->getUsedBytes() >= MAX_POOL_MEMORY_THRESHOLD) {
        mPool->release();
    }
}

void IndexDataPatcher::Close()
{
    if (mIndexWriter == nullptr) {
        return;
    }
    if (mPatchDocCount == 0) {
        for (size_t i = 0; i < mDocCount; i++) {
            EndDocument();
        }
    }
    mIndexWriter->EndSegment();
    SimplePool pool;
    mIndexWriter->Dump(mIndexDir, &pool);
    mIndexWriter = nullptr;
}

int64_t IndexDataPatcher::GetCurrentTotalMemoryUse() const
{
    return mPool->getUsedBytes() + BuildResourceCalculator::GetCurrentTotalMemoryUse(mBuildResourceMetrics);
}

uint32_t IndexDataPatcher::GetDistinctTermCount() const
{
    if (mIndexWriter == nullptr) {
        return 0;
    }
    std::shared_ptr<indexlib::framework::SegmentMetrics> segMetrics(new indexlib::framework::SegmentMetrics);
    mIndexWriter->FillDistinctTermCount(segMetrics);
    if (mIndexConfig->GetShardingType() != IndexConfig::IST_NEED_SHARDING) {
        return segMetrics->GetDistinctTermCount(mIndexConfig->GetIndexName());
    }

    uint32_t termCount = 0;
    auto indexConfigs = mIndexConfig->GetShardingIndexConfigs();
    for (size_t i = 0; i < indexConfigs.size(); i++) {
        termCount += segMetrics->GetDistinctTermCount(indexConfigs[i]->GetIndexName());
    }
    return termCount;
}

bool IndexDataPatcher::DoInit()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    std::shared_ptr<indexlib::framework::SegmentMetrics> segMetrics(new indexlib::framework::SegmentMetrics);
    segMetrics->SetDistinctTermCount(mIndexConfig->GetIndexName(), mInitDistinctTermCount);
    if (mIndexConfig->GetShardingType() == IndexConfig::IST_NEED_SHARDING) {
        auto indexConfigs = mIndexConfig->GetShardingIndexConfigs();
        uint32_t termCount = mInitDistinctTermCount / indexConfigs.size();
        for (size_t i = 0; i < indexConfigs.size(); i++) {
            segMetrics->SetDistinctTermCount(indexConfigs[i]->GetIndexName(), termCount);
        }
    }

    mIndexWriter = IndexWriterFactory::CreateIndexWriter(INVALID_SEGMENTID, mIndexConfig, segMetrics, options,
                                                         mBuildResourceMetrics.get(), mPluginManager,
                                                         /*segIter=*/nullptr);
    if (mIndexWriter == nullptr) {
        IE_LOG(ERROR, "create index writer for [%s] fail!", mIndexConfig->GetIndexName().c_str());
        return false;
    }

    InvertedIndexType type = mIndexConfig->GetInvertedIndexType();
    if (type != it_pack && type != it_expack) {
        return true;
    }

    PackageIndexConfigPtr packIndex = DYNAMIC_POINTER_CAST(PackageIndexConfig, mIndexConfig);
    if (packIndex && packIndex->HasSectionAttribute()) {
        mSectionAttrAppender.reset(new SectionAttributeAppender);
        if (!mSectionAttrAppender->Init(mSchema)) {
            IE_LOG(ERROR, "create section attribute appender for index [%s] fail!",
                   mIndexConfig->GetIndexName().c_str());
            return false;
        }
    }
    return true;
}
}} // namespace indexlib::partition
