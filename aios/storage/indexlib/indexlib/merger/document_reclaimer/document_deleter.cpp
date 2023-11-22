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
#include "indexlib/merger/document_reclaimer/document_deleter.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <iterator>
#include <vector>

#include "alog/Logger.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "indexlib/base/Constant.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/fslib/FslibOption.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_factory.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil::mem_pool;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::file_system;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, DocumentDeleter);

DocumentDeleter::DocumentDeleter(const IndexPartitionSchemaPtr& schema) : mPool(new Pool), mSchema(schema) {}

DocumentDeleter::~DocumentDeleter()
{
    // reset writer before reset pool
    mDeletionMapWriter.reset();
    mSubDeletionMapWriter.reset();

    mPartitionData.reset();
}

void DocumentDeleter::Init(const index_base::PartitionDataPtr& partitionData)
{
    mPartitionData = partitionData;
    mDeletionMapWriter.reset(new DeletionMapWriter(true));
    mDeletionMapWriter->Init(partitionData.get());
    if (mSchema->GetSubIndexPartitionSchema()) {
        mMainJoinAttributeReader = AttributeReaderFactory::CreateJoinAttributeReader(
            mSchema, MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME, partitionData);
        mSubDeletionMapWriter.reset(new DeletionMapWriter(mPool.get()));
        mSubDeletionMapWriter->Init(partitionData->GetSubPartitionData().get());
    }
}

bool DocumentDeleter::Delete(docid_t docId)
{
    assert(mDeletionMapWriter);
    if (mSubDeletionMapWriter) {
        docid_t subDocIdBegin = INVALID_DOCID;
        docid_t subDocIdEnd = INVALID_DOCID;
        GetSubDocIdRange(docId, subDocIdBegin, subDocIdEnd);
        // [Begin, end)
        for (docid_t i = subDocIdBegin; i < subDocIdEnd; ++i) {
            if (!mSubDeletionMapWriter->Delete(i)) {
                IE_LOG(ERROR, "failed to delete sub document [%d]", i);
            }
        }
    }
    return mDeletionMapWriter->Delete(docId);
}

void DocumentDeleter::Dump(const file_system::DirectoryPtr& directory)
{
    assert(mDeletionMapWriter);
    mDeletionMapWriter->Dump(directory);
    if (mSubDeletionMapWriter) {
        DirectoryPtr subDirectory = directory->MakeDirectory(SUB_SEGMENT_DIR_NAME);
        mSubDeletionMapWriter->Dump(subDirectory);
    }
}

bool DocumentDeleter::IsDirty() const
{
    assert(mDeletionMapWriter);
    return mDeletionMapWriter->IsDirty();
}

uint32_t DocumentDeleter::GetMaxTTL() const
{
    vector<segmentid_t> patchSegIds;
    mDeletionMapWriter->GetPatchedSegmentIds(patchSegIds);
    if (mSubDeletionMapWriter) {
        mSubDeletionMapWriter->GetPatchedSegmentIds(patchSegIds);
    }

    sort(patchSegIds.begin(), patchSegIds.end());
    vector<segmentid_t>::iterator it = unique(patchSegIds.begin(), patchSegIds.end());
    patchSegIds.resize(distance(patchSegIds.begin(), it));
    uint32_t maxTTL = 0;
    ;
    for (size_t i = 0; i < patchSegIds.size(); ++i) {
        index_base::SegmentData segData = mPartitionData->GetSegmentData(patchSegIds[i]);
        maxTTL = max(maxTTL, segData.GetSegmentInfo()->maxTTL);
    }
    return maxTTL;
}

void DocumentDeleter::GetSubDocIdRange(docid_t mainDocId, docid_t& subDocIdBegin, docid_t& subDocIdEnd) const
{
    subDocIdBegin = 0;
    if (mainDocId > 0) {
        subDocIdBegin = mMainJoinAttributeReader->GetJoinDocId(mainDocId - 1);
    }
    subDocIdEnd = mMainJoinAttributeReader->GetJoinDocId(mainDocId);

    if (subDocIdEnd < 0 || subDocIdBegin < 0 || (subDocIdBegin > subDocIdEnd)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "failed to get join docid [%d, %d), mainDocId [%d]", subDocIdBegin,
                             subDocIdEnd, mainDocId);
    }
}
}} // namespace indexlib::merger
