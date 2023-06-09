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
#include "indexlib/index/normal/source/source_segment_reader.h"

#include "indexlib/document/index_document/normal_document/source_document_formatter.h"
#include "indexlib/index/data_structure/var_len_data_param_helper.h"
#include "indexlib/index/normal/source/source_define.h"

using autil::StringView;
using std::string;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SourceSegmentReader);

SourceSegmentReader::SourceSegmentReader(const config::SourceSchemaPtr& sourceSchema) : mSourceSchema(sourceSchema) {}

SourceSegmentReader::~SourceSegmentReader() {}

bool SourceSegmentReader::Open(const index_base::SegmentData& segData, const index_base::SegmentInfo& segmentInfo)
{
    if (!mSourceSchema) {
        IE_LOG(ERROR, "source schema is empty");
        return false;
    }

    if (mSourceSchema->IsAllFieldsDisabled()) {
        IE_LOG(ERROR, "all source field group disabled.");
        return false;
    }

    file_system::DirectoryPtr sourceDir = segData.GetSourceDirectory(true);
    for (auto iter = mSourceSchema->Begin(); iter != mSourceSchema->End(); iter++) {
        if ((*iter)->IsDisabled()) {
            mGroupReaders.push_back(VarLenDataReaderPtr());
            continue;
        }

        auto param = VarLenDataParamHelper::MakeParamForSourceData(*iter);
        auto fileCompressConfig = (*iter)->GetParameter().GetFileCompressConfig();
        if (param.dataCompressorName.empty() && fileCompressConfig) {
            param.dataCompressorName = "uncertain";
        }
        VarLenDataReaderPtr groupReader(new VarLenDataReader(param, /*isOnline*/ true));
        groupid_t groupId = (*iter)->GetGroupId();
        file_system::DirectoryPtr groupDir = sourceDir->GetDirectory(SourceDefine::GetDataDir(groupId), true);
        groupReader->Init(segmentInfo.docCount, groupDir, SOURCE_OFFSET_FILE_NAME, SOURCE_DATA_FILE_NAME);
        mGroupReaders.push_back(groupReader);
    }
    mMetaReader.reset(new VarLenDataReader(VarLenDataParamHelper::MakeParamForSourceMeta(), /*isOnline*/ true));
    file_system::DirectoryPtr metaDir = sourceDir->GetDirectory(SOURCE_META_DIR, true);
    mMetaReader->Init(segmentInfo.docCount, metaDir, SOURCE_OFFSET_FILE_NAME, SOURCE_DATA_FILE_NAME);
    return true;
}

bool SourceSegmentReader::GetDocument(docid_t localDocId, document::SourceDocument* sourceDocument) const
{
    // TODO : yiping.typ : maybe use more graceful method to control memory
    // now use source document pool to allocate memory for serDoc
    assert(sourceDocument);
    autil::mem_pool::PoolBase* pool = sourceDocument->GetPool();
    document::SerializedSourceDocumentPtr serDoc(new document::SerializedSourceDocument);
    for (auto& groupReader : mGroupReaders) {
        StringView groupData = StringView::empty_instance();
        if (!groupReader) {
            serDoc->AddGroupValue(groupData);
            continue;
        }

        if (!groupReader->GetValue(localDocId, groupData, pool)) {
            return false;
        }
        serDoc->AddGroupValue(groupData);
    }

    StringView meta = StringView::empty_instance();
    if (!mMetaReader->GetValue(localDocId, meta, pool)) {
        return false;
    }
    serDoc->SetMeta(meta);
    document::SourceDocumentFormatter formatter;
    formatter.Init(mSourceSchema);
    formatter.DeserializeSourceDocument(serDoc, sourceDocument);

    // if (mPool.getTotalBytes() > DEFAULT_POOL_SIZE) {
    //     mPool.release();
    // } else {
    //     mPool.reset();
    // }
    return true;
}
}} // namespace indexlib::index
