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
#include "indexlib/index/normal/source/source_writer_impl.h"

#include "indexlib/index/data_structure/var_len_data_param_helper.h"
#include "indexlib/index/normal/source/source_define.h"
#include "indexlib/index_define.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SourceWriterImpl);

SourceWriterImpl::SourceWriterImpl() {}

SourceWriterImpl::~SourceWriterImpl() {}

void SourceWriterImpl::Init(const config::SourceSchemaPtr& sourceSchema,
                            util::BuildResourceMetrics* buildResourceMetrics)
{
    IE_LOG(INFO, "begin init source writer");
    mSourceSchema = sourceSchema;
    for (auto iter = sourceSchema->Begin(); iter != sourceSchema->End(); iter++) {
        SourceGroupWriterPtr writer(new SourceGroupWriter);
        writer->Init(*iter, buildResourceMetrics);
        mDataWriters.push_back(writer);
    }

    mMetaWriter.reset(new GroupFieldDataWriter(std::shared_ptr<config::FileCompressConfig>()));
    mMetaWriter->Init(SOURCE_DATA_FILE_NAME, SOURCE_OFFSET_FILE_NAME, VarLenDataParamHelper::MakeParamForSourceMeta(),
                      buildResourceMetrics);
    IE_LOG(INFO, "end init source writer");
}

void SourceWriterImpl::AddDocument(const document::SerializedSourceDocumentPtr& document)
{
    assert(!mDataWriters.empty());
    const autil::StringView meta = document->GetMeta();
    mMetaWriter->AddDocument(meta);
    for (size_t groupId = 0; groupId < mDataWriters.size(); groupId++) {
        const autil::StringView groupValue = document->GetGroupValue(groupId);
        mDataWriters[groupId]->AddDocument(groupValue);
    }
}

void SourceWriterImpl::Dump(const file_system::DirectoryPtr& dir, autil::mem_pool::PoolBase* dumpPool)
{
    assert(!mDataWriters.empty());
    IE_LOG(INFO, "begin dump source meta");
    file_system::DirectoryPtr metaDir = dir->MakeDirectory(SOURCE_META_DIR);
    mMetaWriter->Dump(metaDir, dumpPool, mTemperatureLayer);
    IE_LOG(INFO, "dump source meta end");
    for (size_t groupId = 0; groupId < mDataWriters.size(); groupId++) {
        file_system::DirectoryPtr groupDir = dir->MakeDirectory(SourceDefine::GetDataDir(groupId));
        mDataWriters[groupId]->Dump(groupDir, dumpPool, mTemperatureLayer);
        IE_LOG(INFO, "dump source group[%lu] end", groupId);
    }
}

const InMemSourceSegmentReaderPtr SourceWriterImpl::CreateInMemSegmentReader()
{
    InMemSourceSegmentReaderPtr reader(new InMemSourceSegmentReader(mSourceSchema));
    VarLenDataAccessor* metaAccessor = mMetaWriter->GetDataAccessor();
    vector<VarLenDataAccessor*> dataAccessors;
    for (size_t groupId = 0; groupId < mDataWriters.size(); groupId++) {
        dataAccessors.push_back(mDataWriters[groupId]->GetDataAccessor());
    }
    reader->Init(metaAccessor, dataAccessors);
    return reader;
}
}} // namespace indexlib::index
