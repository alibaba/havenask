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
#include "indexlib/index/normal/source/in_mem_source_segment_reader.h"

#include "autil/ConstString.h"
#include "indexlib/document/index_document/normal_document/source_document_formatter.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, InMemSourceSegmentReader);

InMemSourceSegmentReader::InMemSourceSegmentReader(const config::SourceSchemaPtr& sourceSchema)
    : mSourceSchema(sourceSchema)
    , mMetaAccessor(NULL)
{
}

InMemSourceSegmentReader::~InMemSourceSegmentReader() {}

void InMemSourceSegmentReader::Init(VarLenDataAccessor* metaAccessor,
                                    const std::vector<VarLenDataAccessor*>& dataAccessors)
{
    mMetaAccessor = metaAccessor;
    mDataAccessors = dataAccessors;
}

bool InMemSourceSegmentReader::GetDocument(docid_t localDocId, document::SourceDocument* sourceDocument) const
{
    if ((uint64_t)localDocId >= mMetaAccessor->GetDocCount()) {
        return false;
    }

    document::SerializedSourceDocumentPtr serDoc(new document::SerializedSourceDocument);

    uint8_t* metaValue = NULL;
    uint32_t metaSize = 0;
    mMetaAccessor->ReadData(localDocId, metaValue, metaSize);
    if (metaSize == 0) {
        return false;
    }
    autil::StringView meta((char*)metaValue, metaSize);
    serDoc->SetMeta(meta);

    for (sourcegroupid_t groupId = 0; groupId < mDataAccessors.size(); ++groupId) {
        auto& groupReader = mDataAccessors[groupId];
        uint8_t* value = NULL;
        uint32_t size = 0;
        groupReader->ReadData(localDocId, value, size);
        if (size == 0) {
            return false;
        }
        autil::StringView groupValue((char*)value, size);
        serDoc->SetGroupValue(groupId, groupValue);
    }

    document::SourceDocumentFormatter formatter;
    formatter.Init(mSourceSchema);
    formatter.DeserializeSourceDocument(serDoc, sourceDocument);

    return true;
}
}} // namespace indexlib::index
