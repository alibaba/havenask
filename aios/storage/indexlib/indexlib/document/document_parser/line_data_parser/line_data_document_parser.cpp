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
#include "indexlib/document/document_parser/line_data_parser/line_data_document_parser.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::common;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, LineDataDocumentParser);

LineDataDocumentParser::LineDataDocumentParser(const IndexPartitionSchemaPtr& schema) : DocumentParser(schema) {}

LineDataDocumentParser::~LineDataDocumentParser() {}

bool LineDataDocumentParser::DoInit()
{
    if (mSchema->GetTableType() != tt_linedata) {
        ERROR_COLLECTOR_LOG(ERROR,
                            "init LineDataDocumentParser fail, "
                            "unsupport tableType [%s]!",
                            IndexPartitionSchema::TableType2Str(mSchema->GetTableType()).c_str());
        return false;
    }
    return true;
}

DocumentPtr LineDataDocumentParser::Parse(const IndexlibExtendDocumentPtr& extendDoc)
{
    const RawDocumentPtr& rawDoc = extendDoc->GetRawDocument();
    if (!rawDoc) {
        IE_LOG(ERROR, "empty raw document!");
        return DocumentPtr();
    }

    const ClassifiedDocumentPtr& classifiedDoc = extendDoc->getClassifiedDocument();
    NormalDocumentPtr indexDoc(new NormalDocument(classifiedDoc->getPoolPtr()));
    indexDoc->SetIndexDocument(classifiedDoc->getIndexDocument());
    indexDoc->SetSummaryDocument(classifiedDoc->getSerSummaryDoc());
    indexDoc->SetAttributeDocument(classifiedDoc->getAttributeDoc());
    classifiedDoc->clear();

    int64_t timestamp = rawDoc->getDocTimestamp();
    DocOperateType opType = rawDoc->getDocOperateType();
    indexDoc->SetTimestamp(timestamp);
    indexDoc->SetSource(rawDoc->getDocSource());
    indexDoc->SetDocOperateType(opType);
    indexDoc->SetRegionId(extendDoc->getRegionId());

    // attention: make binary_version=8 document not used for docTrace,
    // atomicly serialize to version6, to make online compitable
    // remove this code when binary version 6 is useless
    if (!extendDoc->GetRawDocument()->NeedTrace() && indexDoc->GetSerializedVersion() == 8) {
        indexDoc->SetSerializedVersion(6);
    }
    return indexDoc;
}

DocumentPtr LineDataDocumentParser::Parse(const StringView& serializedData)
{
    DataBuffer dataBuffer(const_cast<char*>(serializedData.data()), serializedData.length());
    NormalDocumentPtr document;
    dataBuffer.read(document);
    return document;
}
}} // namespace indexlib::document
