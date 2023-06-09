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
#include "indexlib/index/normal/ttl_decoder.h"

#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace indexlib::document;
using namespace indexlib::config;
using namespace indexlib::common;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, TTLDecoder);

TTLDecoder::TTLDecoder(const IndexPartitionSchemaPtr& schema)
{
    mTTLFieldId = INVALID_FIELDID;
    if (!schema) {
        IE_LOG(WARN, "null schema");
        return;
    }
    auto tableType = schema->GetTableType();
    if (tableType == tt_kv || tableType == tt_kkv) {
        // kv/kkv ttl is setted by kv_document_parser
        return;
    }
    const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
    if (!attrSchema) {
        IE_LOG(WARN, "null attribute schema");
        return;
    }

    AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig(schema->GetTTLFieldName());
    if (!attrConfig) {
        IE_LOG(WARN, "can't find ttl field [%s] in attribute", schema->GetTTLFieldName().c_str());
        return;
    }
    mTTLFieldId = attrConfig->GetFieldId();
    mConverter.reset(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
}

TTLDecoder::~TTLDecoder() {}

void TTLDecoder::SetDocumentTTL(const DocumentPtr& document) const
{
    if (INVALID_FIELDID == mTTLFieldId) {
        return;
    }
    // only invert_index support doc ttl
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    const AttributeDocumentPtr& attributeDocument = doc->GetAttributeDocument();
    if (!attributeDocument) {
        return;
    }
    const autil::StringView& field = attributeDocument->GetField(mTTLFieldId);
    uint32_t ttl = 0;
    if (!field.empty()) {
        common::AttrValueMeta meta = mConverter->Decode(field);
        assert(sizeof(uint32_t) == meta.data.size());
        ttl = *(uint32_t*)meta.data.data();
    }
    doc->SetTTL(ttl);
}
}} // namespace indexlib::index
