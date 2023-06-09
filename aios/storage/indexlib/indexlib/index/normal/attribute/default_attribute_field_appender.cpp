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
#include "indexlib/index/normal/attribute/default_attribute_field_appender.h"

#include "indexlib/common/field_format/attribute/attribute_value_initializer.h"
#include "indexlib/common/field_format/attribute/attribute_value_initializer_creator.h"
#include "indexlib/common/field_format/attribute/default_attribute_value_initializer_creator.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"

using namespace std;
using namespace autil;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, DefaultAttributeFieldAppender);

DefaultAttributeFieldAppender::DefaultAttributeFieldAppender() {}

DefaultAttributeFieldAppender::~DefaultAttributeFieldAppender() {}

void DefaultAttributeFieldAppender::Init(const IndexPartitionSchemaPtr& schema,
                                         const InMemorySegmentReaderPtr& inMemSegmentReader)
{
    assert(schema);
    mSchema = schema;
    InitAttributeValueInitializers(mSchema->GetAttributeSchema(), inMemSegmentReader);
    InitAttributeValueInitializers(mSchema->GetVirtualAttributeSchema(), inMemSegmentReader);
}

void DefaultAttributeFieldAppender::AppendDefaultFieldValues(const NormalDocumentPtr& document)
{
    assert(document->GetDocOperateType() == ADD_DOC);
    InitEmptyFields(document, mSchema->GetAttributeSchema(), false);
    InitEmptyFields(document, mSchema->GetVirtualAttributeSchema(), true);
}

void DefaultAttributeFieldAppender::InitAttributeValueInitializers(const AttributeSchemaPtr& attrSchema,
                                                                   const InMemorySegmentReaderPtr& inMemSegmentReader)
{
    if (!attrSchema) {
        return;
    }

    AttributeSchema::Iterator iter = attrSchema->Begin();
    for (; iter != attrSchema->End(); iter++) {
        const AttributeConfigPtr& attrConf = *iter;
        AttributeValueInitializerCreatorPtr creator = attrConf->GetAttrValueInitializerCreator();
        if (!creator) {
            creator.reset(new DefaultAttributeValueInitializerCreator(attrConf));
        }
        fieldid_t fieldId = attrConf->GetFieldId();
        assert(fieldId != INVALID_FIELDID);

        if ((size_t)fieldId >= mAttrInitializers.size()) {
            mAttrInitializers.resize(fieldId + 1);
        }
        mAttrInitializers[fieldId] = creator->Create(inMemSegmentReader);
    }
}

void DefaultAttributeFieldAppender::InitEmptyFields(const NormalDocumentPtr& document,
                                                    const AttributeSchemaPtr& attrSchema, bool isVirtual)
{
    if (!attrSchema) {
        return;
    }

    if (!document->GetAttributeDocument()) {
        AttributeDocumentPtr newAttrDoc(new AttributeDocument);
        newAttrDoc->Reserve(attrSchema->GetAttributeCount());
        newAttrDoc->SetDocId(document->GetDocId());
        document->SetAttributeDocument(newAttrDoc);
    }

    const AttributeDocumentPtr& attrDoc = document->GetAttributeDocument();
    assert(attrDoc);

    uint32_t attrCount = attrSchema->GetAttributeCount();
    uint32_t notEmptyAttrFieldCount = attrDoc->GetNotEmptyFieldCount();
    if (!isVirtual && (notEmptyAttrFieldCount >= attrCount)) {
        IE_LOG(DEBUG, "all attribute field not empty!");
        return;
    }

    docid_t docId = attrDoc->GetDocId();
    AttributeSchema::Iterator iter = attrSchema->Begin();
    for (; iter != attrSchema->End(); iter++) {
        const AttributeConfigPtr& attrConfig = *iter;
        if (attrConfig->IsDeleted()) {
            continue;
        }

        fieldid_t fieldId = attrConfig->GetFieldId();
        assert(fieldId != INVALID_FIELDID);
        bool isNull = false;
        const StringView& fieldValue = attrDoc->GetField(fieldId, isNull);
        if (isNull) {
            continue;
        }

        if (fieldValue.empty() && (attrConfig->GetPackAttributeConfig() == NULL)) {
            StringView initValue;
            if (!mAttrInitializers[fieldId]) {
                IE_LOG(ERROR, "attributeValueInitializer [fieldId:%d] is NULL", fieldId);
                ERROR_COLLECTOR_LOG(ERROR, "attributeValueInitializer [fieldId:%d] is NULL", fieldId);
                continue;
            }

            if (!mAttrInitializers[fieldId]->GetInitValue(docId, initValue, document->GetPool())) {
                IE_LOG(ERROR, "GetInitValue for field [%s] in document [%d] fail!", attrConfig->GetAttrName().c_str(),
                       docId);
                ERROR_COLLECTOR_LOG(ERROR, "GetInitValue for field [%s] in document [%d] fail!",
                                    attrConfig->GetAttrName().c_str(), docId);
                continue;
            }
            attrDoc->SetField(fieldId, initValue);
        }
    }
}
}} // namespace indexlib::index
