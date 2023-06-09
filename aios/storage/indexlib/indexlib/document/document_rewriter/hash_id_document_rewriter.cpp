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
#include "indexlib/document/document_rewriter/hash_id_document_rewriter.h"

#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::common;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, HashIdDocumentRewriter);

HashIdDocumentRewriter::HashIdDocumentRewriter() : mHashIdFieldId(INVALID_FIELDID) {}

HashIdDocumentRewriter::~HashIdDocumentRewriter() {}

void HashIdDocumentRewriter::Init(const IndexPartitionSchemaPtr& schema)
{
    // check config
    TableType tableType = schema->GetTableType();
    if (tableType == tt_kv || tableType == tt_kkv || tableType == tt_customized) {
        INDEXLIB_FATAL_ERROR(InitializeFailed, "HashIdDocumentRewriter not support kv/kkv/customized table");
    }

    string hashIdFieldName = schema->GetHashIdFieldName();
    const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
    AttributeConfigPtr attrConfig;
    if (attrSchema) {
        attrConfig = attrSchema->GetAttributeConfig(hashIdFieldName);
    }

    if (!attrConfig) {
        INDEXLIB_FATAL_ERROR(InitializeFailed, "hashId field [%s] should in attribute schema.",
                             hashIdFieldName.c_str());
    }

    FieldConfigPtr fieldConfig = attrConfig->GetFieldConfig();
    if (fieldConfig->GetFieldType() != ft_uint16 || fieldConfig->IsMultiValue()) {
        INDEXLIB_FATAL_ERROR(InitializeFailed, "HashIdDocumentRewriter should rewrite (hash id) field with "
                                               "ft_uint16 type and not multiValue");
    }

    mHashIdFieldId = fieldConfig->GetFieldId();
    assert(mHashIdFieldId != INVALID_FIELDID);
    mHashIdConvertor.reset(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig, tableType));
    if (!mHashIdConvertor) {
        INDEXLIB_FATAL_ERROR(InitializeFailed, "HashIdDocumentRewriter init hashId field converter fail.");
    }
    IE_LOG(INFO, "HashIdDocumentRewriter init done with hashId field [%s]", hashIdFieldName.c_str());
}

void HashIdDocumentRewriter::Rewrite(const document::DocumentPtr& doc)
{
    if (doc->GetDocOperateType() != ADD_DOC) {
        return;
    }

    NormalDocumentPtr normalDoc = DYNAMIC_POINTER_CAST(NormalDocument, doc);
    if (!normalDoc) {
        return;
    }

    const AttributeDocumentPtr& attrDoc = normalDoc->GetAttributeDocument();
    if (!attrDoc) {
        INDEXLIB_FATAL_ERROR(UnSupported, "no attribute document!");
    }

    const StringView& value = attrDoc->GetField(mHashIdFieldId);
    if (!value.empty()) {
        IE_LOG(DEBUG, "already set hash id field for pk [%s].", normalDoc->GetPrimaryKey().c_str());
        return;
    }

    const std::string& hashIdTagStr = doc->GetTag(DOCUMENT_HASHID_TAG_KEY);
    if (hashIdTagStr.empty()) {
        IE_LOG(ERROR, "hashId tag value is empty for pk [%s].", normalDoc->GetPrimaryKey().c_str());
        return;
    }
    StringView convertedValue =
        mHashIdConvertor->Encode(StringView(hashIdTagStr), normalDoc->GetPool(), attrDoc->GetFormatErrorLable());
    attrDoc->SetField(mHashIdFieldId, convertedValue);
}
}} // namespace indexlib::document
