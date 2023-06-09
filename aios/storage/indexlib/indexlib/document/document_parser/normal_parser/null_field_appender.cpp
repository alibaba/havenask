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
#include "indexlib/document/document_parser/normal_parser/null_field_appender.h"

#include "indexlib/config/field_config.h"
#include "indexlib/config/field_schema.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, NullFieldAppender);

NullFieldAppender::NullFieldAppender() {}

NullFieldAppender::~NullFieldAppender() {}

bool NullFieldAppender::Init(const FieldSchemaPtr& fieldSchema)
{
    if (!fieldSchema) {
        return false;
    }

    for (FieldSchema::Iterator it = fieldSchema->Begin(); it != fieldSchema->End(); ++it) {
        const FieldConfigPtr& fieldConfig = *it;
        if (fieldConfig->IsDeleted()) {
            continue;
        }
        if (fieldConfig->IsEnableNullField()) {
            mEnableNullFields.push_back(fieldConfig);
        }
    }
    return !mEnableNullFields.empty();
}

void NullFieldAppender::Append(const RawDocumentPtr& rawDocument)
{
    if (rawDocument->getDocOperateType() != ADD_DOC) {
        return;
    }

    for (size_t i = 0; i < mEnableNullFields.size(); i++) {
        const string& fieldName = mEnableNullFields[i]->GetFieldName();
        if (!rawDocument->exist(fieldName)) {
            rawDocument->setField(fieldName, mEnableNullFields[i]->GetNullFieldLiteralString());
        }
    }
}
}} // namespace indexlib::document
