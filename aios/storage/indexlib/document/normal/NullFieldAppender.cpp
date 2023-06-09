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
#include "indexlib/document/normal/NullFieldAppender.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/document/RawDocument.h"

using namespace std;
using namespace indexlibv2::config;

namespace indexlibv2 { namespace document {
AUTIL_LOG_SETUP(indexlib.document, NullFieldAppender);

NullFieldAppender::NullFieldAppender() {}

NullFieldAppender::~NullFieldAppender() {}

bool NullFieldAppender::Init(const vector<shared_ptr<FieldConfig>>& fieldConfigs)
{
    for (const auto& fieldConfig : fieldConfigs) {
        if (fieldConfig->IsEnableNullField()) {
            _enableNullFields.push_back(fieldConfig);
        }
    }
    return !_enableNullFields.empty();
}

void NullFieldAppender::Append(const shared_ptr<RawDocument>& rawDocument)
{
    if (rawDocument->getDocOperateType() != ADD_DOC) {
        return;
    }

    for (size_t i = 0; i < _enableNullFields.size(); i++) {
        const string& fieldName = _enableNullFields[i]->GetFieldName();
        if (!rawDocument->exist(fieldName)) {
            rawDocument->setField(fieldName, _enableNullFields[i]->GetNullFieldLiteralString());
        }
    }
}
}} // namespace indexlibv2::document
