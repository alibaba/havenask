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
#ifndef __INDEXLIB_NULL_FIELD_APPENDER_H
#define __INDEXLIB_NULL_FIELD_APPENDER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/raw_document.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, FieldSchema);
DECLARE_REFERENCE_CLASS(config, FieldConfig);

namespace indexlib { namespace document {

class NullFieldAppender
{
public:
    NullFieldAppender();
    ~NullFieldAppender();

public:
    bool Init(const config::FieldSchemaPtr& fieldSchema);
    void Append(const RawDocumentPtr& rawDocument);

private:
    std::vector<config::FieldConfigPtr> mEnableNullFields;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(NullFieldAppender);
}} // namespace indexlib::document

#endif //__INDEXLIB_NULL_FIELD_APPENDER_H
