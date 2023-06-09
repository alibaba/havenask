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
#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/extend_document/indexlib_extend_document.h"
#include "indexlib/document/extend_document/tokenize/tokenize_field.h"
#include "indexlib/document/extend_document/tokenize_document.h"
#include "indexlib/test/document_parser.h"

namespace indexlib { namespace test {

class DocumentConvertor
{
public:
    static document::IndexlibExtendDocumentPtr CreateExtendDocFromRawDoc(const config::IndexPartitionSchemaPtr& schema,
                                                                         const test::RawDocumentPtr& rawDoc);

private:
    static void ConvertModifyFields(const config::IndexPartitionSchemaPtr& schema,
                                    const document::IndexlibExtendDocumentPtr& extDoc,
                                    const test::RawDocumentPtr& rawDoc);

    static void PrepareTokenizedDoc(const config::IndexPartitionSchemaPtr& schema,
                                    const document::IndexlibExtendDocumentPtr& extDoc,
                                    const test::RawDocumentPtr& rawDoc);

    static void PrepareTokenizedField(const std::string& fieldName, const config::FieldConfigPtr& fieldConfig,
                                      const config::IndexSchemaPtr& indexSchema, const test::RawDocumentPtr& rawDoc,
                                      const document::TokenizeDocumentPtr& tokenDoc);

    static bool TokenizeValue(const document::TokenizeFieldPtr& field, const std::string& fieldValue,
                              const std::string& sep = test::DocumentParser::DP_TOKEN_SEPARATOR);
    static bool TokenizeSingleValueField(const document::TokenizeFieldPtr& field, const std::string& fieldValue);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentConvertor);

}} // namespace indexlib::test
