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
