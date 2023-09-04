#include "indexlib/test/sub_document_extractor.h"

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, SubDocumentExtractor);

namespace {
string LAST_VALUE_PREFIX = "__last_value__";
}

SubDocumentExtractor::SubDocumentExtractor(const IndexPartitionSchemaPtr& schema) : _schema(schema) {}

SubDocumentExtractor::~SubDocumentExtractor() {}

void SubDocumentExtractor::extractSubDocuments(const RawDocumentPtr& originalDocument,
                                               vector<RawDocumentPtr>& subDocuments)
{
    const IndexPartitionSchemaPtr& subSchema = _schema->GetSubIndexPartitionSchema();
    assert(subSchema);

    const FieldSchemaPtr& subFieldSchema = subSchema->GetFieldSchema();
    for (RawDocument::Iterator iter = originalDocument->Begin(); iter != originalDocument->End();) {
        const string& fieldName = iter->first;
        const string& fieldValue = iter->second;
        if (subFieldSchema->IsFieldNameInSchema(fieldName)) {
            addFieldsToSubDocuments(fieldName, fieldValue, subDocuments);
            iter++;
            originalDocument->Erasefield(fieldName);
        } else {
            iter++;
            continue;
        }
    }
    string fieldValue = originalDocument->GetField(RESERVED_SUB_MODIFY_FIELDS);
    addFieldsToSubDocuments(RESERVED_MODIFY_FIELDS, fieldValue, subDocuments);
    fieldValue = originalDocument->GetField(RESERVED_SUB_MODIFY_VALUES);
    addFieldsToSubDocuments(RESERVED_MODIFY_VALUES, fieldValue, subDocuments);

    const string& timeStamp = originalDocument->GetField(RESERVED_TIMESTAMP);
    for (size_t i = 0; i < subDocuments.size(); i++) {
        subDocuments[i]->SetDocOperateType(originalDocument->GetDocOperateType());
        subDocuments[i]->SetField(RESERVED_TIMESTAMP, timeStamp);
    }
}

void SubDocumentExtractor::addFieldsToSubDocuments(const string& fieldName, const string& fieldValue,
                                                   vector<RawDocumentPtr>& subDocuments)
{
    if (fieldValue.empty()) {
        return;
    }

    const vector<string>& subFieldValues = StringUtil::split(fieldValue, SUB_DOC_SEPARATOR, false);

    if (subDocuments.size() != 0 && subFieldValues.size() != subDocuments.size()) {
        IE_LOG(WARN, "field[%s]:value[%s] does not match with current sub doc count[%ld]", fieldName.c_str(),
               fieldValue.c_str(), subDocuments.size());
    }

    for (size_t i = 0; i < subFieldValues.size(); i++) {
        if (i >= subDocuments.size()) {
            RawDocumentPtr subDoc(new RawDocument);
            subDocuments.push_back(subDoc);
        }
        subDocuments[i]->SetField(fieldName, subFieldValues[i]);
    }
}
}} // namespace indexlib::test
