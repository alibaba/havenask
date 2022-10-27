#include "build_service/processor/SubDocumentExtractor.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);

using namespace build_service::document;
namespace build_service {
namespace processor {
BS_LOG_SETUP(processor, SubDocumentExtractor);

SubDocumentExtractor::SubDocumentExtractor(
        const IndexPartitionSchemaPtr& schema)
    : _schema(schema)
{
}

SubDocumentExtractor::~SubDocumentExtractor() {
}

void SubDocumentExtractor::extractSubDocuments(
        const RawDocumentPtr& originalDocument,
        vector<RawDocumentPtr>& subDocuments)
{
    const IndexPartitionSchemaPtr& subSchema = _schema->GetSubIndexPartitionSchema();
    assert(subSchema);

    const FieldSchemaPtr& subFieldSchema = subSchema->GetFieldSchema();
    for (FieldSchema::Iterator it = subFieldSchema->Begin();
         it != subFieldSchema->End(); ++it)
    {
        const string &fieldName = (*it)->GetFieldName();
        string fieldValue = originalDocument->getField(fieldName);
        if (fieldValue.empty()) {
            continue;
        }
        addFieldsToSubDocuments(fieldName, fieldValue, subDocuments, originalDocument);
    }

    // transfer HA_RESERVED_SUB_MODIFY_FIELDS to sub docs
    const string &fieldValue =
        originalDocument->getField(HA_RESERVED_SUB_MODIFY_FIELDS);
    addFieldsToSubDocuments(HA_RESERVED_MODIFY_FIELDS, fieldValue, subDocuments, originalDocument);

    const string& cmd = originalDocument->getField(CMD_TAG);
    const string& timeStamp = originalDocument->getField(HA_RESERVED_TIMESTAMP);
    for (size_t i = 0; i < subDocuments.size(); i++) {
        subDocuments[i]->setField(CMD_TAG, cmd);
        subDocuments[i]->setField(HA_RESERVED_TIMESTAMP, timeStamp);
    }
}

void SubDocumentExtractor::addFieldsToSubDocuments(
        const string& fieldName, const string& fieldValue,
        vector<RawDocumentPtr>& subDocuments,
        const RawDocumentPtr& originalDocument)
{
    if (fieldValue.empty())
    {
        return;
    }

    const vector<string>& subFieldValues =
        StringUtil::split(fieldValue, string(1, SUB_DOC_SEPARATOR), false);

    if (subDocuments.size() != 0 && subFieldValues.size() != subDocuments.size())
    {
        stringstream ss;
        ss << "field[" << fieldName 
           << "]:value[" << fieldValue 
           << "] does not match with current sub doc count[%" << subDocuments.size() << "]";
        string errorMsg = ss.str();
        BS_LOG(WARN, "%s", errorMsg.c_str());
    }
    for (size_t i = 0; i < subFieldValues.size(); i++) {
        if (i >= subDocuments.size()) {
            RawDocumentPtr subDoc(originalDocument->createNewDocument());
            subDocuments.push_back(subDoc);
        }
        subDocuments[i]->setField(fieldName, subFieldValues[i]);
    }
}

}
}
