#include <autil/StringTokenizer.h>
#include <indexlib/config/index_partition_schema.h>
#include "build_service/processor/ModifiedFieldsDocumentProcessor.h"
#include "build_service/processor/ModifiedFieldsModifierCreator.h"
#include "build_service/document/DocumentDefine.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/util/Monitor.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
using namespace build_service::document;
using namespace build_service::config;

namespace build_service {
namespace processor {
BS_LOG_SETUP(processor, ModifiedFieldsDocumentProcessor);

const string ModifiedFieldsDocumentProcessor::PROCESSOR_NAME = MODIFIED_FIELDS_DOCUMENT_PROCESSOR_NAME;
const string ModifiedFieldsDocumentProcessor::DERIVATIVE_RELATIONSHIP =
    string("derivative_relationship");
const string ModifiedFieldsDocumentProcessor::IGNORE_FIELDS =
    string("ignore_fields");
const string ModifiedFieldsDocumentProcessor::IGNORE_FIELDS_SEP = string(",");
const string ModifiedFieldsDocumentProcessor::DERIVATIVE_FAMILY_SEP = string(";");
const string ModifiedFieldsDocumentProcessor::DERIVATIVE_PATERNITY_SEP = string(":");
const string ModifiedFieldsDocumentProcessor::DERIVATIVE_BROTHER_SEP = string(",");

ModifiedFieldsDocumentProcessor::ModifiedFieldsDocumentProcessor()
{
}

ModifiedFieldsDocumentProcessor::~ModifiedFieldsDocumentProcessor()
{
}

bool ModifiedFieldsDocumentProcessor::init(const DocProcessorInitParam &param)
{
    _schema = param.schemaPtr;
    _subSchema = _schema->GetSubIndexPartitionSchema();
    return parseParamRelationship(param.parameters)
        && parseParamIgnoreFields(param.parameters);
}

bool ModifiedFieldsDocumentProcessor::isAddDoc(const ExtendDocumentPtr &document)
{
    RawDocumentPtr rawDoc = document->getRawDocument();
    const ConstString &modifyFields =
        rawDoc->getField(ConstString(HA_RESERVED_MODIFY_FIELDS));
    vector<string> st = StringTokenizer::tokenize(
            modifyFields, HA_RESERVED_MODIFY_FIELDS_SEPARATOR,
            StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    if (0 == st.size()) {
        return true;
    }

    return false;
}

void ModifiedFieldsDocumentProcessor::doInitModifiedFieldSet(
        const IndexPartitionSchemaPtr &schema,
        const ExtendDocumentPtr &document,
        FieldIdSet &unknownFieldIdSet)
{
    const RawDocumentPtr &rawDoc = document->getRawDocument();
    const IndexlibExtendDocumentPtr &indexExtendDoc = document->getIndexExtendDoc();

    const ConstString &modifyFields =
        rawDoc->getField(ConstString(HA_RESERVED_MODIFY_FIELDS));
    vector<string> st = StringTokenizer::tokenize(
            modifyFields, HA_RESERVED_MODIFY_FIELDS_SEPARATOR,
            StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    if (0 == st.size()) {
        return;
    }

    const FieldSchemaPtr &fieldSchema = schema->GetFieldSchema();
    const IndexPartitionSchemaPtr &subSchema = schema->GetSubIndexPartitionSchema();
    FieldSchemaPtr subFieldSchema;
    if (subSchema) {
        subFieldSchema = subSchema->GetFieldSchema();
    }

    for (size_t i = 0; i < st.size(); ++i)
    {
        const string &fieldName = st[i];
        fieldid_t fid = fieldSchema->GetFieldId(fieldName);
        if (fid != INVALID_FIELDID) {
            indexExtendDoc->addModifiedField(fid);
            continue;
        }
        if (subFieldSchema) {
            fid = subFieldSchema->GetFieldId(fieldName);
            if (fid != INVALID_FIELDID) {
                indexExtendDoc->addSubModifiedField(fid);
                continue;
            }
        }
        FieldIdMap::const_iterator it = _unknownFieldIdMap.find(fieldName);
        if (it != _unknownFieldIdMap.end()) {
            unknownFieldIdSet.insert(it->second);
        }
    }
}

void ModifiedFieldsDocumentProcessor::initModifiedFieldSet(
        const ExtendDocumentPtr &document, FieldIdSet &unknownFieldIdSet)
{
    doInitModifiedFieldSet(_schema, document, unknownFieldIdSet);

    if (_subSchema) {
        for (size_t i = 0; i < document->getSubDocumentsCount(); ++i) {
            const ExtendDocumentPtr &subDocument = document->getSubDocument(i);
            FieldIdSet tempFieldIdSet;
            doInitModifiedFieldSet(_subSchema, subDocument, tempFieldIdSet);
        }
    }
}

bool ModifiedFieldsDocumentProcessor::checkModifiedFieldSet(
        const ExtendDocumentPtr &document) const
{
    const IndexlibExtendDocumentPtr& indexExtendDoc = document->getIndexExtendDoc();
    const FieldIdSet &subModifiedFieldSet = indexExtendDoc->getSubModifiedFieldSet();

    const ExtendDocumentVec &subDocs = document->getAllSubDocuments();
    if (subDocs.size() == 0) {
        return subModifiedFieldSet.size() == 0;
    }

    for (FieldIdSet::const_iterator it = subModifiedFieldSet.begin();
         it != subModifiedFieldSet.end(); ++it)
    {
        size_t idx = 0;
        for (; idx < subDocs.size(); ++idx) {
            const IndexlibExtendDocumentPtr &subExtDoc = subDocs[idx]->getIndexExtendDoc();
            if (subExtDoc->hasFieldInModifiedFieldSet(*it)) {
                break;
            }
        }
        if (idx >= subDocs.size()) {
            BS_LOG(DEBUG, "field [%d] only in subModifiedFieldSet", *it);
            ERROR_COLLECTOR_LOG(WARN, "field [%d] only in subModifiedFieldSet", *it);
            return false;
        }
    }
    
    
    for (size_t i = 0; i < subDocs.size(); ++i) {
        const IndexlibExtendDocumentPtr &subExtDoc = subDocs[i]->getIndexExtendDoc();
        const FieldIdSet &subSet = subExtDoc->getModifiedFieldSet();
        for (FieldIdSet::const_iterator it = subSet.begin();
             it != subSet.end(); ++it)
        {
            if (!indexExtendDoc->hasFieldInSubModifiedFieldSet(*it)) {
                stringstream ss;
                ss << "field [" << *it << "] only in sub docs";
                string errorMsg = ss.str();
                BS_LOG(WARN, "%s", errorMsg.c_str());
                return false;
            }
        }
    }

    return true;
}

void ModifiedFieldsDocumentProcessor::batchProcess(const vector<ExtendDocumentPtr> &docs) {
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

bool ModifiedFieldsDocumentProcessor::process(const ExtendDocumentPtr &document)
{
    assert(document->getRawDocument()->getDocOperateType() == ADD_DOC);
    if (isAddDoc(document))
    {
        return true;
    }
    FieldIdSet unknownFieldIdSet;
    initModifiedFieldSet(document, unknownFieldIdSet);
    if (!checkModifiedFieldSet(document)) {
        clearFieldId(document);
        BS_LOG(DEBUG, "check modified fields failed.");
        return true;
    }

    for (size_t i = 0; i < _ModifiedFieldsModifierVec.size(); ++i) {
        _ModifiedFieldsModifierVec[i]->process(document, unknownFieldIdSet);
    }

    const IndexlibExtendDocumentPtr &extDoc = document->getIndexExtendDoc();
    const RawDocumentPtr& rawDoc = document->getRawDocument();
    if (extDoc->isModifiedFieldSetEmpty()) {
        rawDoc->setDocOperateType(SKIP_DOC);
        BS_LOG(TRACE1, "document [%s] skip.",
               document->getRawDocument()->toString().c_str());
        ERROR_COLLECTOR_LOG(WARN, "document [%s] skip.",
               document->getRawDocument()->toString().c_str());
        return true;
    }

    return true;
}

void ModifiedFieldsDocumentProcessor::clearFieldId(
        const document::ExtendDocumentPtr &document) const
{
    const IndexlibExtendDocumentPtr& extDoc = document->getIndexExtendDoc();
    extDoc->clearModifiedFieldSet();

    const ExtendDocumentVec& subDocs = document->getAllSubDocuments();
    for (size_t i = 0; i < subDocs.size(); ++i) {
        const IndexlibExtendDocumentPtr &subExtDoc = subDocs[i]->getIndexExtendDoc();        
        subExtDoc->clearModifiedFieldSet();
    }
}

void ModifiedFieldsDocumentProcessor::destroy() {
    delete this;
}

DocumentProcessor* ModifiedFieldsDocumentProcessor::clone() {
    return new ModifiedFieldsDocumentProcessor(*this);
}

bool ModifiedFieldsDocumentProcessor::parseParamIgnoreFields(
        const KeyValueMap &kvMap)
{
    // "ignore_fields": "commend,"
    const string &ignoreFieldsStr = getValueFromKeyValueMap(kvMap, IGNORE_FIELDS);
    StringTokenizer ignoreFieldsSt(ignoreFieldsStr, IGNORE_FIELDS_SEP,
                                   StringTokenizer::TOKEN_IGNORE_EMPTY
                                   | StringTokenizer::TOKEN_TRIM);
    if (ignoreFieldsSt.getNumTokens() == 0) {
        return true;
    }

    for (StringTokenizer::Iterator ignoreIt = ignoreFieldsSt.begin();
         ignoreIt != ignoreFieldsSt.end(); ++ignoreIt)
    {
        const string &ignoreFieldName = *ignoreIt;
        ModifiedFieldsModifierPtr modifier =
            ModifiedFieldsModifierCreator::createIgnoreModifier(
                ignoreFieldName, _schema);
        if (!modifier)
        {
            string errorMsg = "create ignore rule failed for field[" + ignoreFieldName
                              + "], ignore_fields[" + ignoreFieldsStr + "]";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        _ModifiedFieldsModifierVec.push_back(modifier);
    }
    return true;
}

bool ModifiedFieldsDocumentProcessor::parseRelationshipSinglePaternity(
        const string &paternityStr)
{
    StringTokenizer::Option stOpt =
        StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM;
    StringTokenizer stPaternity(
            paternityStr, DERIVATIVE_PATERNITY_SEP, stOpt);
    if (stPaternity.getNumTokens() != 2) {
        string errorMsg = "derivative_relationship[" + paternityStr + "] parse paternity failed.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    StringTokenizer stBrother(
            stPaternity[1], DERIVATIVE_BROTHER_SEP, stOpt);
    if (0 == stBrother.getNumTokens()) {
        string errorMsg = "derivative_relationship[" + paternityStr + "] parse brother failed.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    StringTokenizer::Iterator it = stBrother.begin();
    for (; it != stBrother.end(); ++it) {
        const string &src = stPaternity[0];
        const string &dst = *it;
        ModifiedFieldsModifierPtr modifier = 
            ModifiedFieldsModifierCreator::createModifiedFieldsModifier(
                src, dst, _schema, _unknownFieldIdMap);
        if (!modifier)
        {
            stringstream ss;
            ss << "create relation modifier failed for rule["
               << src << ":" << dst <<"], relation rules [" << paternityStr << "]";
            string errorMsg = ss.str();
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        _ModifiedFieldsModifierVec.push_back(modifier);
    }
    return true;
}


bool ModifiedFieldsDocumentProcessor::parseParamRelationship(
        const KeyValueMap &kvMap)
{
    // "derivative_relationship" = "f1:f1a,f1b;f2:f2a"
    StringTokenizer::Option stOpt =
        StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM;
    KeyValueMap::const_iterator it = kvMap.find(DERIVATIVE_RELATIONSHIP);
    if (it == kvMap.end()) {
        return true;
    }

    const string &relationShipStr = it->second;
    StringTokenizer stFamily(
            relationShipStr, DERIVATIVE_FAMILY_SEP, stOpt);

    for (StringTokenizer::Iterator fIt = stFamily.begin();
         fIt != stFamily.end(); ++fIt)
    {
        if (!parseRelationshipSinglePaternity(*fIt)) {
            return false;
        }
    }
    return true;
}

//ATTENTION: no sub pk, can not set all sub field ignore,
//           we can not recognize sub doc count change.
}
}
