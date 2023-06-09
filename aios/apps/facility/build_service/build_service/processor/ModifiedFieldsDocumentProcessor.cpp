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
#include "build_service/processor/ModifiedFieldsDocumentProcessor.h"

#include "autil/StringTokenizer.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/document/DocumentDefine.h"
#include "build_service/processor/ModifiedFieldsModifierCreator.h"
#include "build_service/util/Monitor.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/legacy_schema_adapter.h"

namespace build_service { namespace processor {
namespace {
using autil::StringTokenizer;
using autil::StringView;
} // namespace

BS_LOG_SETUP(processor, ModifiedFieldsDocumentProcessor);

const std::string ModifiedFieldsDocumentProcessor::PROCESSOR_NAME = config::MODIFIED_FIELDS_DOCUMENT_PROCESSOR_NAME;
const std::string ModifiedFieldsDocumentProcessor::DERIVATIVE_RELATIONSHIP = std::string("derivative_relationship");
const std::string ModifiedFieldsDocumentProcessor::IGNORE_FIELDS = std::string("ignore_fields");

const std::string ModifiedFieldsDocumentProcessor::REWRITE_MODIFY_FIELDS_BY_SCHEMA =
    std::string("rewrite_modify_fields_by_schema");

const std::string ModifiedFieldsDocumentProcessor::IGNORE_FIELDS_SEP = std::string(",");
const std::string ModifiedFieldsDocumentProcessor::DERIVATIVE_FAMILY_SEP = std::string(";");
const std::string ModifiedFieldsDocumentProcessor::DERIVATIVE_PATERNITY_SEP = std::string(":");
const std::string ModifiedFieldsDocumentProcessor::DERIVATIVE_BROTHER_SEP = std::string(",");

ModifiedFieldsDocumentProcessor::ModifiedFieldsDocumentProcessor() : _needRewriteModifyFields(false) {}

ModifiedFieldsDocumentProcessor::~ModifiedFieldsDocumentProcessor() {}

bool ModifiedFieldsDocumentProcessor::init(const DocProcessorInitParam& param)
{
    _schema = param.schema;

    auto legacySchemaAdapter = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(param.schema);
    if (legacySchemaAdapter) {
        _subSchema = legacySchemaAdapter->GetLegacySchema()->GetSubIndexPartitionSchema();
    }

    auto iter = param.parameters.find(REWRITE_MODIFY_FIELDS_BY_SCHEMA);
    if (iter != param.parameters.end() && iter->second == "true") {
        _needRewriteModifyFields = true;
    }

    if (_needRewriteModifyFields && param.metricProvider) {
        _rewriteModifyFieldQps =
            DECLARE_METRIC(param.metricProvider, "basic/rewriteModifyFieldQps", kmonitor::QPS, "count");
    }
    return parseParamRelationship(param.parameters) && parseParamIgnoreFields(param.parameters);
}

// Add field from schema to main and sub modified field which are saved in document. The field has to at least exist in
// main schema.
template <typename SchemaType, typename SubSchemaType>
void ModifiedFieldsDocumentProcessor::doInitModifiedFieldSet(const std::shared_ptr<SchemaType>& schema,
                                                             const std::shared_ptr<SubSchemaType>& subSchema,
                                                             const document::ExtendDocumentPtr& document,
                                                             FieldIdSet* unknownFieldIdSet)
{
    const document::RawDocumentPtr& rawDoc = document->getRawDocument();
    const StringView& modifiedFields = rawDoc->getField(StringView(document::HA_RESERVED_MODIFY_FIELDS));
    std::vector<std::string> modifiedFieldsVec = StringTokenizer::tokenize(
        modifiedFields, document::HA_RESERVED_MODIFY_FIELDS_SEPARATOR, StringTokenizer::TOKEN_TRIM);
    // If the doc does not have modified fields, it's a pure add doc.
    if (0 == modifiedFieldsVec.size()) {
        return;
    }

    indexlib::document::IndexlibExtendDocumentPtr indexExtendDoc = document->getLegacyExtendDoc();
    for (size_t i = 0; i < modifiedFieldsVec.size(); ++i) {
        const std::string& fieldName = modifiedFieldsVec[i];
        fieldid_t fid = schema->GetFieldId(fieldName);
        if (fid != INVALID_FIELDID) {
            document->addModifiedField(fid);
            continue;
        }
        if (subSchema) {
            fid = subSchema->GetFieldId(fieldName);
            if (fid != INVALID_FIELDID) {
                assert(indexExtendDoc);
                indexExtendDoc->addSubModifiedField(fid);
                continue;
            }
        }
        FieldIdMap::const_iterator it = _unknownFieldIdMap.find(fieldName);
        if (it != _unknownFieldIdMap.end()) {
            if (unknownFieldIdSet != nullptr) {
                unknownFieldIdSet->insert(it->second);
            }
        }
    }
}

void ModifiedFieldsDocumentProcessor::initModifiedFieldSet(const document::ExtendDocumentPtr& document,
                                                           FieldIdSet* unknownFieldIdSet)
{
    doInitModifiedFieldSet(_schema, _subSchema, document, unknownFieldIdSet);

    if (_subSchema) {
        for (size_t i = 0; i < document->getSubDocumentsCount(); ++i) {
            const document::ExtendDocumentPtr& subDocument = document->getSubDocument(i);
            doInitModifiedFieldSet(_subSchema, indexlib::config::IndexPartitionSchemaPtr(), subDocument,
                                   /*unknownFieldIdSet=*/nullptr);
        }
    }
}

// If there is sub doc, make sure sub doc's modified fields(set A) and values are also included in main doc(set B).
bool ModifiedFieldsDocumentProcessor::checkModifiedFieldSet(const document::ExtendDocumentPtr& document) const
{
    indexlib::document::IndexlibExtendDocumentPtr indexExtendDoc = document->getLegacyExtendDoc();
    if (!indexExtendDoc) {
        assert(!_subSchema);
        return true;
    }
    assert(indexExtendDoc);
    const FieldIdSet& subModifiedFieldSet = indexExtendDoc->getSubModifiedFieldSet();

    const ExtendDocumentVec& subDocs = document->getAllSubDocuments();
    if (subDocs.size() == 0) {
        return subModifiedFieldSet.size() == 0;
    }

    // check all fields in subModifiedFieldSet from main doc are actually in at least one of sub doc's modified field
    // set B is contained by set A
    for (FieldIdSet::const_iterator it = subModifiedFieldSet.begin(); it != subModifiedFieldSet.end(); ++it) {
        size_t idx = 0;
        for (; idx < subDocs.size(); ++idx) {
            if (subDocs[idx]->hasFieldInModifiedFieldSet(*it)) {
                break;
            }
        }
        if (idx >= subDocs.size()) {
            BS_LOG(DEBUG, "field [%d] only in subModifiedFieldSet", *it);
            ERROR_COLLECTOR_LOG(WARN, "field [%d] only in subModifiedFieldSet", *it);
            return false;
        }
    }
    // check all sub docs' modified fields are in main doc's subModifiedField
    // set A is contained by set B
    for (size_t i = 0; i < subDocs.size(); ++i) {
        const indexlib::document::IndexlibExtendDocumentPtr& subExtDoc = subDocs[i]->getLegacyExtendDoc();
        assert(subExtDoc);
        const FieldIdSet& subSet = subExtDoc->getModifiedFieldSet();
        for (FieldIdSet::const_iterator it = subSet.begin(); it != subSet.end(); ++it) {
            if (!indexExtendDoc->hasFieldInSubModifiedFieldSet(*it)) {
                std::stringstream ss;
                ss << "field [" << *it << "] only in sub docs";
                std::string errorMsg = ss.str();
                BS_LOG(WARN, "%s", errorMsg.c_str());
                return false;
            }
        }
    }

    return true;
}

void ModifiedFieldsDocumentProcessor::batchProcess(const std::vector<document::ExtendDocumentPtr>& docs)
{
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(document::ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

bool ModifiedFieldsDocumentProcessor::process(const document::ExtendDocumentPtr& document)
{
    assert(document->getRawDocument()->getDocOperateType() == ADD_DOC);

    const document::RawDocumentPtr& rawDoc = document->getRawDocument();
    const StringView& modifiedFields = rawDoc->getField(StringView(document::HA_RESERVED_MODIFY_FIELDS));
    _modifiedFieldsVec = StringTokenizer::tokenize(modifiedFields, document::HA_RESERVED_MODIFY_FIELDS_SEPARATOR,
                                                   StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    if (_needRewriteModifyFields) {
        bool needSkip = false;
        rewriteModifyFields(document, &needSkip);
        if (needSkip) {
            rawDoc->setDocOperateType(SKIP_DOC);
            BS_LOG(TRACE1, "document [%s] skip.", rawDoc->toString().c_str());
            ERROR_COLLECTOR_LOG(WARN, "document [%s] skip.", rawDoc->toString().c_str());
            return true;
        }
    }

    if (_modifiedFieldsVec.empty()) {
        // pure add
        return true;
    }
    FieldIdSet unknownFieldIdSet;
    initModifiedFieldSet(document, &unknownFieldIdSet);
    if (!checkModifiedFieldSet(document)) {
        clearFieldId(document);
        BS_LOG(DEBUG, "check modified fields failed.");
        return true;
    }

    for (size_t i = 0; i < _modifiedFieldsModifierVec.size(); ++i) {
        _modifiedFieldsModifierVec[i]->process(document, unknownFieldIdSet);
    }

    if (document->isModifiedFieldSetEmpty()) {
        rawDoc->setDocOperateType(SKIP_DOC);
        BS_LOG(TRACE1, "document [%s] skip.", rawDoc->toString().c_str());
        ERROR_COLLECTOR_LOG(WARN, "document [%s] skip.", rawDoc->toString().c_str());
        return true;
    }

    // For main doc's fields, copy values from modified values for each field to LAST_VALUE_PREFIX+field.
    // For sub doc's fields, these copy are done in SubDocExtractor.
    // TODO(panghai.hj): _modifiedFields and _subModifiedFields in IndexlibExtendDocument seems redundant. Optimize or
    // test it.
    if (rawDoc->exist(document::HA_RESERVED_MODIFY_VALUES)) {
        StringView modifiedValues = rawDoc->getField(StringView(document::HA_RESERVED_MODIFY_VALUES));
        if (!modifiedValues.empty()) {
            if (modifiedValues.data()[0] != document::HA_RESERVED_MODIFY_VALUES_MAGIC_HEADER) {
                BS_LOG(ERROR, "ha_reserved_modify_values should start with HA_RESERVED_MODIFY_VALUES_MAGIC_HEADER");
                return true;
            }
            modifiedValues = modifiedValues.substr(sizeof(document::HA_RESERVED_MODIFY_VALUES_MAGIC_HEADER));
            std::vector<std::string> modifiedValuesVec = StringTokenizer::tokenize(
                modifiedValues, document::HA_RESERVED_MODIFY_VALUES_SEPARATOR, StringTokenizer::TOKEN_TRIM);
            if (modifiedValuesVec.size() != _modifiedFieldsVec.size()) {
                BS_LOG(TRACE1, "modifiedFields[%s] and modifiedValues[%s] are inconsistent.",
                       std::string(modifiedFields).c_str(), std::string(modifiedValues).c_str());
                return true;
            }
            for (size_t i = 0; i < modifiedValuesVec.size(); ++i) {
                rawDoc->setField(document::LAST_VALUE_PREFIX + _modifiedFieldsVec[i], modifiedValuesVec[i]);
            }
        }
    }
    return true;
}

void ModifiedFieldsDocumentProcessor::clearFieldId(const document::ExtendDocumentPtr& document) const
{
    document->clearModifiedFieldSet();

    const ExtendDocumentVec& subDocs = document->getAllSubDocuments();
    for (size_t i = 0; i < subDocs.size(); ++i) {
        subDocs[i]->clearModifiedFieldSet();
    }
}

void ModifiedFieldsDocumentProcessor::destroy() { delete this; }

DocumentProcessor* ModifiedFieldsDocumentProcessor::clone() { return new ModifiedFieldsDocumentProcessor(*this); }

bool ModifiedFieldsDocumentProcessor::parseParamIgnoreFields(const KeyValueMap& kvMap)
{
    // "ignore_fields": "commend,"
    const std::string& ignoreFieldsStr = getValueFromKeyValueMap(kvMap, IGNORE_FIELDS);
    StringTokenizer ignoreFieldsSt(ignoreFieldsStr, IGNORE_FIELDS_SEP,
                                   StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    if (ignoreFieldsSt.getNumTokens() == 0) {
        return true;
    }

    for (StringTokenizer::Iterator ignoreIt = ignoreFieldsSt.begin(); ignoreIt != ignoreFieldsSt.end(); ++ignoreIt) {
        const std::string& ignoreFieldName = *ignoreIt;
        ModifiedFieldsModifierPtr modifier =
            ModifiedFieldsModifierCreator::createIgnoreModifier(ignoreFieldName, _schema);
        if (!modifier) {
            std::string errorMsg =
                "create ignore rule failed for field[" + ignoreFieldName + "], ignore_fields[" + ignoreFieldsStr + "]";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        _modifiedFieldsModifierVec.push_back(modifier);
    }
    return true;
}

bool ModifiedFieldsDocumentProcessor::parseRelationshipSinglePaternity(const std::string& paternityStr)
{
    StringTokenizer::Option stOpt = StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM;
    StringTokenizer stPaternity(paternityStr, DERIVATIVE_PATERNITY_SEP, stOpt);
    if (stPaternity.getNumTokens() != 2) {
        std::string errorMsg = "derivative_relationship[" + paternityStr + "] parse paternity failed.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    StringTokenizer stBrother(stPaternity[1], DERIVATIVE_BROTHER_SEP, stOpt);
    if (0 == stBrother.getNumTokens()) {
        std::string errorMsg = "derivative_relationship[" + paternityStr + "] parse brother failed.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    StringTokenizer::Iterator it = stBrother.begin();
    for (; it != stBrother.end(); ++it) {
        const std::string& src = stPaternity[0];
        const std::string& dst = *it;
        ModifiedFieldsModifierPtr modifier =
            ModifiedFieldsModifierCreator::createModifiedFieldsModifier(src, dst, _schema, _unknownFieldIdMap);
        if (!modifier) {
            std::stringstream ss;
            ss << "create relation modifier failed for rule[" << src << ":" << dst << "], relation rules ["
               << paternityStr << "]";
            std::string errorMsg = ss.str();
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        _modifiedFieldsModifierVec.push_back(modifier);
    }
    return true;
}

bool ModifiedFieldsDocumentProcessor::parseParamRelationship(const KeyValueMap& kvMap)
{
    // "derivative_relationship" = "f1:f1a,f1b;f2:f2a"
    StringTokenizer::Option stOpt = StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM;
    KeyValueMap::const_iterator it = kvMap.find(DERIVATIVE_RELATIONSHIP);
    if (it == kvMap.end()) {
        return true;
    }

    const std::string& relationShipStr = it->second;
    StringTokenizer stFamily(relationShipStr, DERIVATIVE_FAMILY_SEP, stOpt);

    for (StringTokenizer::Iterator fIt = stFamily.begin(); fIt != stFamily.end(); ++fIt) {
        if (!parseRelationshipSinglePaternity(*fIt)) {
            return false;
        }
    }
    return true;
}

// Rewrite ModifiedFields according to schema, for example if some fields in ModifiedFields are not in schema, those
// fields will be skipped.
void ModifiedFieldsDocumentProcessor::rewriteModifyFields(const document::ExtendDocumentPtr& document,
                                                          bool* needSkipDoc)
{
    *needSkipDoc = false;
    if (0 == _modifiedFieldsVec.size()) {
        return;
    }

    std::vector<std::string> actualModifiedFieldsVec;
    actualModifiedFieldsVec.reserve(_modifiedFieldsVec.size());
    for (size_t i = 0; i < _modifiedFieldsVec.size(); ++i) {
        const std::string& fieldName = _modifiedFieldsVec[i];
        fieldid_t fid = _schema->GetFieldId(fieldName);
        if (fid != INVALID_FIELDID) {
            actualModifiedFieldsVec.push_back(fieldName);
            continue;
        }

        if (_subSchema) {
            fid = _subSchema->GetFieldId(fieldName);
            if (fid != INVALID_FIELDID) {
                actualModifiedFieldsVec.push_back(fieldName);
                continue;
            }
        }
    }

    document::RawDocumentPtr rawDoc = document->getRawDocument();
    if (actualModifiedFieldsVec.size() != _modifiedFieldsVec.size()) {
        if (_rewriteModifyFieldQps) {
            kmonitor::MetricsTags tag("schema_name", _schema->GetTableName());
            if (actualModifiedFieldsVec.empty()) {
                tag.AddTag("modify_field", "NONE");
            } else {
                tag.AddTag("modify_field", "ANY");
            }
            _rewriteModifyFieldQps->Report(&tag, 1.0);
        }
        rawDoc->setField(
            document::HA_RESERVED_MODIFY_FIELDS,
            autil::StringUtil::toString(actualModifiedFieldsVec, document::HA_RESERVED_MODIFY_FIELDS_SEPARATOR));
        if (actualModifiedFieldsVec.empty()) {
            *needSkipDoc = true;
        }
    }
}

// ATTENTION: no sub pk, can not set all sub field ignore,
//           we can not recognize sub doc count change.
}} // namespace build_service::processor
