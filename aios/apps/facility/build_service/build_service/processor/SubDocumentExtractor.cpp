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
#include "build_service/processor/SubDocumentExtractor.h"

#include <algorithm>
#include <assert.h>
#include <ext/alloc_traits.h>
#include <memory>
#include <ostream>

#include "alog/Logger.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "build_service/document/DocumentDefine.h"
#include "indexlib/base/Constant.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/indexlib.h"

using namespace build_service::document;

namespace build_service { namespace processor {
namespace {
using autil::StringTokenizer;

void SortAndUnique(std::vector<autil::StringView>* modifiedFieldsVec)
{
    std::sort(modifiedFieldsVec->begin(), modifiedFieldsVec->end());
    modifiedFieldsVec->erase(std::unique(modifiedFieldsVec->begin(), modifiedFieldsVec->end()),
                             modifiedFieldsVec->end());
}
} // namespace

BS_LOG_SETUP(processor, SubDocumentExtractor);

SubDocumentExtractor::SubDocumentExtractor(const indexlib::config::IndexPartitionSchemaPtr& schema) : _schema(schema) {}

SubDocumentExtractor::~SubDocumentExtractor() {}

// If HA_RESERVED_SUB_MODIFY_FIELDS is set, it needs to be consistent with HA_RESERVED_MODIFY_VALUES.
// TODO(panghai.hj): Currently if HA_RESERVED_SUB_MODIFY_FIELDS is set, modified values is not supported and old
// behavior is kept.

// ModifiedValues is added a few years after HA_RESERVED_SUB_MODIFY_FIELDS. At the time HA_RESERVED_SUB_MODIFY_FIELDS is
// addded, only attribute fields update were taken into consideration, and there was no need to keep track of the old
// values. However, in order to support updating index fields, we need both the old and new values of index fields and
// do diff based on the two to generate diff tokens. Thus we introduced the HA_RESERVED_MODIFY_VALUES for the
// upstream to provide old values of fields to be updated.

// If HA_RESERVED_SUB_MODIFY_FIELDS is not set, only HA_RESERVED_MODIFY_FIELDS and HA_RESERVED_MODIFY_VALUES are set,
// both index and attribute update should work.

// If HA_RESERVED_MODIFY_VALUES is not set, HA_RESERVED_MODIFY_FIELDS and HA_RESERVED_SUB_MODIFY_FIELDS are set,
// attribute update should work.
// TODO(panghai.hj): Remove HA_RESERVED_SUB_MODIFY_FIELDS after verifying that no one uses it.
// TODO(panghai.hj): Figure out a more general approach to represent modified fields and values, e.g. proto etc.

// Expected format:
// modified fields: f1;f2;subf1;subf2
// modified values:
// old_f1_value;old_f2_value;old_subf1_doc1_value^Cold_subf1_doc2_value^Cold_subf1_doc3_value;old_subf2_doc1_value^Cold_subf2_doc2_value^Cold_subf2_doc3_value;...

// sub modified fields number of sub docs per field can be smaller than actual total number of sub docs. That's why it's
// not straight forward to support it with modified values.
// TODO(panghai.hj): Fix this.
void SubDocumentExtractor::extractSubDocuments(const document::RawDocumentPtr& originalDocument,
                                               std::vector<document::RawDocumentPtr>* subDocuments)
{
    const indexlib::config::IndexPartitionSchemaPtr& subSchema = _schema->GetSubIndexPartitionSchema();
    assert(subSchema);

    std::vector<autil::StringView> subFieldsVec;
    const auto& fieldConfigs = subSchema->GetFieldConfigs();
    for (const auto& fieldConfig : fieldConfigs) {
        const std::string& fieldName = fieldConfig->GetFieldName();
        autil::StringView fieldValue = originalDocument->getField(autil::StringView(fieldName));
        if (fieldValue.empty()) {
            continue;
        }
        subFieldsVec.push_back(autil::StringView(fieldName));
        addFieldsToSubDocuments(autil::StringView(fieldName), fieldValue, originalDocument, subDocuments);
    }
    SortAndUnique(&subFieldsVec);

    // transfer HA_RESERVED_SUB_MODIFY_FIELDS to sub docs
    autil::StringView subModifiedFields =
        originalDocument->getField(autil::StringView(document::HA_RESERVED_SUB_MODIFY_FIELDS));
    // Set modified fields for sub docs
    if (!subModifiedFields.empty()) {
        // Currently if HA_RESERVED_SUB_MODIFY_FIELDS is set, modified values will not take effect.
        // Remove this special condition until e.g. update to attribute etc is fixed.
        // This is necessary now because modified fields might have different number of fields than modified values,
        // causing diff phase to confuse.
        // TODO(panghai.hj): Verify that HA_RESERVED_MODIFY_FIELDS for sub docs still work correctly when generated
        // instead of using sub modified fields as indicator. Or make HA_RESERVED_SUB_MODIFY_FIELDS's format
        // consistent with HA_RESERVED_MODIFY_VALUES. i.e. All sub docs' modify fields and values are occupied, no skip.
        // The skip logic will be done in update field or update index, so that if old and new values are the same, no
        // update will be performed.
        addFieldsToSubDocuments(autil::StringView(document::HA_RESERVED_MODIFY_FIELDS), subModifiedFields,
                                originalDocument, subDocuments);
        const std::string& cmd = originalDocument->getField(CMD_TAG);
        const std::string& timeStamp = originalDocument->getField(HA_RESERVED_TIMESTAMP);
        for (size_t i = 0; i < subDocuments->size(); i++) {
            (*subDocuments)[i]->setField(CMD_TAG, cmd);
            (*subDocuments)[i]->setField(HA_RESERVED_TIMESTAMP, timeStamp);
        }
        return;
    }

    auto [modifiedFieldsVec, modifiedValuesVec] = getModifiedFieldsAndValues(originalDocument);
    // If sub doc modified field has sub doc's pk field, we cannot update the sub doc. This is because when sub doc pk
    // changes, we need to do ADD in order to make sub doc stay in its correct form. This may not be necessary in the
    // future if sub doc's format change.
    indexlib::config::IndexSchemaPtr subIndexSchema = subSchema->GetIndexSchema();
    bool setModifiedFieldsAndValues = false;
    if (subIndexSchema) {
        setModifiedFieldsAndValues =
            std::find(modifiedFieldsVec.begin(), modifiedFieldsVec.end(),
                      subIndexSchema->GetPrimaryKeyIndexFieldName()) == modifiedFieldsVec.end();
    }
    if (setModifiedFieldsAndValues) {
        std::vector<size_t> subModifiedFieldsIndices;
        getSubModifiedFields(modifiedFieldsVec, subFieldsVec, &subModifiedFieldsIndices);
        setModifiedFieldsForSubDocs(modifiedFieldsVec, subModifiedFieldsIndices, subDocuments);
        setModifiedValuesForSubDocs(originalDocument, modifiedFieldsVec, modifiedValuesVec, subModifiedFieldsIndices,
                                    subDocuments);
    }

    const std::string& cmd = originalDocument->getField(CMD_TAG);
    const std::string& timeStamp = originalDocument->getField(HA_RESERVED_TIMESTAMP);
    for (size_t i = 0; i < subDocuments->size(); i++) {
        (*subDocuments)[i]->setField(CMD_TAG, cmd);
        (*subDocuments)[i]->setField(HA_RESERVED_TIMESTAMP, timeStamp);
    }
}

std::pair<std::vector<autil::StringView>, std::vector<autil::StringView>>
SubDocumentExtractor::getModifiedFieldsAndValues(const document::RawDocumentPtr& originalDocument)
{
    autil::StringView modifiedFields =
        originalDocument->getField(autil::StringView(document::HA_RESERVED_MODIFY_FIELDS));
    autil::StringView modifiedValues =
        originalDocument->getField(autil::StringView(document::HA_RESERVED_MODIFY_VALUES));
    const std::vector<autil::StringView> modifiedFieldsVec =
        StringTokenizer::constTokenize(modifiedFields, document::HA_RESERVED_MODIFY_FIELDS_SEPARATOR,
                                       StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    std::vector<autil::StringView> modifiedValuesVec;
    if (!modifiedValues.empty()) {
        if (modifiedValues.data()[0] != document::HA_RESERVED_MODIFY_VALUES_MAGIC_HEADER) {
            BS_LOG(ERROR, "ha_reserved_modify_values should start with HA_RESERVED_MODIFY_VALUES_MAGIC_HEADER");
        } else {
            modifiedValues = modifiedValues.substr(sizeof(document::HA_RESERVED_MODIFY_VALUES_MAGIC_HEADER));
            modifiedValuesVec = StringTokenizer::constTokenize(
                modifiedValues, document::HA_RESERVED_MODIFY_VALUES_SEPARATOR, StringTokenizer::TOKEN_TRIM);
        }
    }
    assert(modifiedValuesVec.empty() or modifiedFieldsVec.size() == modifiedValuesVec.size());
    return {modifiedFieldsVec, modifiedValuesVec};
}

void SubDocumentExtractor::setModifiedFieldsForSubDocs(const std::vector<autil::StringView>& modifiedFieldsVec,
                                                       const std::vector<size_t>& subModifiedFieldsIndices,
                                                       std::vector<document::RawDocumentPtr>* subDocuments)
{
    std::string newModifiedFieldsStringForSubDoc;
    for (size_t i = 0; i < subModifiedFieldsIndices.size(); ++i) {
        const size_t indice = subModifiedFieldsIndices[i];
        autil::StringView fieldName = modifiedFieldsVec[indice];
        if (i != 0) {
            newModifiedFieldsStringForSubDoc += document::HA_RESERVED_MODIFY_FIELDS_SEPARATOR;
        }
        newModifiedFieldsStringForSubDoc += fieldName.to_string();
    }
    if (newModifiedFieldsStringForSubDoc.empty()) {
        return;
    }
    // assumes all sub docs have same modify_field field.
    for (size_t i = 0; i < subDocuments->size(); i++) {
        (*subDocuments)[i]->setField(HA_RESERVED_MODIFY_FIELDS, newModifiedFieldsStringForSubDoc);
    }
}

// modified values: sub1_doc1^sub1_doc2;sub2_doc1^sub2^doc2
// after this function:
// sub doc 1's modified value: sub1_doc1;sub2_doc1
// sub doc 2's modified value: sub1_doc2;sub2_doc2
// If the data is invalid or fails, no modified fields will be extrated for sub docs, so that the doc can be treated as
// a normal ADD doc later and avoid invalid partial update.
void SubDocumentExtractor::setModifiedValuesForSubDocs(const document::RawDocumentPtr& originalDocument,
                                                       const std::vector<autil::StringView>& modifiedFieldsVec,
                                                       const std::vector<autil::StringView>& modifiedValuesVec,
                                                       const std::vector<size_t>& subModifiedFieldsIndices,
                                                       std::vector<document::RawDocumentPtr>* subDocuments)
{
    if (modifiedValuesVec.empty() or subModifiedFieldsIndices.empty()) {
        return;
    }
    const size_t numberOfSubDocs = subDocuments->size();
    const size_t numberOfFields = subModifiedFieldsIndices.size();
    std::vector<std::vector<std::string>> fieldForSubDocs(numberOfFields);
    for (size_t i = 0; i < numberOfFields; ++i) {
        const size_t indice = subModifiedFieldsIndices[i];
        const auto& modifiedField = modifiedFieldsVec[indice];
        const auto& modifiedValue = modifiedValuesVec[indice];
        fieldForSubDocs[i] = autil::StringUtil::split(modifiedValue.to_string(), std::string(1, SUB_DOC_SEPARATOR),
                                                      /*ignoreEmpty=*/false);
        if (fieldForSubDocs[i].size() != numberOfSubDocs) {
            std::stringstream ss;
            ss << "Actual number of sub docs[" << numberOfSubDocs << "], number of splits[" << fieldForSubDocs[i].size()
               << " not equal. modified value[" << modifiedValue << "] splits, modified field[" << modifiedField << "]";
            std::string errorMsg = ss.str();
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return;
        }
    }
    std::vector<std::string> modifiedValuesForSubDocs(numberOfSubDocs);
    for (size_t i = 0; i < numberOfFields; ++i) {
        const size_t indice = subModifiedFieldsIndices[i];
        const auto& modifiedField = modifiedFieldsVec[indice];
        for (size_t j = 0; j < numberOfSubDocs; ++j) {
            if (i != 0) {
                modifiedValuesForSubDocs[j] += document::HA_RESERVED_MODIFY_VALUES_SEPARATOR;
            } else {
                modifiedValuesForSubDocs[j] = document::HA_RESERVED_MODIFY_VALUES_MAGIC_HEADER;
            }
            modifiedValuesForSubDocs[j] += fieldForSubDocs[i][j];
            // Set last_value for sub docs
            (*subDocuments)[j]->setField(LAST_VALUE_PREFIX + modifiedField.to_string(), fieldForSubDocs[i][j]);
        }
    }
    addFieldsToSubDocuments(autil::StringView(HA_RESERVED_MODIFY_VALUES), modifiedValuesForSubDocs, originalDocument,
                            subDocuments);
}

// Return true if the schema, modified fields, sub modified fields and modified values are consistent. Also set
// subModifiedFields string and populate subModifiedFieldsIndices.
void SubDocumentExtractor::getSubModifiedFields(const std::vector<autil::StringView>& modifiedFieldsVec,
                                                const std::vector<autil::StringView>& subFieldsVec,
                                                std::vector<size_t>* subModifiedFieldsIndices)
{
    if (modifiedFieldsVec.empty()) {
        return;
    }
    for (size_t i = 0; i < subFieldsVec.size(); i++) {
        auto it = std::find(modifiedFieldsVec.begin(), modifiedFieldsVec.end(), subFieldsVec[i]);
        if (it != modifiedFieldsVec.end()) {
            subModifiedFieldsIndices->push_back(it - modifiedFieldsVec.begin());
        }
    }
    return;
}

// bool SubDocumentExtractor::verifySubModifiedFieldsAndValues(const std::vector<std::string>& subModifiedFieldsVec,
//                                                           const std::vector<std::string> & modifiedValuesVec,
//                                                           const std::vector<size_t> &subModifiedFieldsIndices) {
// if (!modifiedValues.empty() and modifiedFieldsVec.size() != modifiedValuesVec.size()) {
//         logErrorData(modifiedFields, modifiedValues, subModifiedFields);
//         return;
// }
//     if(modifiedValuesVec.empty()) {
//         return true;
//     }
//     // // Check that the modifiedValuesVec are consistent, each modifiedValue has equal number of sub docs.
//     // int64 numSubDocsFromModifiedValues = -1;
//     // for(size_t i=0;i<modifiedValuesVec.size();++i){
//     //     const std::vector<std::string>& subDocs =
//     //         autil::StringUtil::split(modifiedValuesVec[i], std::string(1, SUB_DOC_SEPARATOR),
//     /*ignoreEmpty=*/false);
//     //     if(numSubDocsFromModifiedValues>=0 && numSubDocsFromModifiedValues!= subDocs.size()){
//     //         BS_LOG(ERROR, "modifiedValuesVec not consistent.");
//     //         return false;
//     //     }else{
//     //         numSubDocsFromModifiedValues = subDocs.size();
//     //     }
//     // }
//     // for(size_t i=0;i<subModifiedFieldsVec.size();++i) {
//     //     const std::string& subModifiedFieldString = subModifiedFieldsVec[i];
//     //     if(!subModifiedFieldString.empty()) {
//     //         const std::vector<std::string>& oneFieldSubDocs =
//     //             autil::StringUtil::split(subModifiedFieldString, std::string(1, SUB_DOC_SEPARATOR),
//     /*ignoreEmpty=*/false);
//     //         if(oneFieldSubDocs.size() >
//     //     }

//     //     // split.
//     // }
//     return true;
// }

void SubDocumentExtractor::addFieldsToSubDocuments(autil::StringView fieldName, autil::StringView fieldValue,
                                                   const document::RawDocumentPtr& originalDocument,
                                                   std::vector<document::RawDocumentPtr>* subDocuments)
{
    if (fieldValue.empty()) {
        return;
    }

    std::vector<std::string> subFieldValues =
        autil::StringUtil::split(fieldValue.to_string(), std::string(1, SUB_DOC_SEPARATOR), /*ignoreEmpty=*/false);
    addFieldsToSubDocuments(fieldName, subFieldValues, originalDocument, subDocuments);
}

void SubDocumentExtractor::addFieldsToSubDocuments(autil::StringView fieldName,
                                                   const std::vector<std::string>& fieldValues,
                                                   const document::RawDocumentPtr& originalDocument,
                                                   std::vector<document::RawDocumentPtr>* subDocuments)
{
    if (fieldValues.empty()) {
        return;
    }

    if (subDocuments->size() != 0 && fieldValues.size() != subDocuments->size()) {
        std::stringstream ss;
        ss << "field[" << fieldName << "]:value size[" << fieldValues.size()
           << "] does not match with current sub doc count[" << subDocuments->size() << "]";
        std::string errorMsg = ss.str();
        BS_LOG(WARN, "%s", errorMsg.c_str());
    }
    for (size_t i = 0; i < fieldValues.size(); i++) {
        if (i >= subDocuments->size()) {
            document::RawDocumentPtr subDoc(originalDocument->createNewDocument());
            subDocuments->push_back(subDoc);
        }
        // TODO(panghai.hj): Maybe set null if fieldValues[i] is empty
        (*subDocuments)[i]->setField(fieldName, autil::StringView(fieldValues[i]));
    }
}
}} // namespace build_service::processor
