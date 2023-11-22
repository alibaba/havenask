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
#include "build_service/processor/DeleteSubRawDocRewriter.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <iterator>
#include <memory>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "build_service/document/DocumentDefine.h"
#include "build_service/processor/SubDocumentExtractor.h"
#include "build_service/util/Monitor.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/document/RawDocumentDefine.h"
#include "indexlib/document/raw_document/raw_document_define.h"
#include "indexlib/indexlib.h"

using namespace std;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, DeleteSubRawDocRewriter);

std::vector<document::RawDocumentPtr>
DeleteSubRawDocRewriter::rewrite(const indexlib::config::IndexPartitionSchemaPtr& schema,
                                 const document::RawDocumentPtr& rawDoc, ProcessorMetricReporter* reporter)
{
    assert(rawDoc);
    if (rawDoc->getDocOperateType() != ADD_DOC) {
        return std::vector<document::RawDocumentPtr> {rawDoc};
    }
    indexlib::config::IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    if (schema == nullptr or schema->GetTableType() != indexlib::TableType::tt_index or subSchema == nullptr) {
        return std::vector<document::RawDocumentPtr> {rawDoc};
    }
    indexlib::config::IndexSchemaPtr subIndexSchema = subSchema->GetIndexSchema();
    if (subIndexSchema == nullptr or not subIndexSchema->HasPrimaryKeyIndex()) {
        return std::vector<document::RawDocumentPtr> {rawDoc};
    }

    auto [modifiedFieldsVec, modifiedValuesVec] = SubDocumentExtractor::getModifiedFieldsAndValues(rawDoc);
    if (unlikely(modifiedFieldsVec.size() != modifiedValuesVec.size())) {
        if (modifiedValuesVec.empty()) {
            BS_LOG(WARN, "[%s] invalid doc without modify values, may be legacy data source: [%lu] != [%lu]",
                   subSchema->GetSchemaName().c_str(), modifiedFieldsVec.size(), modifiedValuesVec.size());
        } else {
            BS_LOG(ERROR, "[%s] invalid doc, modify field values illegal: [%lu] != [%lu]",
                   subSchema->GetSchemaName().c_str(), modifiedFieldsVec.size(), modifiedValuesVec.size());
        }
        return {std::vector<document::RawDocumentPtr> {rawDoc}};
    }

    std::string subPkFieldName = subIndexSchema->GetPrimaryKeyIndexFieldName();
    if (unlikely(subPkFieldName.empty())) {
        return std::vector<document::RawDocumentPtr> {rawDoc};
    }
    auto [isSuccess1, oldSubPks] = GetOldSubPks(subPkFieldName, modifiedFieldsVec, modifiedValuesVec);
    if (!isSuccess1) {
        return std::vector<document::RawDocumentPtr> {rawDoc};
    }
    std::set<std::string> oldSubPkSet(oldSubPks.begin(), oldSubPks.end());
    if (unlikely(oldSubPkSet.size() != oldSubPks.size())) {
        BS_LOG(ERROR, "[%s] duplicate sub pk [%s] in doc", schema->GetSchemaName().c_str(),
               autil::StringUtil::toString(oldSubPks).c_str());
        return std::vector<document::RawDocumentPtr> {rawDoc};
    }
    std::set<std::string> newSubPkSet = GetNewSubPks(subPkFieldName, rawDoc);
    if (newSubPkSet.size() >= oldSubPks.size()) {
        return std::vector<document::RawDocumentPtr> {rawDoc};
    }

    std::vector<std::string> deleteSubPks;
    std::vector<std::string> addSubPks;
    std::set_difference(newSubPkSet.begin(), newSubPkSet.end(), oldSubPkSet.begin(), oldSubPkSet.end(),
                        std::inserter(addSubPks, addSubPks.begin()));
    std::set_difference(oldSubPkSet.begin(), oldSubPkSet.end(), newSubPkSet.begin(), newSubPkSet.end(),
                        std::inserter(deleteSubPks, deleteSubPks.begin()));
    if (addSubPks.size() > 0 or deleteSubPks.size() == 0) {
        return std::vector<document::RawDocumentPtr> {rawDoc};
    }

    INCREASE_QPS(reporter ? reporter->_addDocDeleteSubQpsMetric : nullptr);
    auto [isSuccess, docs] =
        splitToDeleteSubAndAddDoc(subSchema, rawDoc, modifiedFieldsVec, modifiedValuesVec, oldSubPks, deleteSubPks);
    if (isSuccess and reporter) {
        INCREASE_QPS(reporter->_rewriteDeleteSubDocQpsMetric);
    }
    return docs;
}

std::pair<bool, std::vector<std::string>>
DeleteSubRawDocRewriter::GetOldSubPks(const std::string& subPkFieldName,
                                      const std::vector<autil::StringView>& modifiedFieldsVec,
                                      const std::vector<autil::StringView>& modifiedValuesVec)
{
    if (modifiedFieldsVec.size() != modifiedValuesVec.size() or modifiedValuesVec.empty()) {
        return {false, {}};
    }
    auto it = std::find(modifiedFieldsVec.begin(), modifiedFieldsVec.end(), autil::StringView(subPkFieldName));
    if (it == modifiedFieldsVec.end()) {
        return {false, {}};
    }
    autil::StringView subPksFieldValue = modifiedValuesVec[it - modifiedFieldsVec.begin()];
    std::vector<std::string> subPks =
        autil::StringUtil::split(subPksFieldValue.to_string(), std::string(1, document::SUB_DOC_SEPARATOR),
                                 /*ignoreEmpty=*/false);
    return {true, subPks};
}

std::set<std::string> DeleteSubRawDocRewriter::GetNewSubPks(const std::string& subPkFieldName,
                                                            const document::RawDocumentPtr& rawDoc)
{
    std::string subPkFieldValue = rawDoc->getField(subPkFieldName);
    std::vector<std::string> subPks =
        autil::StringUtil::split(subPkFieldValue, std::string(1, document::SUB_DOC_SEPARATOR), /*ignoreEmpty=*/false);
    return std::set<std::string>(subPks.begin(), subPks.end());
}

// Split rawDoc and generate potentially two new docs, one delete_sub doc(doc1) and one add doc(doc2).
// delete_sub format allows multiple sub doc pks to be included in one delete_sub doc(doc1).
// The rest of the changes in the orignal rawDoc will be included in the add doc(doc2). If rawDoc contains no other
// changes than deleting sub docs, the add doc(doc2) will be omitted.
std::pair<bool, std::vector<document::RawDocumentPtr>> DeleteSubRawDocRewriter::splitToDeleteSubAndAddDoc(
    const indexlib::config::IndexPartitionSchemaPtr& subSchema, const document::RawDocumentPtr& rawDoc,
    const std::vector<autil::StringView>& modifiedFieldsVec, const std::vector<autil::StringView>& modifiedValuesVec,
    const std::vector<std::string>& oldSubPks, const std::vector<std::string>& deleteSubPks)
{
    assert(!deleteSubPks.empty());
    document::RawDocumentPtr deleteSubDoc(rawDoc->clone());
    deleteSubDoc->setDocOperateType(DELETE_SUB_DOC);
    deleteSubDoc->setField(subSchema->GetIndexSchema()->GetPrimaryKeyIndexFieldName(),
                           autil::StringUtil::toString(deleteSubPks, std::string(1, document::SUB_DOC_SEPARATOR)));
    deleteSubDoc->AddTag(indexlib::document::DOC_FLAG_FOR_METRIC, "rewriteDeleteSubDoc");

    std::set<size_t> deleteSubPkPositions;
    for (const std::string& subPk : deleteSubPks) {
        auto it = std::find(oldSubPks.begin(), oldSubPks.end(), subPk);
        assert(it != oldSubPks.end());
        deleteSubPkPositions.insert(it - oldSubPks.begin());
    }

    std::vector<std::string> validModiFieldsVec;
    std::vector<std::string> newModifyValuesVec;
    for (size_t i = 0; i < modifiedFieldsVec.size(); ++i) {
        std::string fieldName = modifiedFieldsVec[i].to_string();
        std::string oldValuesStr = modifiedValuesVec[i].to_string();
        if (subSchema->GetFieldSchema()->GetFieldId(fieldName) == INVALID_FIELDID) {
            validModiFieldsVec.push_back(fieldName);
            newModifyValuesVec.push_back(oldValuesStr);
            continue;
        }
        std::vector<std::string> oldValues =
            autil::StringUtil::split(oldValuesStr, std::string(1, document::SUB_DOC_SEPARATOR), /*ignoreEmpty=*/false);
        std::vector<std::string> newValues;
        if (unlikely(oldSubPks.size() != oldValues.size())) {
            BS_LOG(ERROR, "[%s] invalid doc, sub doc field [%s] count illegal: [%lu] != [%lu]",
                   subSchema->GetSchemaName().c_str(), fieldName.c_str(), oldSubPks.size(), oldValues.size());
            return {false, std::vector<document::RawDocumentPtr> {rawDoc}};
        }
        for (size_t position = 0; position < oldValues.size(); ++position) {
            if (deleteSubPkPositions.find(position) == deleteSubPkPositions.end()) {
                newValues.push_back(oldValues[position]);
            }
        }
        std::string newModifyValue =
            autil::StringUtil::toString(newValues, std::string(1, document::SUB_DOC_SEPARATOR));
        if (newModifyValue != rawDoc->getField(fieldName)) {
            validModiFieldsVec.push_back(fieldName);
            newModifyValuesVec.push_back(newModifyValue);
        }
    }
    if (validModiFieldsVec.empty()) {
        BS_LOG(INFO,
               "[%s] rewrite doc to delete-sub-doc [%s] only, add doc is skipped because no other change is found.",
               subSchema->GetSchemaName().c_str(), autil::StringUtil::toString(deleteSubPks).c_str());
        return {true, std::vector<document::RawDocumentPtr> {deleteSubDoc}};
    }

    rawDoc->setField(document::HA_RESERVED_MODIFY_FIELDS,
                     autil::StringUtil::toString(validModiFieldsVec, document::HA_RESERVED_MODIFY_FIELDS_SEPARATOR));
    rawDoc->setField(
        document::HA_RESERVED_MODIFY_VALUES,
        std::string(1, document::HA_RESERVED_MODIFY_VALUES_MAGIC_HEADER) +
            autil::StringUtil::toString(newModifyValuesVec, document::HA_RESERVED_MODIFY_VALUES_SEPARATOR));
    rawDoc->AddTag(indexlib::document::DOC_FLAG_FOR_METRIC, "rewriteDeleteSubDoc");
    BS_LOG(INFO, "[%s] rewrite doc to delete-sub-doc [%s] and add-doc", subSchema->GetSchemaName().c_str(),
           autil::StringUtil::toString(deleteSubPks).c_str());
    return {true, std::vector<document::RawDocumentPtr> {deleteSubDoc, rawDoc}};
}

}} // namespace build_service::processor
