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
#include "indexlib/partition/raw_document_field_extractor.h"

#include "autil/ConstString.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/index_document/normal_document/search_summary_document.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/source/source_reader.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/partition/index_partition_reader.h"

using namespace std;
using namespace autil;

using namespace indexlib::index;
using namespace indexlib::util;
using namespace indexlib::common;
using namespace indexlib::document;
using namespace indexlib::config;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, RawDocumentFieldExtractor);

RawDocumentFieldExtractor::RawDocumentFieldExtractor(const vector<string>& requiredFields)
{
    if (!requiredFields.empty()) {
        mRequiredFields.insert(requiredFields.begin(), requiredFields.end());
    }
}

RawDocumentFieldExtractor::~RawDocumentFieldExtractor() {}

bool RawDocumentFieldExtractor::Init(const IndexPartitionReaderPtr& indexReader, bool preferSourceIndex)
{
    if (!indexReader) {
        IE_LOG(ERROR, "indexPartitionReader is NULL!");
        return false;
    }

    auto sourceReader = indexReader->GetSourceReader();
    if (sourceReader && preferSourceIndex) {
        return Init(sourceReader, indexReader->GetDeletionMapReader());
    }

    vector<string> fieldNames;
    if (!mRequiredFields.empty()) {
        fieldNames.insert(fieldNames.end(), mRequiredFields.begin(), mRequiredFields.end());
        return Init(indexReader, fieldNames);
    }

    const IndexPartitionSchemaPtr& schema = indexReader->GetSchema();
    assert(schema);
    const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
    const SummarySchemaPtr& summarySchema = schema->GetSummarySchema();
    const auto& fieldConfigs = schema->GetFieldConfigs();
    for (const auto& fieldConfig : fieldConfigs) {
        const string& fieldName = fieldConfig->GetFieldName();
        if (IsFieldInAttribute(attrSchema, fieldName) || IsFieldInSummary(summarySchema, fieldName)) {
            fieldNames.push_back(fieldName);
        }
    }
    return Init(indexReader, fieldNames);
}

bool RawDocumentFieldExtractor::Init(const SourceReaderPtr& sourceReader, const DeletionMapReaderPtr& deletionmapReader)
{
    IE_LOG(INFO, "init from source reader.");
    mSourceReader = sourceReader;
    mDeletionMapReader = deletionmapReader;
    return true;
}

bool RawDocumentFieldExtractor::Init(const IndexPartitionReaderPtr& indexReader, const vector<string>& fieldNames)
{
    IE_LOG(INFO, "init from attribute & summary reader.");
    if (!indexReader) {
        IE_LOG(ERROR, "indexPartitionReader is NULL!");
        return false;
    }

    const IndexPartitionSchemaPtr& schema = indexReader->GetSchema();
    if (!ValidateFields(schema, fieldNames)) {
        IE_LOG(ERROR, "validate required fields failed!");
        return false;
    }
    bool needReadFromSummary = JudgeReadFromSummary(schema, fieldNames);
    if (needReadFromSummary) {
        mSummaryDoc.reset(new SearchSummaryDocument(NULL, schema->GetSummarySchema()->GetSummaryCount()));
        mSummaryReader = indexReader->GetSummaryReader();
        if (!mSummaryReader) {
            IE_LOG(ERROR, "summary reader should not be NULL");
            return false;
        }
    }
    mFieldNames = fieldNames;
    GenerateFieldInfos(schema, indexReader, needReadFromSummary, fieldNames);
    mFieldValues.resize(fieldNames.size());
    mDeletionMapReader = indexReader->GetDeletionMapReader();
    return true;
}

bool RawDocumentFieldExtractor::ValidateFields(const IndexPartitionSchemaPtr& schema,
                                               const vector<string>& fieldNames) const
{
    if (fieldNames.empty()) {
        IE_LOG(ERROR, "no fields required!");
        return false;
    }
    // check pack attribute
    assert(schema);
    const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
    const SummarySchemaPtr& summarySchema = schema->GetSummarySchema();
    for (size_t i = 0; i < fieldNames.size(); ++i) {
        if (IsFieldInAttribute(attrSchema, fieldNames[i])) {
            if (IsFieldInPackAttribute(attrSchema, fieldNames[i])) {
                IE_LOG(ERROR, "does not support in-pack attribute field[%s]", fieldNames[i].c_str());
                return false;
            }
            continue;
        }
        if (!IsFieldInSummary(summarySchema, fieldNames[i])) {
            IE_LOG(ERROR, "field[%s] not in attribute or summary!", fieldNames[i].c_str());
            return false;
        }
    }
    return true;
}

bool RawDocumentFieldExtractor::JudgeReadFromSummary(const IndexPartitionSchemaPtr& schema,
                                                     const vector<string>& fieldNames) const
{
    const SummarySchemaPtr& summarySchema = schema->GetSummarySchema();
    if (!summarySchema) {
        return false;
    }
    const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
    for (size_t i = 0; i < fieldNames.size(); ++i) {
        if (IsFieldInSummary(summarySchema, fieldNames[i]) && !IsFieldInAttribute(attrSchema, fieldNames[i])) {
            return true;
        }
    }
    return false;
}

bool RawDocumentFieldExtractor::IsFieldInAttribute(const AttributeSchemaPtr& attrSchema, const string& fieldName) const
{
    return attrSchema ? attrSchema->IsInAttribute(fieldName) : false;
}

bool RawDocumentFieldExtractor::IsFieldInPackAttribute(const AttributeSchemaPtr& attrSchema,
                                                       const string& fieldName) const
{
    if (!attrSchema) {
        return false;
    }
    const AttributeConfigPtr& attrConfig = attrSchema->GetAttributeConfig(fieldName);
    return attrConfig ? attrConfig->GetPackAttributeConfig() != NULL : false;
}

bool RawDocumentFieldExtractor::IsFieldInSummary(const SummarySchemaPtr& summarySchema, const string& fieldName) const
{
    return summarySchema ? (summarySchema->GetSummaryConfig(fieldName) != NULL) : false;
}

void RawDocumentFieldExtractor::GenerateFieldInfos(const IndexPartitionSchemaPtr& schema,
                                                   const IndexPartitionReaderPtr& indexReader,
                                                   bool preferReadFromSummary, const vector<string>& fieldNames)
{
    const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
    const SummarySchemaPtr& summarySchema = schema->GetSummarySchema();
    for (size_t i = 0; i < fieldNames.size(); ++i) {
        FieldInfo fieldInfo;
        bool isInSummary = IsFieldInSummary(summarySchema, fieldNames[i]);
        bool isInAttribute = IsFieldInAttribute(attrSchema, fieldNames[i]);
        if (isInAttribute) {
            auto attrReader = indexReader->GetAttributeReader(fieldNames[i]);
            attrReader->EnableGlobalReadContext();
        }

        if (isInSummary && isInAttribute && preferReadFromSummary) {
            fieldInfo.summaryFieldId = summarySchema->GetSummaryFieldId(schema->GetFieldId(fieldNames[i]));
            mFieldInfos.push_back(fieldInfo);
            continue;
        }
        if (isInAttribute) {
            fieldInfo.attrReader = indexReader->GetAttributeReader(fieldNames[i]);
        } else {
            fieldInfo.summaryFieldId = summarySchema->GetSummaryFieldId(schema->GetFieldId(fieldNames[i]));
        }
        mFieldInfos.push_back(fieldInfo);
    }
}

SeekStatus RawDocumentFieldExtractor::Seek(docid_t docId)
{
    if (mDeletionMapReader && mDeletionMapReader->IsDeleted(docId)) {
        return SS_DELETED;
    }

    SeekStatus ret = SS_ERROR;
    try {
        ret = FillFieldValues(docId, mFieldValues);
    } catch (const FileIOException& e) {
        IE_LOG(ERROR, "Seek docId[%d] failed. Caught file io exception: %s", docId, e.what());
        return SS_ERROR;
    } catch (const ExceptionBase& e) {
        // IndexCollapsedException, UnSupportedException,
        // BadParameterException, InconsistentStateException, ...
        IE_LOG(ERROR, "Seek docId[%d] failed. Caught exception: %s", docId, e.what());
        return SS_ERROR;
    } catch (const exception& e) {
        IE_LOG(ERROR, "Seek docId[%d] failed. Caught std exception: %s", docId, e.what());
        return SS_ERROR;
    } catch (...) {
        IE_LOG(ERROR, "Seek docId[%d] failed. Caught unknown exception", docId);
        return SS_ERROR;
    }
    return ret;
}

SeekStatus RawDocumentFieldExtractor::FillFieldValues(docid_t docId, vector<string>& fieldValues)
{
    if (mPool.getUsedBytes() > MAX_POOL_MEMORY_USE) {
        mPool.reset();
    }

    if (mSourceReader) {
        // read from source reader
        return FillFieldValuesFromSourceIndex(docId, fieldValues);
    }

    bool readSuccess = false;
    if (mSummaryDoc) {
        mSummaryDoc.reset(new SearchSummaryDocument(NULL, mSummaryDoc->GetFieldCount()));
        assert(mSummaryReader);
        if (!mSummaryReader->GetDocument(docId, mSummaryDoc.get())) {
            IE_LOG(ERROR, "GetDocument docId[%d] from summary reader failed.", docId);
            return SS_ERROR;
        }
    }
    assert(fieldValues.size() == mFieldInfos.size());
    for (size_t i = 0; i < mFieldInfos.size(); ++i) {
        if (mFieldInfos[i].attrReader) {
            readSuccess = mFieldInfos[i].attrReader->Read(docId, fieldValues[i], &mPool);
        } else {
            assert(mSummaryDoc);
            assert(mFieldInfos[i].summaryFieldId != index::INVALID_SUMMARYFIELDID);
            const StringView* fieldValue = mSummaryDoc->GetFieldValue(mFieldInfos[i].summaryFieldId);
            readSuccess = (fieldValue != NULL);
            if (fieldValue) {
                fieldValues[i] = string(fieldValue->data(), fieldValue->size());
            }
        }
        if (!readSuccess) {
            IE_LOG(ERROR, "read field[%s] of docId[%d] failed.", mFieldNames[i].c_str(), docId);
            return SS_ERROR;
        }
    }
    return SS_OK;
}

SeekStatus RawDocumentFieldExtractor::FillFieldValuesFromSourceIndex(docid_t docId, vector<string>& fieldValues)
{
    assert(mSourceReader);
    SourceDocument srcDoc(&mPool);
    if (!mSourceReader->GetDocument(docId, &srcDoc)) {
        return SS_ERROR;
    }

    mFieldNames.clear();
    mFieldValues.clear();
    srcDoc.ExtractFields(mFieldNames, mFieldValues, mRequiredFields);
    return SS_OK;
}

RawDocumentFieldExtractor::FieldIterator RawDocumentFieldExtractor::CreateIterator()
{
    return FieldIterator(mFieldNames, mFieldValues);
}
}} // namespace indexlib::partition
