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

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/document/raw_document.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, SourceReader);
DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(index, SummaryReader);
DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, AttributeSchema);
DECLARE_REFERENCE_CLASS(config, SummarySchema);
DECLARE_REFERENCE_CLASS(document, SearchSummaryDocument);

namespace indexlib { namespace partition {

enum SeekStatus { SS_OK = 0, SS_DELETED, SS_ERROR };

class RawDocumentFieldExtractor
{
public:
    class FieldIterator
    {
    public:
        FieldIterator(const std::vector<std::string>& fieldNames, const std::vector<std::string>& fieldValues)
            : names(fieldNames)
            , values(fieldValues)
            , cur(0) {};
        bool HasNext() const { return cur < names.size(); }
        void Next(std::string& name, std::string& value)
        {
            name = names[cur];
            value = values[cur];
            cur++;
        }

        void ToRawDocument(document::RawDocument& rawDoc) const
        {
            assert(names.size() == values.size());
            for (size_t i = 0; i < names.size(); i++) {
                autil::StringView fieldName(names[i]);
                autil::StringView fieldValue(values[i]);
                rawDoc.setField(fieldName, fieldValue);
            }
        }

    private:
        const std::vector<std::string>& names;
        const std::vector<std::string>& values;
        size_t cur;
    };

    struct FieldInfo {
        FieldInfo() : summaryFieldId(index::INVALID_SUMMARYFIELDID) {}
        index::AttributeReaderPtr attrReader;
        index::summaryfieldid_t summaryFieldId;
    };

public:
    RawDocumentFieldExtractor(const std::vector<std::string>& requiredFields = {});
    ~RawDocumentFieldExtractor();

public:
    bool Init(const IndexPartitionReaderPtr& indexReader, bool preferSourceIndex = true);

    SeekStatus Seek(docid_t docId);
    FieldIterator CreateIterator();
    bool IsReadFromSource() const { return mSourceReader != nullptr; }

private:
    bool Init(const index::SourceReaderPtr& sourceReader, const index::DeletionMapReaderPtr& deletionmapReader);

    // read from attribute & summary
    bool Init(const IndexPartitionReaderPtr& indexReader, const std::vector<std::string>& fieldNames);

    bool ValidateFields(const config::IndexPartitionSchemaPtr& schema,
                        const std::vector<std::string>& fieldNames) const;

    bool JudgeReadFromSummary(const config::IndexPartitionSchemaPtr& schema,
                              const std::vector<std::string>& fieldNames) const;

    bool IsFieldInAttribute(const config::AttributeSchemaPtr& attrSchema, const std::string& fieldName) const;

    bool IsFieldInPackAttribute(const config::AttributeSchemaPtr& attrSchema, const std::string& fieldName) const;

    bool IsFieldInSummary(const config::SummarySchemaPtr& summarySchema, const std::string& fieldName) const;

    void GenerateFieldInfos(const config::IndexPartitionSchemaPtr& schema, const IndexPartitionReaderPtr& indexReader,
                            bool preferReadFromSummary, const std::vector<std::string>& fieldNames);

    SeekStatus FillFieldValues(docid_t docId, std::vector<std::string>& fieldValues);

    SeekStatus FillFieldValuesFromSourceIndex(docid_t docId, std::vector<std::string>& fieldValues);

private:
    friend class RawDocumentFieldExtractorTest;
    static const size_t MAX_POOL_MEMORY_USE = 64 * 1024 * 1024; // 64M

private:
    std::vector<std::string> mFieldNames;
    std::vector<FieldInfo> mFieldInfos;
    std::vector<std::string> mFieldValues;
    document::SearchSummaryDocumentPtr mSummaryDoc;
    index::SummaryReaderPtr mSummaryReader;
    index::SourceReaderPtr mSourceReader;
    index::DeletionMapReaderPtr mDeletionMapReader;
    autil::mem_pool::Pool mPool;
    std::set<std::string> mRequiredFields;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RawDocumentFieldExtractor);
}} // namespace indexlib::partition
