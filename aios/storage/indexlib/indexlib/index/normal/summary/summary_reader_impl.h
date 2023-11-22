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
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, LocalDiskSummaryReader);

namespace indexlib { namespace index {

// Group manager
class SummaryReaderImpl final : public SummaryReader
{
public:
    SummaryReaderImpl(const config::SummarySchemaPtr& summarySchema);
    ~SummaryReaderImpl();
    DECLARE_SUMMARY_READER_IDENTIFIER(group);

public:
    bool Open(const index_base::PartitionDataPtr& partitionData,
              const std::shared_ptr<PrimaryKeyIndexReader>& pkIndexReader,
              const SummaryReader* hintReader) override final;

    void AddAttrReader(fieldid_t fieldId, const AttributeReaderPtr& attrReader) override final;
    void AddPackAttrReader(fieldid_t fieldId, const PackAttributeReaderPtr& attrReader) override final;

    future_lite::coro::Lazy<index::ErrorCodeVec>
    GetDocument(const std::vector<docid_t>& docIds, autil::mem_pool::Pool* sessionPool, file_system::ReadOption option,
                const SearchSummaryDocVec* docs) const noexcept override final;
    future_lite::coro::Lazy<index::ErrorCodeVec>
    GetDocument(const std::vector<docid_t>& docIds, const SummaryGroupIdVec& groupVec,
                autil::mem_pool::Pool* sessionPool, file_system::ReadOption option,
                const SearchSummaryDocVec* docs) const noexcept override final;

    bool GetDocument(docid_t docId, document::SearchSummaryDocument* summaryDoc) const override final
    {
        return GetDocument(docId, summaryDoc, mAllGroupIds);
    }

    bool GetDocument(docid_t docId, document::SearchSummaryDocument* summaryDoc,
                     const SummaryGroupIdVec& groupVec) const override final;
    bool GetDocumentByPkStr(const std::string& pkStr, document::SearchSummaryDocument* summaryDoc) const override final;

private:
    bool DoGetDocument(docid_t docId, document::SearchSummaryDocument* summaryDoc,
                       const SummaryGroupIdVec& groupVec) const;

private:
    future_lite::coro::Lazy<index::ErrorCodeVec> InnerGetDocumentAsync(const std::vector<docid_t>& docIds,
                                                                       const SummaryGroupIdVec& groupVec,
                                                                       autil::mem_pool::Pool* sessionPool,
                                                                       file_system::ReadOption option,
                                                                       const SearchSummaryDocVec* docs) const noexcept;
    future_lite::coro::Lazy<index::ErrorCodeVec>
    InnerGetDocumentAsyncOrdered(const std::vector<docid_t>& docIds, const SummaryGroupIdVec& groupVec,
                                 autil::mem_pool::Pool* sessionPool, file_system::ReadOption option,
                                 const SearchSummaryDocVec* docs) const noexcept;

private:
    typedef std::vector<LocalDiskSummaryReaderPtr> SummaryGroupVec;
    SummaryGroupVec mSummaryGroups;
    SummaryGroupIdVec mAllGroupIds;
    future_lite::Executor* mExecutor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SummaryReaderImpl);
}} // namespace indexlib::index
