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

#include "future_lite/coro/Lazy.h"
#include "indexlib/common_define.h"
#include "indexlib/index/primary_key/PrimaryKeyReader.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"

DECLARE_REFERENCE_CLASS(document, SearchSummaryDocument);
DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, PackAttributeReader);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(config, SummarySchema);

namespace indexlib { namespace index {

#define DECLARE_SUMMARY_READER_IDENTIFIER(id)                                                                          \
    static std::string Identifier() { return std::string("summary.reader." #id); }                                     \
    std::string GetIdentifier() const override { return Identifier(); }

typedef std::vector<document::SearchSummaryDocument*> SearchSummaryDocVec;
typedef std::vector<summarygroupid_t> SummaryGroupIdVec;

class SummaryReader
{
public:
    SummaryReader(const config::SummarySchemaPtr& summarySchema);
    virtual ~SummaryReader();

public:
    virtual bool Open(const index_base::PartitionDataPtr& partitionData,
                      const std::shared_ptr<PrimaryKeyIndexReader>& pkIndexReader,
                      const SummaryReader* hintReader = nullptr)
    {
        assert(false);
        return false;
    }

    virtual future_lite::coro::Lazy<index::ErrorCodeVec> GetDocument(const std::vector<docid_t>& docIds,
                                                                     const SummaryGroupIdVec& groupVec,
                                                                     autil::mem_pool::Pool* sessionPool,
                                                                     file_system::ReadOption option,
                                                                     const SearchSummaryDocVec* docs) const noexcept
    {
        assert(false);
        co_return index::ErrorCodeVec();
    }

    virtual future_lite::coro::Lazy<index::ErrorCodeVec> GetDocument(const std::vector<docid_t>& docIds,
                                                                     autil::mem_pool::Pool* sessionPool,
                                                                     file_system::ReadOption option,
                                                                     const SearchSummaryDocVec* docs) const noexcept
    {
        assert(false);
        co_return index::ErrorCodeVec();
    }
    virtual bool GetDocument(docid_t docId, document::SearchSummaryDocument* summaryDoc) const = 0;
    virtual bool GetDocument(docid_t docId, document::SearchSummaryDocument* summaryDoc,
                             const SummaryGroupIdVec& groupVec) const
    {
        assert(false);
        return false;
    }

    virtual std::string GetIdentifier() const = 0;

    virtual void AddAttrReader(fieldid_t fieldId, const AttributeReaderPtr& attrReader) = 0;
    virtual void AddPackAttrReader(fieldid_t fieldId, const PackAttributeReaderPtr& attrReader) { assert(false); }

    virtual bool GetDocumentByPkStr(const std::string& pkStr, document::SearchSummaryDocument* summaryDoc) const;
    bool GetDocumentByPkStr(const std::string& pkStr, document::SearchSummaryDocument* summaryDoc,
                            const SummaryGroupIdVec& groupVec) const;
    template <typename Key>
    bool GetDocumentByPkHash(const Key& key, document::SearchSummaryDocument* summaryDoc);

protected:
    void AssertPkIndexExist() const;

protected:
    config::SummarySchemaPtr mSummarySchema;
    std::shared_ptr<PrimaryKeyIndexReader> mPKIndexReader;

private:
    IE_LOG_DECLARE();
};

typedef std::shared_ptr<SummaryReader> SummaryReaderPtr;

////////////////////////////////////////////////////////////////

inline void SummaryReader::AssertPkIndexExist() const
{
    if (unlikely(!mPKIndexReader)) {
        INDEXLIB_FATAL_ERROR(UnSupported, "primary key index may not be configured");
    }
}

inline bool SummaryReader::GetDocumentByPkStr(const std::string& pkStr,
                                              document::SearchSummaryDocument* summaryDoc) const
{
    AssertPkIndexExist();
    docid_t docId = mPKIndexReader->Lookup(pkStr);
    return GetDocument(docId, summaryDoc);
}

inline bool SummaryReader::GetDocumentByPkStr(const std::string& pkStr, document::SearchSummaryDocument* summaryDoc,
                                              const SummaryGroupIdVec& groupVec) const
{
    AssertPkIndexExist();
    docid_t docId = mPKIndexReader->Lookup(pkStr);
    return GetDocument(docId, summaryDoc, groupVec);
}

template <typename Key>
inline bool SummaryReader::GetDocumentByPkHash(const Key& key, document::SearchSummaryDocument* summaryDoc)
{
    AssertPkIndexExist();

    std::shared_ptr<indexlibv2::index::PrimaryKeyReader<Key>> pkReader =
        std::static_pointer_cast<indexlibv2::index::PrimaryKeyReader<Key>>(mPKIndexReader);
    if (pkReader.get() == NULL) {
        INDEXLIB_FATAL_ERROR(Runtime, "hash key type[%s], indexName[%s]", typeid(key).name(),
                             mPKIndexReader->GetIndexName().c_str());
    }

    docid_t docId = pkReader->Lookup(key);

    if (docId == INVALID_DOCID) {
        return false;
    }
    return GetDocument(docId, summaryDoc);
}
}} // namespace indexlib::index
