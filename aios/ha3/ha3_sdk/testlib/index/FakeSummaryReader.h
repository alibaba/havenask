#pragma once

#include <algorithm>
#include <assert.h>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/ConstString.h"
#include "autil/legacy/exception.h"
#include "indexlib/config/summary_group_config.h"
#include "indexlib/config/summary_schema.h"
#include "indexlib/document/index_document/normal_document/search_summary_document.h"
#include "indexlib/document/index_document/normal_document/summary_document.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/util/Exception.h"

namespace indexlib {
namespace index {

class FakeSummaryReader : public SummaryReader {
public:
    FakeSummaryReader()
        : SummaryReader(config::SummarySchemaPtr()) {}

    FakeSummaryReader(const config::SummarySchemaPtr &summarySchema)
        : SummaryReader(summarySchema) {}

    ~FakeSummaryReader() {
        for (std::vector<indexlib::document::SearchSummaryDocument *>::iterator it
             = _summaryDocuments.begin();
             it != _summaryDocuments.end();
             ++it) {
            delete *it;
        }
    }

    void AddSummaryDocument(indexlib::document::SearchSummaryDocument *doc) {
        _summaryDocuments.push_back(doc);
    }

    virtual uint32_t GetDocCount() const {
        assert(false);
        return 0;
    }

public:
    virtual bool ReOpen(const index_base::SegmentDirectoryPtr &segDir,
                        const std::shared_ptr<InvertedIndexReader> &pkIndexReader) {
        assert(false);
        return false;
    }

    virtual bool WarmUpDocument(docid_t docId) {
        assert(false);
        return false;
    }

    virtual bool IsInCache(docid_t docId) const {
        assert(false);
        return false;
    }

    bool GetDocument(docid_t docId,
                     indexlib::document::SearchSummaryDocument *summaryDoc) const override {
        if (docId < (docid_t)_summaryDocuments.size()) {
            for (size_t i = 0; i < _summaryDocuments[docId]->GetFieldCount(); ++i) {
                const autil::StringView *str = _summaryDocuments[docId]->GetFieldValue(i);
                if (str) {
                    summaryDoc->SetFieldValue(i, str->data(), str->size());
                }
            }
            return true;
        }

        return false;
    }
    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    GetDocument(const std::vector<docid_t> &docIds,
                autil::mem_pool::Pool *sessionPool,
                file_system::ReadOption option,
                const SearchSummaryDocVec *docs) const noexcept override {
        indexlib::index::ErrorCodeVec ret;
        for (size_t i = 0; i < docIds.size(); ++i) {
            if (GetDocument(docIds[i], (*docs)[i])) {
                ret.push_back(indexlib::index::ErrorCode::OK);
            } else {
                ret.push_back(indexlib::index::ErrorCode::Runtime);
            }
        }
        co_return ret;
    }

    bool GetDocument(docid_t docId,
                     indexlib::document::SearchSummaryDocument *summaryDoc,
                     const SummaryGroupIdVec &groupVec) const override {
        if (docId < (docid_t)_summaryDocuments.size()) {
            for (size_t i = 0; i < groupVec.size(); ++i) {
                const indexlib::config::SummaryGroupConfigPtr &summaryGroupConfig
                    = mSummarySchema->GetSummaryGroupConfig(groupVec[i]);
                summaryfieldid_t fieldId = summaryGroupConfig->GetSummaryFieldIdBase();
                for (size_t j = 0; j < summaryGroupConfig->GetSummaryFieldsCount(); j++) {
                    const autil::StringView *str = _summaryDocuments[docId]->GetFieldValue(fieldId);
                    if (str) {
                        summaryDoc->SetFieldValue(fieldId, str->data(), str->size());
                    }
                    fieldId++;
                }
            }
            return true;
        }
        return false;
    }
    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    GetDocument(const std::vector<docid_t> &docIds,
                const SummaryGroupIdVec &groupVec,
                autil::mem_pool::Pool *sessionPool,
                file_system::ReadOption option,
                const SearchSummaryDocVec *docs) const noexcept override {
        indexlib::index::ErrorCodeVec ret;
        for (size_t i = 0; i < docIds.size(); ++i) {
            if (GetDocument(docIds[i], (*docs)[i], groupVec)) {
                ret.push_back(indexlib::index::ErrorCode::OK);
            } else {
                ret.push_back(indexlib::index::ErrorCode::Runtime);
            }
        }
        co_return ret;
    }

    bool GetDocumentByPkStr(const std::string &pkStr,
                            indexlib::document::SearchSummaryDocument *summaryDoc) const override {
        return true;
    }

    void AddAttrReader(fieldid_t fieldId, const AttributeReaderPtr &attrReader) override {}

    AttributeReaderPtr GetAttrReader(fieldid_t fid) const {
        return AttributeReaderPtr();
    }

    virtual std::string GetField(docid_t docId, fieldid_t fid) const {
        assert(false);
        return "";
    }

    virtual SummaryReader *Clone() const {
        assert(false);
        return NULL;
    }

    virtual std::string GetIdentifier() const override {
        assert(false);
        return "";
    }
    virtual void WarmUp() {
        assert(false);
    }

private:
    std::vector<indexlib::document::SearchSummaryDocument *> _summaryDocuments;
};

typedef std::shared_ptr<FakeSummaryReader> FakeSummaryReaderPtr;

class ExceptionSummaryReader : public FakeSummaryReader {
    using FakeSummaryReader::GetDocument;

public:
    /*override*/
    virtual document::SummaryDocumentPtr GetDocument(docid_t docId) const {
        AUTIL_LEGACY_THROW(util::FileIOException, "fake exception");
    }

    virtual bool GetDocument(docid_t docId,
                             indexlib::document::SearchSummaryDocument *summaryDoc) const {
        AUTIL_LEGACY_THROW(util::FileIOException, "fake exception");
    }
};

typedef std::shared_ptr<ExceptionSummaryReader> ExceptionSummaryReaderPtr;

} // namespace index
} // namespace indexlib
