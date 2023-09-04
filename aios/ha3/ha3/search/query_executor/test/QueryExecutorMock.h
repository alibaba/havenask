#pragma once

#include <memory>
#include <stddef.h>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/QueryExecutor.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace isearch {
namespace search {

class QueryExecutorMock : public QueryExecutor {
public:
    QueryExecutorMock(const std::string &docIdStr);
    QueryExecutorMock(const std::vector<docid_t> &docIds);
    ~QueryExecutorMock();

private:
    QueryExecutorMock(const QueryExecutorMock &);
    QueryExecutorMock &operator=(const QueryExecutorMock &);

public:
    const std::string getName() const override {
        return std::string("QueryExecutorMock");
    }
    void reset() override;
    df_t getDF(GetDFType type) const override {
        return _docIds.size();
    }
    docid_t seekSubDoc(docid_t docId, docid_t subDocId, docid_t subDocEnd, bool needSubMatchdata);
    indexlib::index::ErrorCode seekSubDoc(docid_t docId,
                                          docid_t subDocId,
                                          docid_t subDocEnd,
                                          bool needSubMatchdata,
                                          docid_t &result) override {
        result = seekSubDoc(docId, subDocId, subDocEnd, needSubMatchdata);
        return indexlib::index::ErrorCode::OK;
    }

    /* override */ bool isMainDocHit(docid_t docId) const override {
        return true;
    };

public:
    /* override */ std::string toString() const override;

private:
    indexlib::index::ErrorCode doSeek(docid_t id, docid_t &result) override;

private:
    size_t _cursor;
    std::vector<docid_t> _docIds;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QueryExecutorMock> QueryExecutorMockPtr;

} // namespace search
} // namespace isearch
