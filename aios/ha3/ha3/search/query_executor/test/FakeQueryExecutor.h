#pragma once

#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/QueryExecutor.h"
#include "indexlib/indexlib.h"

namespace isearch {
namespace search {

class FakeQueryExecutor : public QueryExecutor {
public:
    FakeQueryExecutor();
    ~FakeQueryExecutor();

public:
    /* override */ const std::string getName() const;

    /* override */ docid_t getDocId();

    /* override */ docid_t seek(docid_t id);
    /* override */ void reset() {}
    /* override */ df_t getDF(GetDFType type) const {
        return 0;
    }

public:
    /* override */ std::string toString() const;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace search
} // namespace isearch
