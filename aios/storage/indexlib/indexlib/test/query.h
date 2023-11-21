#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace test {

enum QueryType {
    qt_atomic = 0,
    qt_and,
    qt_or,
    qt_docid,
    qt_source,
    qt_unknown,
};

class Query
{
public:
    Query(QueryType queryType);
    virtual ~Query();

public:
    virtual docid_t Seek(docid_t docid) = 0;
    virtual pos_t SeekPosition(pos_t pos) = 0;
    QueryType GetQueryType() const { return mQueryType; }

    void SetSubQuery() { mIsSubQuery = true; }
    bool IsSubQuery() const { return mIsSubQuery; }

private:
    bool mIsSubQuery;
    QueryType mQueryType;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(Query);
}} // namespace indexlib::test
