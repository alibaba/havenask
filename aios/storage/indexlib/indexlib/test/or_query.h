#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/query.h"

namespace indexlib { namespace test {

class OrQuery : public Query
{
public:
    OrQuery();
    ~OrQuery();

public:
    virtual docid_t Seek(docid_t docid);

    // TODO: not support
    virtual pos_t SeekPosition(pos_t pos);

    void AddQuery(QueryPtr query) { mQuerys.push_back(query); }
    size_t GetQueryCount() { return mQuerys.size(); }

private:
    void Init();

private:
    std::vector<QueryPtr> mQuerys;
    std::vector<docid_t> mDocIds;
    bool mInited;
    size_t mCursor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OrQuery);
}} // namespace indexlib::test
