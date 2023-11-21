#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/query.h"

namespace indexlib { namespace test {

class DocidQuery : public Query
{
public:
    DocidQuery();
    ~DocidQuery();

public:
    bool Init(const std::string& docidStr);
    virtual docid_t Seek(docid_t docid);
    virtual pos_t SeekPosition(pos_t pos) { return INVALID_POSITION; }

private:
    std::vector<docid_t> mDocIdVec;
    size_t mCursor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocidQuery);
}} // namespace indexlib::test
