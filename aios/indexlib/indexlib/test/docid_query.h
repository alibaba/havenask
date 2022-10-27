#ifndef __INDEXLIB_DOCID_QUERY_H
#define __INDEXLIB_DOCID_QUERY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/test/query.h"

IE_NAMESPACE_BEGIN(test);

class DocidQuery : public Query
{
public:
    DocidQuery();
    ~DocidQuery();
public:
    bool Init(const std::string& docidStr);
    virtual docid_t Seek(docid_t docid);
    virtual pos_t SeekPosition(pos_t pos)
    { return INVALID_POSITION; }

private:
    typedef std::vector<docid_t> DocIdVec;
    DocIdVec mDocIdVec;
    size_t mCursor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocidQuery);

IE_NAMESPACE_END(test);

#endif //__INDEXLIB_DOCID_QUERY_H
