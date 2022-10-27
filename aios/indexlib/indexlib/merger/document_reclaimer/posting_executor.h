#ifndef __INDEXLIB_POSTING_EXECUTOR_H
#define __INDEXLIB_POSTING_EXECUTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(merger);

class PostingExecutor
{
public:
    PostingExecutor()
        : mCurrent(INVALID_DOCID)
    {}
    virtual ~PostingExecutor() {}
public:
    virtual df_t GetDF() const = 0;
    docid_t Seek(docid_t id)
    {
        if (id > mCurrent)
        {
            mCurrent = DoSeek(id);
        }
        return mCurrent;        
    }
private:
    virtual docid_t DoSeek(docid_t id) = 0;
    
protected:
    docid_t mCurrent;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PostingExecutor);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_POSTING_EXECUTOR_H
