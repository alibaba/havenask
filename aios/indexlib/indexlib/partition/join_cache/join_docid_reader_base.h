#ifndef __INDEXLIB_JOIN_DOCID_READER_BASE_H
#define __INDEXLIB_JOIN_DOCID_READER_BASE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(partition);

class JoinDocidReaderBase
{
public:
    JoinDocidReaderBase() {}
    virtual ~JoinDocidReaderBase() {}
public:
    virtual docid_t GetAuxDocid(docid_t mainDocid) = 0;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(JoinDocidReaderBase);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_JOIN_DOCID_READER_BASE_H
