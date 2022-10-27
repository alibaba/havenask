#ifndef __INDEXLIB_PAYLOAD_CHECKER_H
#define __INDEXLIB_PAYLOAD_CHECKER_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/misc/exception.h"
#include <tr1/memory>

IE_NAMESPACE_BEGIN(index);

class PayloadChecker
{
public:
    PayloadChecker() {}
    ~PayloadChecker() {}
public:
    template<class IteratorType>
    static bool CatchDocPayloadException(IteratorType& it)
    {
        bool exceptionCatched = false;
        try 
        {
            it.GetDocPayload();
        }
        catch (autil::legacy::InvalidOperation ex)
        {
            exceptionCatched = true;
        }
        return exceptionCatched;
    }

    template<class IteratorType>
    static bool CatchPosPayloadException(IteratorType& it)
    {
        bool exceptionCatched = false;
        try 
        {
            it.GetPosPayload();
        }
        catch (autil::legacy::InvalidOperation ex)
        {
            exceptionCatched = true;
        }
        return exceptionCatched;
    }

private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<PayloadChecker> PayloadCheckerPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PAYLOAD_CHECKER_H
