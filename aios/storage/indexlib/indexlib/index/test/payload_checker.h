#ifndef __INDEXLIB_PAYLOAD_CHECKER_H
#define __INDEXLIB_PAYLOAD_CHECKER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace index {

class PayloadChecker
{
public:
    PayloadChecker() {}
    ~PayloadChecker() {}

public:
    template <class IteratorType>
    static bool CatchDocPayloadException(IteratorType& it)
    {
        bool exceptionCatched = false;
        try {
            it.GetDocPayload();
        } catch (autil::legacy::InvalidOperation ex) {
            exceptionCatched = true;
        }
        return exceptionCatched;
    }

    template <class IteratorType>
    static bool CatchPosPayloadException(IteratorType& it)
    {
        bool exceptionCatched = false;
        try {
            it.GetPosPayload();
        } catch (autil::legacy::InvalidOperation ex) {
            exceptionCatched = true;
        }
        return exceptionCatched;
    }

private:
    IE_LOG_DECLARE();
};

typedef std::shared_ptr<PayloadChecker> PayloadCheckerPtr;
}} // namespace indexlib::index

#endif //__INDEXLIB_PAYLOAD_CHECKER_H
