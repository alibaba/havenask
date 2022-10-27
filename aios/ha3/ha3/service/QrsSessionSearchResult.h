#ifndef ISEARCH_QRSSESSIONSEARCHRESULT_H
#define ISEARCH_QRSSESSIONSEARCHRESULT_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/monitor/Ha3BizMetrics.h>
#include <multi_call/common/ErrorCode.h>

BEGIN_HA3_NAMESPACE(service);

struct QrsSessionSearchResult {
    QrsSessionSearchResult(const std::string resultStr_ = "",
                           HaCompressType resultCompressType_ = NO_COMPRESS,
                           ResultFormatType formatType_ = RF_XML)
        : multicallEc(multi_call::MULTI_CALL_ERROR_RPC_FAILED)
        , resultStr(resultStr_)
        , resultCompressType(resultCompressType_)
        , formatType(formatType_)
    {
    }
    multi_call::MultiCallErrorCode multicallEc;
    std::string resultStr;
    std::string errorStr;
    std::string srcStr;
    HaCompressType resultCompressType;
    ResultFormatType formatType;
};

HA3_TYPEDEF_PTR(QrsSessionSearchResult);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_QRSSESSIONSEARCHRESULT_H
