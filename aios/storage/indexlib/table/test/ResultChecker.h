#pragma once

#include <memory>

#include "autil/Log.h"
#include "indexlib/table/test/RawDocument.h"
#include "indexlib/table/test/Result.h"

namespace indexlibv2 { namespace table {

class ResultChecker
{
public:
    static bool Check(const std::shared_ptr<Result>& result, bool expectedError,
                      const std::shared_ptr<Result>& expectResult);
    static bool UnorderCheck(const std::shared_ptr<Result>& result, const std::shared_ptr<Result>& expectResult);

private:
    static bool CheckOneDoc(const std::shared_ptr<RawDocument>& doc, const std::shared_ptr<RawDocument>& expectDoc,
                            bool needLog);
    static bool MatchOneDoc(const std::shared_ptr<Result>& result, const std::shared_ptr<RawDocument>& expectDoc,
                            std::vector<bool>& matchFlagVec);

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::table
