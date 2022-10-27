#ifndef __INDEXLIB_RESULT_CHECKER_H
#define __INDEXLIB_RESULT_CHECKER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/test/result.h"
#include "indexlib/test/raw_document.h"

IE_NAMESPACE_BEGIN(test);

class ResultChecker
{
public:
    ResultChecker();
    ~ResultChecker();
public:
    static bool Check(const ResultPtr& result, const ResultPtr& expectResult);
    static bool UnorderCheck(const ResultPtr& result, const ResultPtr& expectResult);

private:
    static bool CheckOneDoc(const RawDocumentPtr& doc, const RawDocumentPtr& expectDoc,
                            bool needLog = true);

    static bool MatchOneDoc(const ResultPtr& result, const RawDocumentPtr& expectDoc,
                            std::vector<bool>& matchFlagVec);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ResultChecker);

IE_NAMESPACE_END(test);

#endif //__INDEXLIB_RESULT_CHECKER_H
