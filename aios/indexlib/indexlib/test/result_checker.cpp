#include "indexlib/test/result_checker.h"

using namespace std;

IE_NAMESPACE_BEGIN(test);
IE_LOG_SETUP(test, ResultChecker);

ResultChecker::ResultChecker() 
{
}

ResultChecker::~ResultChecker() 
{
}

bool ResultChecker::Check(const ResultPtr& result, const ResultPtr& expectResult)
{
    if (result->GetDocCount() != expectResult->GetDocCount())
    {
        IE_LOG(ERROR, "check doc count failed, actual [%ld] expect [%ld]",
               result->GetDocCount(), expectResult->GetDocCount());
        return false;
    }

    for (size_t i = 0; i < expectResult->GetDocCount(); ++i)
    {
        if (!CheckOneDoc(result->GetDoc(i), expectResult->GetDoc(i)))
        {
            IE_LOG(ERROR, "check [%ld]th doc failed", i);
            return false;
        }
    }
    return true;
}

bool ResultChecker::UnorderCheck(const ResultPtr& result, const ResultPtr& expectResult)
{
    if (result->GetDocCount() != expectResult->GetDocCount())
    {
        IE_LOG(ERROR, "check doc count failed, actual [%ld] expect [%ld]",
               result->GetDocCount(), expectResult->GetDocCount());
        return false;
    }

    std::vector<size_t> unMatchExpectResults;
    std::vector<bool> matchFlagVec(result->GetDocCount(), false);
    for (size_t i = 0; i < expectResult->GetDocCount(); ++i)
    {
        if (!MatchOneDoc(result, expectResult->GetDoc(i), matchFlagVec))
        {
            unMatchExpectResults.push_back(i);
        }
    }

    if (unMatchExpectResults.empty())
    {
        return true;
    }

    size_t idx = 0;
    for (size_t i = 0; i < matchFlagVec.size(); ++i)
    {
        if (matchFlagVec[i]) { continue; }
        size_t unMatchIdx = unMatchExpectResults[idx++];
        bool ret = CheckOneDoc(result->GetDoc(i), expectResult->GetDoc(unMatchIdx));
        assert(!ret);
        (void)ret;
        IE_LOG(ERROR, "check [%ld]th expected doc with [%ld] actual doc failed", unMatchIdx, i);
    }
    assert(idx == unMatchExpectResults.size());
    return false;
}

bool ResultChecker::CheckOneDoc(const RawDocumentPtr& doc,
                                const RawDocumentPtr& expectDoc, bool needLog)
{
    RawDocument::Iterator it = expectDoc->Begin();
    for (; it != expectDoc->End(); ++it)
    {
        string expectValue = it->second;
        string actualValue = doc->GetField(it->first);
        if (expectValue != actualValue)
        {
            if (needLog)
            {
                IE_LOG(ERROR, "check doc [%s] failed, actual [%s] expect [%s]",
                       it->first.c_str(), actualValue.c_str(), expectValue.c_str());
            }
            return false;
        }
    }

    if (expectDoc->GetTimestamp() != INVALID_TIMESTAMP)
    {
        if (expectDoc->GetTimestamp() != doc->GetTimestamp())
        {
            if (needLog)
            {
                IE_LOG(ERROR, "check doc ts failed, actual ts[%ld] expect ts[%ld]",
                       doc->GetTimestamp(), expectDoc->GetTimestamp());
            }
            return false;
        }
    }

    return true;
}

bool ResultChecker::MatchOneDoc(const ResultPtr& result,
                                const RawDocumentPtr& expectDoc,
                                std::vector<bool>& matchFlagVec)
{
    for (size_t i = 0; i < result->GetDocCount(); ++i)
    {
        if (matchFlagVec[i])
        {
            // result already matched before
            continue;
        }
        
        if (CheckOneDoc(result->GetDoc(i), expectDoc, false))
        {
            // match one doc
            matchFlagVec[i] = true;
            return true;
        }
    }
    return false;
}

IE_NAMESPACE_END(test);

