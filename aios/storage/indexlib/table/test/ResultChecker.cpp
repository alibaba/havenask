#include "indexlib/table/test//ResultChecker.h"

#include "indexlib/base/Constant.h"

namespace indexlibv2 { namespace table {
AUTIL_LOG_SETUP(indexlib.table, ResultChecker);

bool ResultChecker::Check(const std::shared_ptr<Result>& result, bool expectedError,
                          const std::shared_ptr<Result>& expectResult)
{
    if (!result && !expectResult) {
        return true;
    }

    if (result && !expectResult) {
        AUTIL_LOG(ERROR, "check result, actual doc count [%ld], expect [%p]", result->GetDocCount(),
                  expectResult.get());
        return false;
    }
    if (!result && expectResult) {
        AUTIL_LOG(ERROR, "check result, actual [%p], expect doc count [%ld]", result.get(),
                  expectResult->GetDocCount());
        return false;
    }

    if (result->HasError() != expectedError) {
        AUTIL_LOG(ERROR, "check result, actual has error [%d], expect has error [%d]", result->HasError(),
                  expectedError);
        return false;
    }

    if (result->GetDocCount() != expectResult->GetDocCount()) {
        AUTIL_LOG(ERROR, "check doc count failed, actual [%ld], expect [%ld], actual docids [%s]",
                  result->GetDocCount(), expectResult->GetDocCount(), result->DebugString().c_str());
        return false;
    }

    for (size_t i = 0; i < expectResult->GetDocCount(); ++i) {
        if (!CheckOneDoc(result->GetDoc(i), expectResult->GetDoc(i), true)) {
            AUTIL_LOG(ERROR, "check [%ld]th doc failed", i);
            return false;
        }
    }
    return true;
}

bool ResultChecker::UnorderCheck(const std::shared_ptr<Result>& result, const std::shared_ptr<Result>& expectResult)
{
    if (result->GetDocCount() != expectResult->GetDocCount()) {
        AUTIL_LOG(ERROR, "check doc count failed, actual [%ld] expect [%ld], actual docids [%s]", result->GetDocCount(),
                  expectResult->GetDocCount(), result->DebugString().c_str());
        return false;
    }

    std::vector<size_t> unMatchExpectResults;
    std::vector<bool> matchFlagVec(result->GetDocCount(), false);
    for (size_t i = 0; i < expectResult->GetDocCount(); ++i) {
        if (!MatchOneDoc(result, expectResult->GetDoc(i), matchFlagVec)) {
            unMatchExpectResults.push_back(i);
        }
    }

    if (unMatchExpectResults.empty()) {
        return true;
    }

    size_t idx = 0;
    for (size_t i = 0; i < matchFlagVec.size(); ++i) {
        if (matchFlagVec[i]) {
            continue;
        }
        size_t unMatchIdx = unMatchExpectResults[idx++];
        bool ret = CheckOneDoc(result->GetDoc(i), expectResult->GetDoc(unMatchIdx), true);
        assert(!ret);
        (void)ret;
        AUTIL_LOG(ERROR, "check [%ld]th expected doc with [%ld] actual doc failed", unMatchIdx, i);
    }
    assert(idx == unMatchExpectResults.size());
    return false;
}

bool ResultChecker::CheckOneDoc(const std::shared_ptr<RawDocument>& doc, const std::shared_ptr<RawDocument>& expectDoc,
                                bool needLog)
{
    RawDocument::Iterator it = expectDoc->Begin();
    for (; it != expectDoc->End(); ++it) {
        std::string expectValue = it->second;
        std::string actualValue = doc->GetField(it->first);
        if (expectValue != actualValue) {
            if (expectValue == "__NULL__" && actualValue.empty()) {
                return true;
            }
            if (needLog) {
                AUTIL_LOG(ERROR, "check doc [%s] failed, actual [%s] expect [%s]", it->first.c_str(),
                          actualValue.c_str(), expectValue.c_str());
            }
            return false;
        }
    }

    if (expectDoc->GetTimestamp() != INVALID_TIMESTAMP) {
        if (expectDoc->GetTimestamp() != doc->GetTimestamp()) {
            if (needLog) {
                AUTIL_LOG(ERROR, "check doc ts failed, actual ts[%ld] expect ts[%ld]", doc->GetTimestamp(),
                          expectDoc->GetTimestamp());
            }
            return false;
        }
    }

    return true;
}

bool ResultChecker::MatchOneDoc(const std::shared_ptr<Result>& result, const std::shared_ptr<RawDocument>& expectDoc,
                                std::vector<bool>& matchFlagVec)
{
    for (size_t i = 0; i < result->GetDocCount(); ++i) {
        if (matchFlagVec[i]) {
            // result already matched before
            continue;
        }

        if (CheckOneDoc(result->GetDoc(i), expectDoc, false)) {
            // match one doc
            matchFlagVec[i] = true;
            return true;
        }
    }
    return false;
}
}} // namespace indexlibv2::table
