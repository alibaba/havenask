#pragma once

#include <memory>

#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/raw_document.h"

namespace indexlib { namespace test {

class Result
{
public:
    Result();
    ~Result();

public:
    void AddDoc(const RawDocumentPtr& doc) { mDocs.push_back(doc); }
    size_t GetDocCount() const { return mDocs.size(); }
    RawDocumentPtr GetDoc(size_t i) const { return mDocs[i]; }
    bool GetIsSubResult() const { return mIsSubResult; }
    void SetIsSubResult(bool isSubResult) { mIsSubResult = isSubResult; }
    std::string DebugString()
    {
        std::vector<docid_t> docIds;
        for (const RawDocumentPtr& doc : mDocs) {
            docIds.push_back(doc->GetDocId());
        }
        return autil::StringUtil::toString(docIds, ",");
    }

private:
    std::vector<RawDocumentPtr> mDocs;
    bool mIsSubResult;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(Result);
}} // namespace indexlib::test
