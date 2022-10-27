#ifndef __INDEXLIB_RESULT_H
#define __INDEXLIB_RESULT_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/testlib/raw_document.h"

IE_NAMESPACE_BEGIN(testlib);

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

private:
    std::vector<RawDocumentPtr> mDocs;
    bool mIsSubResult;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(Result);

IE_NAMESPACE_END(testlib);

#endif //__INDEXLIB_RESULT_H
