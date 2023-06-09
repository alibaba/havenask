/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __INDEXLIB_RESULT_H
#define __INDEXLIB_RESULT_H

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

#endif //__INDEXLIB_RESULT_H
