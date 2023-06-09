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
#ifndef __INDEXLIB_RESULT_CHECKER_H
#define __INDEXLIB_RESULT_CHECKER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/raw_document.h"
#include "indexlib/test/result.h"

namespace indexlib { namespace test {

class ResultChecker
{
public:
    ResultChecker();
    ~ResultChecker();

public:
    static bool Check(const ResultPtr& result, const ResultPtr& expectResult);
    static bool UnorderCheck(const ResultPtr& result, const ResultPtr& expectResult);

private:
    static bool CheckOneDoc(const RawDocumentPtr& doc, const RawDocumentPtr& expectDoc, bool needLog = true);

    static bool MatchOneDoc(const ResultPtr& result, const RawDocumentPtr& expectDoc, std::vector<bool>& matchFlagVec);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ResultChecker);
}} // namespace indexlib::test

#endif //__INDEXLIB_RESULT_CHECKER_H
