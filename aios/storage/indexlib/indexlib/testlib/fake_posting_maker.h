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
#ifndef __INDEXLIB_FAKE_POSTING_MAKER_H
#define __INDEXLIB_FAKE_POSTING_MAKER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/testlib/fake_posting_iterator.h"

namespace indexlib { namespace testlib {

class FakePostingMaker
{
public:
    FakePostingMaker();
    ~FakePostingMaker();

public:
    static void makeFakePostingsDetail(const std::string& str, FakePostingIterator::Map& postingMap);
    static void makeFakePostingsSection(const std::string& str, FakePostingIterator::Map& postingMap);

protected:
    static void parseOnePostingDetail(const std::string& postingStr, FakePostingIterator::Posting& posting);

    static void parseOnePostingSection(const std::string& postingStr, FakePostingIterator::Posting& posting);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakePostingMaker);
}} // namespace indexlib::testlib

#endif //__INDEXLIB_FAKE_POSTING_MAKER_H
