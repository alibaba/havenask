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
#include "ha3/search/BitmapTermQueryExecutor.h"

#include "autil/Log.h"
#include "ha3/common/Term.h"
#include "ha3/search/TermQueryExecutor.h"
#include "indexlib/index/inverted_index/TermPostingInfo.h"

using namespace indexlib::index;
using namespace isearch::common;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, BitmapTermQueryExecutor);

BitmapTermQueryExecutor::BitmapTermQueryExecutor(PostingIterator *iter, const Term &term)
    : TermQueryExecutor(iter, term) {
    initBitmapIterator();
}

BitmapTermQueryExecutor::~BitmapTermQueryExecutor() {}

void BitmapTermQueryExecutor::reset() {
    TermQueryExecutor::reset();
    initBitmapIterator();
}

} // namespace search
} // namespace isearch
