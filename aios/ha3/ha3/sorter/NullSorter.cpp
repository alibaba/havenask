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
#include "ha3/sorter/NullSorter.h"

#include <iosfwd>

#include "suez/turing/expression/common.h"
#include "suez/turing/expression/plugin/Sorter.h"

#include "autil/Log.h"

namespace suez {
namespace turing {
class SorterProvider;
}  // namespace turing
}  // namespace suez

using namespace std;
using namespace suez::turing;

namespace isearch {
namespace sorter {
AUTIL_LOG_SETUP(ha3, NullSorter);

NullSorter::NullSorter()
    : Sorter("NULL")
{
}

NullSorter::~NullSorter() {
}

Sorter *NullSorter::clone() {
    return new NullSorter(*this);
}

bool NullSorter::beginSort(suez::turing::SorterProvider *provider) {
    return true;
}

void NullSorter::sort(suez::turing::SortParam &sortParam) {
}

void NullSorter::endSort() {
}

} // namespace sorter
} // namespace isearch
