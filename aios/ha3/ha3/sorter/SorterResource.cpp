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
#include "ha3/sorter/SorterResource.h"


namespace isearch {
namespace sorter {

SorterLocation transSorterLocation(const std::string &location) {
    if (location == "SL_SEARCHER") {
        return SL_SEARCHER;
    } else if (location == "SL_SEARCHCACHEMERGER") {
        return SL_SEARCHCACHEMERGER;
    } else if (location == "SL_PROXY") {
        return SL_PROXY;
    } else if (location == "SL_QRS" ) {
        return SL_QRS;
    } else {
        return SL_UNKNOWN;
    }
}

} // namespace sorter
} // namespace isearch
