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
#pragma once

#include <string>

#include "ha3/rank/Comparator.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace rank {
class DistinctInfo;
}  // namespace rank
}  // namespace isearch
namespace matchdoc {
class MatchDoc;
template <typename T> class Reference;
}  // namespace matchdoc

namespace isearch {
namespace rank {

// This class may not be used
//
class GradeComparator : public Comparator
{
public:
    GradeComparator(matchdoc::Reference<DistinctInfo> *distInfoRef, bool flag = false);
    ~GradeComparator();
public:
    bool compare(matchdoc::MatchDoc a, matchdoc::MatchDoc b) const override;

    std::string getType() const  override { return "grade"; }

private:
    matchdoc::Reference<DistinctInfo> *_distInfoRef;
    bool _flag;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace rank
} // namespace isearch

