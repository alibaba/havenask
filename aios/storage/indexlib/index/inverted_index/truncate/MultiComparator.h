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

#include <memory>

#include "autil/Log.h"
#include "indexlib/index/inverted_index/truncate/Comparator.h"

namespace indexlib::index {

class MultiComparator : public Comparator
{
public:
    MultiComparator() = default;
    ~MultiComparator() = default;

public:
    void AddComparator(const std::shared_ptr<Comparator>& cmp);
    bool LessThan(const DocInfo* left, const DocInfo* right) const override;

private:
    std::vector<std::shared_ptr<Comparator>> _comps;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
