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
#include <vector>

#include "table/Comparator.h"
#include "table/Row.h"

namespace table {
class Comparator;
} // namespace table

namespace table {

class ComboComparator final : public Comparator {
public:
    ComboComparator();
    virtual ~ComboComparator();

public:
    bool compare(Row a, Row b) const override;

public:
    void addComparator(const Comparator *cmp);

private:
    typedef std::vector<const Comparator *> ComparatorVector;
    ComparatorVector _cmpVector;
};

using ComboComparatorPtr = std::shared_ptr<ComboComparator>;

} // namespace table
