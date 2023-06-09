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
#include <string>

#include "ha3/sql/ops/tvf/TvfResourceBase.h"
#include "ha3/summary/SummaryProfileManager.h"

namespace isearch {
namespace sql {

class TvfSummaryResource : public TvfResourceBase {
public:
    TvfSummaryResource(isearch::summary::SummaryProfileManagerPtr &summaryProfileMgrPtr)
        : _summaryProfileMgrPtr(summaryProfileMgrPtr) {}

public:
    virtual std::string name() const {
        return "TvfSummaryResource";
    }

public:
    isearch::summary::SummaryProfileManagerPtr getSummaryProfileManager() {
        return _summaryProfileMgrPtr;
    }

private:
    isearch::summary::SummaryProfileManagerPtr _summaryProfileMgrPtr;
};

typedef std::shared_ptr<TvfSummaryResource> TvfSummaryResourcePtr;
} // namespace sql
} // namespace isearch
