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

#include <set>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/turing/common/Ha3BizMeta.h"
#include "suez/turing/search/ServiceSnapshot.h"

namespace isearch {
namespace turing {

class Ha3ServiceSnapshot : public suez::turing::ServiceSnapshot
                         , public std::enable_shared_from_this<Ha3ServiceSnapshot>
{
public:
    Ha3ServiceSnapshot();
    ~Ha3ServiceSnapshot();
    Ha3ServiceSnapshot(const Ha3ServiceSnapshot &) = delete;
    Ha3ServiceSnapshot& operator=(const Ha3ServiceSnapshot &) = delete;
public:
    void setBasicTuringBizNames(const std::set<std::string> &bizNames);
    Ha3BizMeta *getHa3BizMeta();

private:
    bool collectCatalogInfo() override;

protected:
    std::set<std::string> _basicTuringBizNames;
    Ha3BizMeta _ha3BizMeta;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace isearch
