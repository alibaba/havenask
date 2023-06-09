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

#include "ha3/isearch.h"
#include "ha3/turing/searcher/SearcherBiz.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace turing {

class ParaSearchBiz: public SearcherBiz
{
public:
    ParaSearchBiz();
    ~ParaSearchBiz();
private:
    ParaSearchBiz(const ParaSearchBiz &) = delete;
    ParaSearchBiz& operator=(const ParaSearchBiz &) = delete;
private:
    std::string getDefaultGraphDefPath() override;
    std::string getConfigZoneBizName(const std::string &zoneName) override;
    std::string getOutConfName(const std::string &confName) override;
    int getInterOpThreadPool() override;
    void patchTuringOptionsInfo(
            const isearch::config::TuringOptionsInfo &turingOptionsInfo,
            autil::legacy::json::JsonMap &jsonMap) override;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ParaSearchBiz> ParaSearchBizPtr;

} // namespace turing
} // namespace isearch
