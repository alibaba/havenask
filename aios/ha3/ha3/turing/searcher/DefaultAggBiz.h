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

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/config/ConfigAdapter.h"
#include "ha3/turing/common/Ha3BizBase.h"
#include "tensorflow/core/lib/core/status.h"

namespace isearch {
namespace turing {

class DefaultAggBiz: public Ha3BizBase
{
public:
    DefaultAggBiz(const std::string &aggGraphPrefix);
    ~DefaultAggBiz();
private:
    DefaultAggBiz(const DefaultAggBiz &);
    DefaultAggBiz& operator=(const DefaultAggBiz &);
public:
    static bool isSupportAgg(const std::string &defaultAgg);
protected:
    bool getDefaultBizJson(std::string &defaultBizJson) override;
    tensorflow::Status preloadBiz() override;
    std::string getBizInfoFile() override;
private:
    std::string convertFunctionConf();
private:
    config::ConfigAdapterPtr _configAdapter;
    std::string _aggGraphPrefix;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<DefaultAggBiz> DefaultAggBizPtr;

} // namespace turing
} // namespace isearch

