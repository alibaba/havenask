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

#include "navi/engine/Resource.h"
#include "suez/turing/expression/cava/common/CavaPluginManagerR.h"
#include "suez/turing/expression/plugin/RankConfig.h"
#include "suez/turing/expression/plugin/RankManager.h"

namespace suez {
namespace turing {

class RankManagerR : public navi::Resource {
public:
    RankManagerR();
    ~RankManagerR();
    RankManagerR(const RankManagerR &) = delete;
    RankManagerR &operator=(const RankManagerR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    static const std::string RESOURCE_ID;

public:
    const RankManagerPtr &getManager() const { return _manager; }

private:
    CavaPluginManagerR *_cavaPluginManagerR = nullptr;
    std::string _configPath;
    RankConfig _rankConfig;
    RankManagerPtr _manager;
};

NAVI_TYPEDEF_PTR(RankManagerR);

} // namespace turing
} // namespace suez
