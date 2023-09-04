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

#include <map>
#include <set>
#include <string>

#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"

namespace iquan {
class TvfModels;
} // namespace iquan
namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace sql {
class TvfFunc;
class TvfFuncCreatorR;

class TvfFuncFactoryR : public navi::Resource {
public:
    TvfFuncFactoryR();
    ~TvfFuncFactoryR();
    TvfFuncFactoryR(const TvfFuncFactoryR &) = delete;
    TvfFuncFactoryR &operator=(const TvfFuncFactoryR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    TvfFunc *createTvfFunction(const std::string &tvfName) const;
    bool fillTvfModels(iquan::TvfModels &tvfModels);
    const std::map<std::string, TvfFuncCreatorR *> &getTvfNameToCreator() const {
        return _tvfNameToCreator;
    }

private:
    bool initFuncCreator();
    bool initTvfInfo();

public:
    static const std::string RESOURCE_ID;
    static const std::string DYNAMIC_GROUP;

private:
    std::set<TvfFuncCreatorR *> _tvfFuncCreators;
    std::map<std::string, TvfFuncCreatorR *> _funcToCreator;
    std::map<std::string, TvfFuncCreatorR *> _tvfNameToCreator;
};

NAVI_TYPEDEF_PTR(TvfFuncFactoryR);

} // namespace sql
