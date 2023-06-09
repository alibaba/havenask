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
#include <string>

#include "ha3/sql/ops/tvf/SqlTvfProfileInfo.h"
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

namespace build_service {
namespace config {
class ResourceReader;
} // namespace config
} // namespace build_service

namespace iquan {
class TvfModel;
class TvfModels;
} // namespace iquan

namespace isearch {
namespace sql {

class TvfFunc;

class TvfFuncCreatorR : public navi::Resource {
public:
    TvfFuncCreatorR(const std::string &tvfDef,
                    isearch::sql::SqlTvfProfileInfo info = isearch::sql::SqlTvfProfileInfo())
        : _tvfDef(tvfDef) {
        if (!info.empty()) {
            addTvfFunction(info);
        }
    }
    virtual ~TvfFuncCreatorR() {}
    TvfFuncCreatorR(const TvfFuncCreatorR &) = delete;
    TvfFuncCreatorR &operator=(const TvfFuncCreatorR &) = delete;

public:
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;
    virtual bool init(build_service::config::ResourceReader *resourceReader) {
        return true;
    }

private:
    virtual bool initTvfModel(iquan::TvfModel &tvfModel, const SqlTvfProfileInfo &info) {
        return true;
    }
    virtual TvfFunc *createFunction(const SqlTvfProfileInfo &info) = 0;

public:
    bool regTvfModels(iquan::TvfModels &tvfModels);
    TvfFunc *createFunction(const std::string &name);
    std::string getName();
    void setName(const std::string &name);
    void addTvfFunction(const SqlTvfProfileInfo &sqlTvfProfileInfo);
    SqlTvfProfileInfo getDefaultTvfInfo();

private:
    static bool checkTvfModel(const iquan::TvfModel &tvfModel);
    bool generateTvfModel(iquan::TvfModel &tvfModel, const SqlTvfProfileInfo &info);

protected:
    std::map<std::string, SqlTvfProfileInfo> _sqlTvfProfileInfos;
    std::string _name;
    std::string _tvfDef;
};

NAVI_TYPEDEF_PTR(TvfFuncCreatorR);
} // namespace sql
} // namespace isearch
