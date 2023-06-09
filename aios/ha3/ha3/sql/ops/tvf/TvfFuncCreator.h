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
#include <memory>
#include <string>
#include <utility>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/ops/tvf/SqlTvfProfileInfo.h"
#include "ha3/sql/ops/tvf/TvfFunc.h"

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

class TvfFuncCreator {
public:
    TvfFuncCreator(const std::string &tvfDef,
                   isearch::sql::SqlTvfProfileInfo info = isearch::sql::SqlTvfProfileInfo())
        : _tvfDef(tvfDef) {
        if (!info.empty()) {
            addTvfFunction(info);
        }
    }
    virtual ~TvfFuncCreator() {}

public:
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
    TvfFunc *createFunction(const std::string &name) {
        auto iter = _sqlTvfProfileInfos.find(name);
        if (iter == _sqlTvfProfileInfos.end()) {
            SQL_LOG(WARN, "tvf [%s]'s func info not found.", name.c_str());
            return nullptr;
        }
        TvfFunc *tvfFunc = createFunction(iter->second);
        if (tvfFunc != nullptr) {
            tvfFunc->setName(name);
        }
        return tvfFunc;
    }
    std::string getName() {
        return _name;
    }
    void setName(const std::string &name) {
        _name = name;
    }
    void addTvfFunction(const SqlTvfProfileInfo &sqlTvfProfileInfo) {
        SQL_LOG(INFO, "add tvf function info [%s]", sqlTvfProfileInfo.tvfName.c_str());
        _sqlTvfProfileInfos[sqlTvfProfileInfo.tvfName] = sqlTvfProfileInfo;
    }
    SqlTvfProfileInfo getDefaultTvfInfo() {
        if (_sqlTvfProfileInfos.size() == 1) {
            return _sqlTvfProfileInfos.begin()->second;
        }
        return {};
    }

private:
    static bool checkTvfModel(const iquan::TvfModel &tvfModel);
    bool generateTvfModel(iquan::TvfModel &tvfModel, const SqlTvfProfileInfo &info);

protected:
    std::map<std::string, SqlTvfProfileInfo> _sqlTvfProfileInfos;
    std::string _name;
    std::string _tvfDef;

protected:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<TvfFuncCreator> TvfFuncCreatorPtr;
typedef std::map<std::string, TvfFuncCreator *> TvfFuncCreatorMap;

} // namespace sql
} // namespace isearch
