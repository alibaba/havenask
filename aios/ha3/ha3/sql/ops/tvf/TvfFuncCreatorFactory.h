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

#include "autil/Log.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/plugin/ModuleFactory.h"
#include "ha3/isearch.h"
#include "ha3/sql/common/Log.h"

namespace isearch {
namespace sql {
class TvfResourceContainer;
class TvfFuncCreatorR;
} // namespace sql
} // namespace isearch

namespace isearch {
namespace sql {

#define REGISTER_TVF_FUNC_CREATOR(creatorName, TvfFuncCreatorName)                                 \
    do {                                                                                           \
        TvfFuncCreatorName *creator = new TvfFuncCreatorName();                                    \
        creator->setName(creatorName);                                                             \
        if (!addTvfFunctionCreator(creator)) {                                                     \
            SQL_LOG(ERROR, "registered tvf func [%s] failed", creator->getName().c_str());         \
            delete creator;                                                                        \
            return false;                                                                          \
        }                                                                                          \
    } while (false)

static const std::string MODULE_SQL_TVF_FUNCTION = "_Sql_Tvf_Function";

class TvfFuncCreatorFactory : public build_service::plugin::ModuleFactory {
public:
    TvfFuncCreatorFactory();
    virtual ~TvfFuncCreatorFactory();

private:
    TvfFuncCreatorFactory(const TvfFuncCreatorFactory &);
    TvfFuncCreatorFactory &operator=(const TvfFuncCreatorFactory &);

public:
    virtual bool registerTvfFunctions() = 0;

public:
    virtual bool init(const KeyValueMap &parameters) override;
    virtual bool init(const KeyValueMap &parameters,
                      const build_service::config::ResourceReaderPtr &resourceReaderPtr) override;
    virtual bool init(const KeyValueMap &parameters,
                      const build_service::config::ResourceReaderPtr &resourceReaderPtr,
                      TvfResourceContainer *resourceContainer);
    std::map<std::string, TvfFuncCreatorR *> &getTvfFuncCreators();
    void destroy() override {
        delete this;
    };

public:
    TvfFuncCreatorR *getTvfFuncCreator(const std::string &name);

protected:
    bool addTvfFunctionCreator(TvfFuncCreatorR *creator);

protected:
    std::map<std::string, TvfFuncCreatorR *> _funcCreatorMap;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<TvfFuncCreatorFactory> TvfFuncCreatorFactoryPtr;
} // namespace sql
} // namespace isearch
