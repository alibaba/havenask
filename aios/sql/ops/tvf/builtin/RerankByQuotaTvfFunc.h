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
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "sql/common/Log.h" // IWYU pragma: keep
#include "sql/ops/tvf/TvfFuncCreatorR.h"
#include "sql/ops/tvf/builtin/RerankBase.h"
#include "table/ComboComparator.h"
#include "table/Row.h"
#include "table/Table.h"

namespace sql {
class SqlTvfProfileInfo;
} // namespace sql

namespace sql {
class TvfFunc;
struct TvfFuncInitContext;

class RerankByQuotaTvfFunc : public RerankBase {
public:
    RerankByQuotaTvfFunc();
    ~RerankByQuotaTvfFunc();
    RerankByQuotaTvfFunc(const RerankByQuotaTvfFunc &) = delete;
    RerankByQuotaTvfFunc &operator=(const RerankByQuotaTvfFunc &) = delete;

private:
    std::pair<int32_t, int32_t> rerankByQuota(const table::TablePtr &table,
                                              table::ComboComparatorPtr comparator,
                                              const std::vector<table::Row> &group,
                                              const int32_t quotaNum,
                                              std::vector<table::Row> *pTarget,
                                              std::vector<table::Row> *pExtra);
    void makeUp(std::vector<table::Row> &target, const std::vector<table::Row> &extra);

public:
    bool init(TvfFuncInitContext &context) override;
    bool computeTable(const table::TablePtr &input) override;

private:
    std::vector<int32_t> _firstPhaseQuotaNums;
    std::vector<int32_t> _secondPhaseQuotaNums;
    std::vector<std::string> _rerankFields;
    std::vector<std::string> _sortFields;
    std::vector<bool> _sortFlags;
    std::string _quotaNumStr;    // 100,200;100,200
    std::string _rerankFieldStr; // category1;category2
    std::string _sortStr;        // +field1;-field2

    uint32_t _perCategoryCount;
    uint32_t _windowSize;

    bool _firstPhaseNeedMakeUp, _secondPhaseNeedMakeUp;
};

typedef std::shared_ptr<RerankByQuotaTvfFunc> RerankByQuotaTvfFuncPtr;

class RerankByQuotaTvfFuncCreator : public TvfFuncCreatorR {
public:
    RerankByQuotaTvfFuncCreator();
    ~RerankByQuotaTvfFuncCreator();

public:
    std::string getResourceName() const override {
        return "rerankByQuotaTvf";
    }

private:
    RerankByQuotaTvfFuncCreator(const RerankByQuotaTvfFuncCreator &) = delete;
    RerankByQuotaTvfFuncCreator &operator=(const RerankByQuotaTvfFuncCreator &) = delete;

private:
    TvfFunc *createFunction(const SqlTvfProfileInfo &info) override;
};

typedef std::shared_ptr<RerankByQuotaTvfFuncCreator> RerankByQuotaTvfFuncCreatorPtr;
} // namespace sql
