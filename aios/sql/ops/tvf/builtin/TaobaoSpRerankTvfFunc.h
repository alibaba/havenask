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
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "sql/common/Log.h" // IWYU pragma: keep
#include "sql/ops/tvf/TvfFuncCreatorR.h"
#include "sql/ops/tvf/builtin/RerankBase.h"
#include "table/Row.h"
#include "table/Table.h"

namespace sql {
class SqlTvfProfileInfo;
} // namespace sql

namespace sql {
class TvfFunc;
struct TvfFuncInitContext;

class TaobaoSpRerankTvfFunc : public RerankBase {
public:
    TaobaoSpRerankTvfFunc();
    ~TaobaoSpRerankTvfFunc();
    TaobaoSpRerankTvfFunc(const TaobaoSpRerankTvfFunc &) = delete;
    TaobaoSpRerankTvfFunc &operator=(const TaobaoSpRerankTvfFunc &) = delete;
    bool init(TvfFuncInitContext &context) override;

protected:
    bool computeTable(const table::TablePtr &input) override;
    void computeActualQuota(const std::vector<std::vector<table::Row>> &groups,
                            std::vector<size_t> &actualQuota);
    size_t computeLeftCount(const std::vector<std::vector<table::Row>> &groups,
                            const size_t leftCount,
                            std::vector<size_t> &groupsSize,
                            std::vector<size_t> &actualQuota);

private:
    std::vector<int32_t> _quotaNums;
    std::vector<std::string> _sortFields;
    std::vector<bool> _sortFlags;
    std::string _sortStr;

    bool _needSort;
};

typedef std::shared_ptr<TaobaoSpRerankTvfFunc> TaobaoSpRerankTvfFuncPtr;

class TaobaoSpRerankTvfFuncCreator : public TvfFuncCreatorR {
public:
    TaobaoSpRerankTvfFuncCreator();
    ~TaobaoSpRerankTvfFuncCreator();

public:
    std::string getResourceName() const override {
        return "taobaoSpRerankTvf";
    }

private:
    TaobaoSpRerankTvfFuncCreator(const TaobaoSpRerankTvfFuncCreator &) = delete;
    TaobaoSpRerankTvfFuncCreator &operator=(const TaobaoSpRerankTvfFuncCreator &) = delete;

private:
    TvfFunc *createFunction(const SqlTvfProfileInfo &info) override;
};

typedef std::shared_ptr<TaobaoSpRerankTvfFuncCreator> TaobaoSpRerankTvfFuncCreatorPtr;
} // namespace sql
