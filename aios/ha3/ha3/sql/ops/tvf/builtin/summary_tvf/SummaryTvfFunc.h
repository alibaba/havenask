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
#include <vector>

#include "ha3/common/Request.h"
#include "ha3/config/HitSummarySchema.h"
#include "ha3/sql/ops/tvf/TvfFunc.h"
#include "ha3/sql/ops/tvf/TvfFuncCreatorR.h"
#include "indexlib/indexlib.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "table/Table.h"

namespace table {
class Column;
} // namespace table

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace isearch {
namespace sql {
class SqlTvfProfileInfo;
} // namespace sql

namespace common {
class SummaryHit;
} // namespace common
namespace search {
class SearchCommonResource;
} // namespace search
namespace summary {
class SummaryExtractorChain;
class SummaryExtractorProvider;
class SummaryProfile;
struct SummaryQueryInfo;
} // namespace summary
} // namespace isearch

namespace isearch {
namespace sql {

class SummaryTvfFunc : public TvfFunc {
public:
    SummaryTvfFunc();
    ~SummaryTvfFunc();
    SummaryTvfFunc(const SummaryTvfFunc &) = delete;
    SummaryTvfFunc &operator=(const SummaryTvfFunc &) = delete;

public:
    bool init(TvfFuncInitContext &context) override;
    bool compute(const table::TablePtr &input, bool eof, table::TablePtr &output) override;

private:
    bool doCompute(const table::TablePtr &input);
    bool prepareResource(TvfFuncInitContext &context);
    void toSummarySchema(const table::TablePtr &input);
    void toSummaryHits(const table::TablePtr &input,
                       std::vector<isearch::common::SummaryHit *> &summaryHits);
    template <typename T>
    bool fillSummaryDocs(table::Column *column,
                         std::vector<isearch::common::SummaryHit *> &summaryHits,
                         const table::TablePtr &input,
                         summaryfieldid_t summaryFieldId);
    bool fromSummaryHits(const table::TablePtr &input,
                         std::vector<isearch::common::SummaryHit *> &summaryHits);

private:
    isearch::summary::SummaryExtractorChain *_summaryExtractorChain;
    isearch::summary::SummaryExtractorProvider *_summaryExtractorProvider;
    const isearch::summary::SummaryProfile *_summaryProfile;
    isearch::summary::SummaryQueryInfo *_summaryQueryInfo;
    isearch::common::RequestPtr _request;
    isearch::search::SearchCommonResource *_resource;
    isearch::config::HitSummarySchemaPtr _hitSummarySchema;
    autil::mem_pool::Pool *_queryPool;
    std::vector<std::string> _outputFields;
    std::vector<std::string> _outputFieldsType;
};

typedef std::shared_ptr<SummaryTvfFunc> SummaryTvfFuncPtr;

class SummaryTvfFuncCreator : public TvfFuncCreatorR {
public:
    SummaryTvfFuncCreator();
    ~SummaryTvfFuncCreator();

public:
    void def(navi::ResourceDefBuilder &builder) const override {
        builder.name("SUMMARY_TVF_FUNC");
    }

private:
    SummaryTvfFuncCreator(const SummaryTvfFuncCreator &) = delete;
    SummaryTvfFuncCreator &operator=(const SummaryTvfFuncCreator &) = delete;

private:
    TvfFunc *createFunction(const SqlTvfProfileInfo &info) override;
};

typedef std::shared_ptr<SummaryTvfFuncCreator> SummaryTvfFuncCreatorPtr;
} // namespace sql
} // namespace isearch
