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
#include <set>
#include <stdint.h>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/turing/common/Ha3ServiceSnapshot.h"
#include "ha3/turing/common/Ha3BizMeta.h"
#include "suez/sdk/BizMeta.h"
#include "suez/turing/search/SearchContext.h"
#include "suez/turing/search/base/BizBase.h"

namespace multi_call {
class TopoInfoBuilder;
} // namespace multi_call
namespace suez {
class IndexProvider;
namespace turing {
class Biz;
class GraphRequest;
class GraphResponse;
class ServiceSnapshotBase;
struct SnapshotBaseOptions;
} // namespace turing
} // namespace suez

namespace isearch {
namespace turing {

class SearcherServiceSnapshot : public Ha3ServiceSnapshot {
public:
    SearcherServiceSnapshot();
    ~SearcherServiceSnapshot();

private:
    SearcherServiceSnapshot(const SearcherServiceSnapshot &);
    SearcherServiceSnapshot &operator=(const SearcherServiceSnapshot &);

public:
    void setDefaultAgg(const std::string &defaultAggStr) {
        _defaultAggStr = defaultAggStr;
    }
    void setParaWays(const std::string &paraWayStr) {
        _paraWaysStr = paraWayStr;
    }

protected:
    suez::turing::BizBasePtr createBizBase(const std::string &bizName,
                                           const suez::BizMeta &bizMeta) override;
    std::string getBizName(const std::string &bizName, const suez::BizMeta &bizMeta) const override;
    bool doInit(const suez::turing::SnapshotBaseOptions &options,
                suez::turing::ServiceSnapshotBase *oldSnapshot) override;
    void calculateBizMetas(const suez::BizMetas &currentBizMetas,
                           ServiceSnapshotBase *oldSnapshot,
                           suez::BizMetas &toUpdate,
                           suez::BizMetas &toKeep) override;

private:
    suez::BizMetas addExtraBizMetas(const suez::BizMetas &currentBizMetas) const;
    suez::turing::SearchContext *doCreateContext(const suez::turing::SearchContextArgs &args,
                                                 const suez::turing::GraphRequest *request,
                                                 suez::turing::GraphResponse *response) override;
    bool doWarmup() override;
private:
    std::string _fullVersionStr;
    std::string _defaultAggStr;
    std::string _paraWaysStr;
    mutable bool _enableSql;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SearcherServiceSnapshot> SearcherServiceSnapshotPtr;

} // namespace turing
} // namespace isearch
