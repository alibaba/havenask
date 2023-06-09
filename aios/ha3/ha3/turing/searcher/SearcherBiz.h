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

#include "autil/legacy/json.h"
#include "suez/sdk/BizMeta.h"
#include "suez/sdk/TableReader.h"

#include "ha3/config/ConfigAdapter.h"
#include "ha3/isearch.h"
#include "ha3/monitor/SearcherBizMetrics.h"
#include "ha3/proto/BasicDefs.pb.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/common/Ha3BizBase.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "autil/RangeUtil.h"

namespace isearch {
namespace turing {

class SearcherBiz: public Ha3BizBase
{
public:
    SearcherBiz();
    ~SearcherBiz();
protected:
    bool getDefaultBizJson(std::string &defaultBizJson) override;
    virtual tensorflow::Status preloadBiz() override;
private:
    SearcherBiz(const SearcherBiz &);
    SearcherBiz& operator=(const SearcherBiz &);

protected:
    tensorflow::SessionResourcePtr createSessionResource(uint32_t count) override;
    suez::turing::QueryResourcePtr createQueryResource() override;
    std::string getBizInfoFile() override;
    virtual std::string getDefaultGraphDefPath();
    virtual std::string getConfigZoneBizName(const std::string &zoneName);
    virtual std::string getOutConfName(const std::string &confName);
    virtual int getInterOpThreadPool();
    virtual void patchTuringOptionsInfo(
            const isearch::config::TuringOptionsInfo &turingOptionsInfo,
            autil::legacy::json::JsonMap &jsonMap);
    std::string convertSorterConf();
    std::string convertFunctionConf();
private:
    isearch::service::SearcherResourcePtr createSearcherResource(const std::string &indexRoot);
    bool getMainTableInfo(std::string &schemaName, FullIndexVersion &fullVersion,
                          std::string &indexRoot);
    bool getRange(uint32_t partCount, uint32_t partId, autil::PartitionRange &range);
    proto::PartitionID suezPid2HaPid(const suez::PartitionId& sPid);
    bool getIndexPartition(
            const std::string& tableName,
            const proto::Range &range,
            const suez::TableReader &tableReader,
            std::pair<proto::PartitionID, indexlib::partition::IndexPartitionPtr> &table);
    bool initTimeoutTerminator(int64_t currentTime);
    void clearSnapshot();
protected:
    config::ConfigAdapterPtr _configAdapter;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SearcherBiz> SearcherBizPtr;

} // namespace turing
} // namespace isearch
