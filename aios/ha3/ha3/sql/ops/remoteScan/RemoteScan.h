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

#include "ha3/sql/ops/remoteScan/Connector.h"
#include "ha3/sql/ops/scan/ScanBase.h"
#include "multi_call/interface/SearchService.h"

namespace navi {
class GigQuerySessionR;
}

namespace table {
class Table;
}

namespace isearch {
namespace sql {

class RemoteScan : public ScanBase {
public:
    RemoteScan(const ScanInitParam &param, const navi::GigQuerySessionR *gigQuerySessionR);
    ~RemoteScan();

private:
    bool doInit() override;
    bool doBatchScan(table::TablePtr &table, bool &eof) override;
    bool doUpdateScanQuery(const StreamQueryPtr &inputQuery) override;

private:
    std::unique_ptr<Connector> _connector;
    std::vector<std::string> _pks;
    std::shared_ptr<multi_call::QuerySession> _querySession;
    kmonitor::MetricsReporter *_metricsReporter = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

}
}

