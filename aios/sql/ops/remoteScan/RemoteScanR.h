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

#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "navi/resource/QuerySessionR.h"
#include "sql/framework/PushDownOp.h"
#include "sql/ops/calc/CalcTableR.h"
#include "sql/ops/scan/ScanBase.h"
#include "sql/ops/scan/ScanInitParamR.h"
#include "sql/ops/scan/ScanR.h"

namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace sql {
class Connector;

class RemoteScanR : public ScanBase {
public:
    RemoteScanR();
    ~RemoteScanR();

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    static const std::string RESOURCE_ID;

private:
    bool doBatchScan(table::TablePtr &table, bool &eof) override;
    bool doUpdateScanQuery(const StreamQueryPtr &inputQuery) override;

private:
    RESOURCE_DEPEND_DECLARE_BASE(ScanBase);

private:
    RESOURCE_DEPEND_ON(ScanR, _scanR);
    RESOURCE_DEPEND_ON(CalcTableR, _calcTableR);
    RESOURCE_DEPEND_ON(navi::QuerySessionR, _querySessionR);
    std::unique_ptr<Connector> _connector;
    std::vector<std::string> _pks;
};

} // namespace sql
