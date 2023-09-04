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

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "sql/ops/calc/CalcTableR.h"
#include "sql/ops/scan/AttributeExpressionCreatorR.h"
#include "sql/ops/scan/Collector.h"
#include "sql/ops/scan/ScanBase.h"
#include "sql/ops/scan/ScanInitParamR.h"
#include "sql/ops/scan/ScanR.h"
#include "sql/resource/TabletManagerR.h"
#include "suez_navi/resource/AsyncExecutorR.h"
#include "table/Table.h"

namespace kmonitor {
class MetricsReporter;
} // namespace kmonitor
namespace matchdoc {
class MatchDoc;
class ReferenceBase;
} // namespace matchdoc

namespace navi {
class AsyncPipe;
class ResourceDefBuilder;
class ResourceInitContext;

typedef std::shared_ptr<AsyncPipe> AsyncPipePtr;
} // namespace navi
namespace sql {
class AsyncKVLookupCallbackCtx;
} // end namespace sql

namespace suez {
namespace turing {
class IndexInfo;
} // namespace turing
} // namespace suez

namespace indexlib {
namespace index {
class KVReader;
} // namespace index
} // namespace indexlib

namespace indexlibv2 {
namespace config {
class ITabletSchema;
}
} // namespace indexlibv2

namespace sql {

struct KVLookupOption;

class KVScanR : public ScanBase {
public:
    KVScanR();
    virtual ~KVScanR();

private:
    KVScanR(const KVScanR &);
    KVScanR &operator=(const KVScanR &);

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    static const std::string RESOURCE_ID;

private:
    bool initAsyncParam();
    bool doBatchScan(table::TablePtr &table, bool &eof) override;
    bool doUpdateScanQuery(const StreamQueryPtr &inputQuery) override;
    bool prepareIndexInfo();
    bool prepareLookUpCtx();
    KVLookupOption prepareLookupOption();
    virtual void startLookupCtx();
    bool parseQuery();
    bool prepareFields();
    bool prepareBatchMatchDocAllocator();
    void onBatchScanFinish() override;
    bool fillMatchDocs(std::vector<matchdoc::MatchDoc> &matchDocs);
    std::shared_ptr<indexlib::index::KVReader> prepareKVReaderV1();
    bool prepareKvReaderV2Schema();

private:
    typedef std::vector<matchdoc::ReferenceBase *> ValueReferences;

private:
    RESOURCE_DEPEND_DECLARE_BASE(ScanBase);

private:
    RESOURCE_DEPEND_ON(ScanR, _scanR);
    RESOURCE_DEPEND_ON(CalcTableR, _calcTableR);
    RESOURCE_DEPEND_ON(AttributeExpressionCreatorR, _attributeExpressionCreatorR);
    RESOURCE_DEPEND_ON(suez_navi::AsyncExecutorR, _asyncExecutorR);
    RESOURCE_DEPEND_ON(TabletManagerR, _tabletManagerR);
    std::shared_ptr<AsyncKVLookupCallbackCtx> _lookupCtx;
    std::shared_ptr<void> _kvReader;
    bool _requirePk = true;
    bool _canReuseAllocator = false;
    bool _asyncMode = true;
    std::vector<std::string> _rawPks;
    std::shared_ptr<indexlibv2::config::ITabletSchema> _schema;
    KeyCollectorPtr _pkeyCollector;
    ValueCollectorPtr _valueCollector;
    const suez::turing::IndexInfo *_indexInfo = nullptr;
    std::string _indexName;
    kmonitor::MetricsReporter *_opMetricsReporter = nullptr;
};

NAVI_TYPEDEF_PTR(KVScanR);

} // namespace sql
