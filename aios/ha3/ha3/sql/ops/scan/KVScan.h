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

#include "ha3/proto/BasicDefs.pb.h"
#include "ha3/sql/ops/scan/Collector.h"
#include "ha3/sql/ops/scan/ScanBase.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "table/Table.h"

namespace navi {
class AsyncPipe;
typedef std::shared_ptr<AsyncPipe> AsyncPipePtr;
} // namespace navi
namespace isearch {
namespace sql {
class AsyncKVLookupCallbackCtx;
class CalcTable;
} // end namespace sql
} // end namespace isearch
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
namespace table {
class KVReader;
} // namespace index
} // namespace indexlibv2

namespace isearch {
namespace sql {

struct KVLookupOption;

class KVScan : public ScanBase {
public:
    KVScan(const ScanInitParam &param, navi::AsyncPipePtr pipe, bool requirePk = true);
    virtual ~KVScan();

private:
    KVScan(const KVScan &);
    KVScan &operator=(const KVScan &);

private:
    bool doInit() override;
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
    navi::AsyncPipePtr _asyncPipe;
    std::shared_ptr<AsyncKVLookupCallbackCtx> _lookupCtx;
    std::shared_ptr<void> _kvReader;
    bool _requirePk;
    bool _canReuseAllocator;
    std::vector<std::string> _rawPks;
    std::shared_ptr<indexlibv2::config::ITabletSchema> _schema;
    KeyCollectorPtr _pkeyCollector;
    ValueCollectorPtr _valueCollector;
    const suez::turing::IndexInfo *_indexInfo = nullptr;
    std::string _indexName;
    kmonitor::MetricsReporter *_opMetricsReporter = nullptr;
    std::unique_ptr<CalcTable> _calcTable;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<KVScan> KVScanPtr;
} // namespace sql
} // namespace isearch
