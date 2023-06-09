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
#include "indexlib/index/kkv/kkv_reader.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "table/Table.h"

namespace indexlib::config {
class LegacySchemaAdapter;
}

namespace isearch {
namespace sql {

class CalcTable;

class KKVScan : public ScanBase {
public:
    KKVScan(const ScanInitParam &param, bool requirePk = true);
    virtual ~KKVScan();

private:
    KKVScan(const KKVScan &);
    KKVScan &operator=(const KKVScan &);

private:
    bool doInit() override;
    bool doBatchScan(table::TablePtr &table, bool &eof) override;
    bool doUpdateScanQuery(const StreamQueryPtr &inputQuery) override;
    bool prepareKKVReader();
    void filterPks(const std::vector<std::string> &pks);
    bool parseQuery();
    bool prepareFields();
    bool fillMatchDocs(std::vector<matchdoc::MatchDoc> &matchDocs);

private:
    typedef std::vector<matchdoc::ReferenceBase *> ValueReferences;

private:
    indexlib::index::KKVReaderPtr _kkvReader;
    std::shared_ptr<indexlib::config::LegacySchemaAdapter> _schema;
    std::vector<std::string> _rawPks;
    KeyCollectorPtr _pkeyCollector;
    ValueCollectorPtr _valueCollector;
    std::unique_ptr<CalcTable> _calcTable;
    bool _requirePk;
    bool _canReuseAllocator;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<KKVScan> KKVScanPtr;
} // namespace sql
} // namespace isearch
