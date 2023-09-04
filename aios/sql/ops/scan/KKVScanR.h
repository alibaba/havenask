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

#include "indexlib/partition/index_partition_reader.h"
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "sql/ops/calc/CalcTableR.h"
#include "sql/ops/scan/AttributeExpressionCreatorR.h"
#include "sql/ops/scan/Collector.h"
#include "sql/ops/scan/ScanBase.h"
#include "sql/ops/scan/ScanInitParamR.h"
#include "sql/ops/scan/ScanR.h"
#include "table/Table.h"

namespace matchdoc {
class MatchDoc;
class ReferenceBase;
} // namespace matchdoc
namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace indexlib::config {
class LegacySchemaAdapter;
}

namespace sql {

class KKVScanR : public ScanBase {
public:
    KKVScanR();
    ~KKVScanR();

private:
    KKVScanR(const KKVScanR &);
    KKVScanR &operator=(const KKVScanR &);

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    static const std::string RESOURCE_ID;

private:
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
    RESOURCE_DEPEND_DECLARE_BASE(ScanBase);

private:
    RESOURCE_DEPEND_ON(ScanR, _scanR);
    RESOURCE_DEPEND_ON(CalcTableR, _calcTableR);
    RESOURCE_DEPEND_ON(AttributeExpressionCreatorR, _attributeExpressionCreatorR);
    indexlib::index::KKVReaderPtr _kkvReader;
    std::shared_ptr<indexlib::config::LegacySchemaAdapter> _schema;
    std::vector<std::string> _rawPks;
    KeyCollectorPtr _pkeyCollector;
    ValueCollectorPtr _valueCollector;
    bool _requirePk = true;
    bool _canReuseAllocator;
};

NAVI_TYPEDEF_PTR(KKVScanR);

} // namespace sql
