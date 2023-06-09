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

#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/RangeUtil.h"
#include "ha3/isearch.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/ops/scan/ScanBase.h"
#include "indexlib/document/index_document/normal_document/search_summary_document.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "matchdoc/MatchDocAllocator.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "table/Table.h"

namespace indexlib {
namespace index {
class InvertedIndexReader;
template <typename Key>
class PrimaryKeyIndexReaderTyped;
} // namespace index
namespace partition {
class PartitionReaderSnapshot;
} // namespace partition
} // namespace indexlib
namespace matchdoc {
class MatchDoc;
template <typename T>
class Reference;
} // namespace matchdoc
namespace suez {
namespace turing {
class AttributeExpression;
class CavaPluginManager;
class FunctionInterfaceCreator;
class SummaryInfo;
} // namespace turing
} // namespace suez

namespace navi {
class AsyncPipe;
}
namespace isearch {
namespace sql {
class AsyncSummaryLookupCallbackCtx;
class CalcTable;
class CountedAsyncPipe;
} // end namespace sql
} // end namespace isearch

namespace isearch {
namespace sql {

class SummaryScan : public ScanBase {
private:
    typedef std::vector<indexlib::document::SearchSummaryDocument *> SearchSummaryDocVecType;

public:
    SummaryScan(const ScanInitParam &param, std::shared_ptr<navi::AsyncPipe> pipe, bool requirePk = true);
    virtual ~SummaryScan();

private:
    SummaryScan(const SummaryScan &);
    SummaryScan &operator=(const SummaryScan &);

private:
    bool doInit() override;
    bool doBatchScan(table::TablePtr &table, bool &eof) override;
    bool doUpdateScanQuery(const StreamQueryPtr &inputQuery) override;
    void initSummary();
    void initExtraSummary(
        const std::map<std::string, suez::turing::TableInfoPtr> *dependencyTableInfoMap,
        suez::turing::CavaPluginManager *cavaPluginManager,
        suez::turing::FunctionInterfaceCreator *functionInterfaceCreator,
        indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot);
    bool genDocIdFromRawPks(std::vector<std::string> pks);
    bool parseQuery(std::vector<std::string> &pks, bool &needFilter);
    bool prepareFields();
    bool convertPK2DocId(const std::vector<std::string> &pks);

    bool prepareLookupCtxs();
    virtual void startLookupCtxs();
    virtual bool getSummaryDocs(SearchSummaryDocVecType &summaryDocs);
    template <typename T>
    bool fillSummaryDocs(const matchdoc::Reference<T> *ref,
                         const SearchSummaryDocVecType &summaryDocs,
                         const std::vector<matchdoc::MatchDoc> &matchDocs,
                         int32_t summaryFieldId);
    virtual bool fillSummary(const std::vector<matchdoc::MatchDoc> &matchDocs);
    virtual bool fillAttributes(const std::vector<matchdoc::MatchDoc> &matchDocs);
    void adjustUsedFields();
    void onBatchScanFinish() override;
private:
    std::vector<search::IndexPartitionReaderWrapperPtr> _indexPartitionReaderWrappers;
    std::vector<suez::turing::AttributeExpressionCreatorPtr> _attributeExpressionCreators;
    std::vector<docid_t> _docIds;
    std::vector<int> _tableIdx;
    std::vector<std::vector<suez::turing::AttributeExpression *>> _tableAttributes;
    std::vector<std::string> _usedFields;
    std::unique_ptr<CalcTable> _calcTable;
    std::vector<std::shared_ptr<AsyncSummaryLookupCallbackCtx>> _lookupCtxs;
    std::shared_ptr<navi::AsyncPipe> _asyncPipe;
    std::shared_ptr<CountedAsyncPipe> _countedPipe;
    kmonitor::MetricsReporter *_opMetricsReporter = nullptr;
    suez::turing::SummaryInfo *_summaryInfo = nullptr;
    bool _requirePk;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SummaryScan> SummaryScanPtr;
} // namespace sql
} // namespace isearch
