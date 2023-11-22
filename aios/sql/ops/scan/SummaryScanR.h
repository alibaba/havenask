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
#include <stdint.h>
#include <string>
#include <vector>

#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "indexlib/base/Types.h"
#include "indexlib/indexlib.h"
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "sql/ops/calc/CalcTableR.h"
#include "sql/ops/scan/AttributeExpressionCreatorR.h"
#include "sql/ops/scan/ScanBase.h"
#include "sql/ops/scan/ScanInitParamR.h"
#include "sql/ops/scan/ScanR.h"
#include "suez/turing/expression/cava/common/CavaPluginManagerR.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/function/FunctionInterfaceCreatorR.h"
#include "suez_navi/resource/AsyncExecutorR.h"
#include "table/Table.h"

namespace indexlib {
namespace document {
class SearchSummaryDocument;
} // namespace document
} // namespace indexlib
namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace matchdoc {
class MatchDoc;
template <typename T>
class Reference;
} // namespace matchdoc
namespace suez {
namespace turing {
class AttributeExpression;
class SummaryInfo;
} // namespace turing
} // namespace suez
namespace sql {
class AsyncSummaryLookupCallbackCtx;
class CountedAsyncPipe;
} // end namespace sql

namespace sql {

class SummaryScanR : public ScanBase {
private:
    typedef std::vector<indexlib::document::SearchSummaryDocument *> SearchSummaryDocVecType;

public:
    SummaryScanR();
    virtual ~SummaryScanR();

private:
    SummaryScanR(const SummaryScanR &);
    SummaryScanR &operator=(const SummaryScanR &);

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    void startAsyncLookup() override;

private:
    bool doBatchScan(table::TablePtr &table, bool &eof) override;
    bool doUpdateScanQuery(const StreamQueryPtr &inputQuery) override;
    void initSummary();
    void initExtraSummary();
    bool genDocIdFromRawPks(std::vector<std::string> pks);
    bool parseQuery(std::vector<std::string> &pks, bool &needFilter);
    bool prepareFields();
    bool convertPK2DocId(const std::vector<std::string> &pks);

    bool prepareLookupCtxs();
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

public:
    static const std::string RESOURCE_ID;

private:
    RESOURCE_DEPEND_DECLARE_BASE(ScanBase);

private:
    RESOURCE_DEPEND_ON(ScanR, _scanR);
    RESOURCE_DEPEND_ON(CalcTableR, _calcTableR);
    RESOURCE_DEPEND_ON(suez::turing::FunctionInterfaceCreatorR, _functionInterfaceCreatorR);
    RESOURCE_DEPEND_ON(suez::turing::CavaPluginManagerR, _cavaPluginManagerR);
    RESOURCE_DEPEND_ON(AttributeExpressionCreatorR, _attributeExpressionCreatorR);
    RESOURCE_DEPEND_ON(suez_navi::AsyncExecutorR, _asyncExecutorR);
    std::vector<isearch::search::IndexPartitionReaderWrapperPtr> _indexPartitionReaderWrappers;
    std::vector<suez::turing::AttributeExpressionCreatorPtr> _attributeExpressionCreators;
    std::vector<docid_t> _docIds;
    std::vector<int> _tableIdx;
    std::vector<std::vector<suez::turing::AttributeExpression *>> _tableAttributes;
    std::vector<std::string> _usedFields;
    std::vector<std::shared_ptr<AsyncSummaryLookupCallbackCtx>> _lookupCtxs;
    std::shared_ptr<CountedAsyncPipe> _countedPipe;
    suez::turing::SummaryInfo *_summaryInfo = nullptr;
    bool _requirePk = true;
};

NAVI_TYPEDEF_PTR(SummaryScanR);

} // namespace sql
