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

#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "sql/ops/scan/ScanInitParamR.h"
#include "sql/ops/scan/ScanR.h"
#include "sql/ops/scan/UseSubR.h"
#include "sql/resource/TraceAdapterR.h"
#include "suez/turing/expression/cava/common/CavaPluginManagerR.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/function/FunctionInterfaceCreatorR.h"
#include "suez/turing/expression/provider/FunctionProvider.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "suez/turing/navi/QueryMemPoolR.h"
#include "suez/turing/navi/SuezCavaAllocatorR.h"

namespace matchdoc {
class MatchDocAllocator;
} // namespace matchdoc
namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace sql {

class AttributeExpressionCreatorR : public navi::Resource {
public:
    AttributeExpressionCreatorR();
    ~AttributeExpressionCreatorR();
    AttributeExpressionCreatorR(const AttributeExpressionCreatorR &) = delete;
    AttributeExpressionCreatorR &operator=(const AttributeExpressionCreatorR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

private:
    bool initExpressionCreator();
    bool createExpressionCreator();

public:
    bool initMatchDocAllocator();

public:
    static const std::string RESOURCE_ID;

private:
    RESOURCE_DEPEND_DECLARE();

public:
    RESOURCE_DEPEND_ON(ScanR, _scanR);
    RESOURCE_DEPEND_ON(ScanInitParamR, _scanInitParamR);
    RESOURCE_DEPEND_ON(UseSubR, _useSubR);
    RESOURCE_DEPEND_ON(suez::turing::QueryMemPoolR, _queryMemPoolR);
    RESOURCE_DEPEND_ON(suez::turing::SuezCavaAllocatorR, _suezCavaAllocatorR);
    RESOURCE_DEPEND_ON(TraceAdapterR, _traceAdapterR);
    RESOURCE_DEPEND_ON(suez::turing::FunctionInterfaceCreatorR, _functionInterfaceCreatorR);
    RESOURCE_DEPEND_ON(suez::turing::CavaPluginManagerR, _cavaPluginManagerR);
    isearch::search::IndexPartitionReaderWrapperPtr _indexPartitionReaderWrapper;
    suez::turing::TableInfoPtr _tableInfo;
    std::shared_ptr<matchdoc::MatchDocAllocator> _matchDocAllocator;
    suez::turing::FunctionProviderPtr _functionProvider;
    suez::turing::AttributeExpressionCreatorPtr _attributeExpressionCreator;
};

NAVI_TYPEDEF_PTR(AttributeExpressionCreatorR);

} // namespace sql
