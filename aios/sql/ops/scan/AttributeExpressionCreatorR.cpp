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
#include "sql/ops/scan/AttributeExpressionCreatorR.h"

#include <stddef.h>
#include <vector>

#include "autil/mem_pool/Pool.h"
#include "ha3/search/IndexPartitionReaderUtil.h"
#include "indexlib/partition/partition_reader_snapshot.h"
#include "matchdoc/MatchDocAllocator.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/proto/KernelDef.pb.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "sql/common/Log.h"
#include "sql/common/TracerAdapter.h"
#include "sql/common/common.h"
#include "sql/ops/scan/Collector.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/cava/common/SuezCavaAllocator.h"
#include "suez/turing/expression/function/FunctionInterfaceCreator.h"
#include "suez/turing/navi/TableInfoR.h"

namespace navi {
class ResourceInitContext;
} // namespace navi
namespace suez {
namespace turing {
class VirtualAttribute;
} // namespace turing
} // namespace suez

namespace sql {

const std::string AttributeExpressionCreatorR::RESOURCE_ID = "scan_attribute_expression_creator_r";

AttributeExpressionCreatorR::AttributeExpressionCreatorR() {}

AttributeExpressionCreatorR::~AttributeExpressionCreatorR() {}

void AttributeExpressionCreatorR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_KERNEL);
}

bool AttributeExpressionCreatorR::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode AttributeExpressionCreatorR::init(navi::ResourceInitContext &ctx) {
    if (!initExpressionCreator()) {
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

bool AttributeExpressionCreatorR::initExpressionCreator() {
    _tableInfo = _scanR->_tableInfoR->getTableInfo(_scanInitParamR->tableName);
    if (!_tableInfo) {
        SQL_LOG(ERROR, "table [%s] not exist.", _scanInitParamR->tableName.c_str());
        return false;
    }
    _indexPartitionReaderWrapper
        = isearch::search::IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
            _scanR->partitionReaderSnapshot.get(), _scanInitParamR->tableName);
    if(!_indexPartitionReaderWrapper){
        SQL_LOG(ERROR, "initExpressionCreator _indexPartitionReaderWrapper is null");
        return false;
    }
    _indexPartitionReaderWrapper->setSessionPool(_queryMemPoolR->getPool().get());
    if (!createExpressionCreator()) {
        return false;
    }
    return true;
}

bool AttributeExpressionCreatorR::initMatchDocAllocator() {
    const auto &tableType = _scanInitParamR->tableType;
    if (SCAN_KV_TABLE_TYPE == tableType || SCAN_KKV_TABLE_TYPE == tableType
        || SCAN_AGGREGATION_TABLE_TYPE == tableType) {
        _matchDocAllocator = CollectorUtil::createMountedMatchDocAllocator(
            _indexPartitionReaderWrapper->getSchema(), _scanR->graphMemoryPoolR->getPool());
        return false;
    } else {
        bool useSubFlag = _useSubR->getUseSub() && _tableInfo->hasSubSchema();
        _matchDocAllocator.reset(
            new matchdoc::MatchDocAllocator(_scanR->graphMemoryPoolR->getPool(), useSubFlag));
        return true;
    }
}

bool AttributeExpressionCreatorR::createExpressionCreator() {
    if (!initMatchDocAllocator()) {
        return true;
    }
    const auto &tableInfoWithRel = _scanR->_tableInfoR->getTableInfoWithRel();
    if (!_scanInitParamR->auxTableName.empty() && !tableInfoWithRel) {
        SQL_LOG(ERROR, "relation table info not exist.");
        return false;
    }
    auto pool = _queryMemPoolR->getPool().get();
    _functionProvider.reset(
        new suez::turing::FunctionProvider(_matchDocAllocator.get(),
                                           pool,
                                           _suezCavaAllocatorR->getAllocator().get(),
                                           _traceAdapterR->getTracer().get(),
                                           _scanR->partitionReaderSnapshot.get(),
                                           NULL));
    std::vector<suez::turing::VirtualAttribute *> virtualAttributes;
    _attributeExpressionCreator.reset(new suez::turing::AttributeExpressionCreator(
        pool,
        _matchDocAllocator.get(),
        _scanInitParamR->tableName,
        _scanR->partitionReaderSnapshot.get(),
        _scanInitParamR->auxTableName.empty() ? _tableInfo : tableInfoWithRel,
        virtualAttributes,
        _functionInterfaceCreatorR->getCreator().get(),
        _cavaPluginManagerR->getManager().get(),
        _functionProvider.get(),
        _tableInfo->getSubTableName()));
    return true;
}

REGISTER_RESOURCE(AttributeExpressionCreatorR);

} // namespace sql
