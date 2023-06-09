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
#include "ha3/sql/ops/turbojetCalc/TurboJetCalc.h"

#include "ha3/sql/ops/turbojetCalc/IquanPlanImporter.h"
#include "ha3/sql/resource/MetaInfoResource.h"
#include "ha3/sql/resource/SqlBizResource.h"
#include "ha3/sql/resource/SqlQueryResource.h"
#include "navi/resource/GraphMemoryPoolResource.h"
#include "table/ValueTypeSwitch.h"
#include "turbojet/exec/Session.h"

using namespace std;
using namespace matchdoc;
using namespace turbojet;
using namespace turbojet::expr;

namespace isearch::sql::turbojet {

AUTIL_LOG_SETUP(sql, TurboJetCalc);

TurboJetCalc::TurboJetCalc(const CalcInitParam &param,
                           SqlBizResource *bizResource,
                           SqlQueryResource *queryResource,
                           MetaInfoResource *metaInfoResource,
                           navi::GraphMemoryPoolResource *memoryPoolResource)
    : _param(param)
    , _bizResource(bizResource)
    , _queryResource(queryResource)
    , _metaInfoResource(metaInfoResource)
    , _memoryPoolResource(memoryPoolResource) {}

bool TurboJetCalc::init() {
    (void)_queryResource;
    (void)_metaInfoResource;
    auto r = doInit();
    if (r.is_err()) {
        AUTIL_LOG(ERROR, "%s", r.get_error().get_stack_trace().c_str());
        return false;
    }
    return true;
}

bool TurboJetCalc::compute(table::TablePtr &table,
                           bool &isEof) {
    AUTIL_LOG(ERROR, "compute not implemented");
    return false;
}

Result<> TurboJetCalc::doInit() {
    AR_REQUIRE_TRUE(_bizResource, RuntimeError::make("bizResource == nullptr"));
    auto catalog = dynamic_cast<Catalog *>(_bizResource->getTurboJetCalcCompiler());
    AR_REQUIRE_TRUE(catalog, RuntimeError::make("turbojet runtime is not available"));
    AR_RET_IF_ERR(IquanPlanImporter::import(_param, _node, _outMap));
    AUTIL_LOG(DEBUG, "sink is [%s]", _node->ToString().c_str());
    _program = AR_RET_IF_ERR(Session(catalog->shared_from_this()).Compile(_node));
    return {};
}

Result<> TurboJetCalc::doCompute(suez::turing::FunctionProvider *functionProvider,
                                 table::TablePtr &table,
                                 bool &isEof) {

    int rowCount = (int)table->getRowCount();
    auto pool = _memoryPoolResource->getPool();

    auto inputAllocator = table->getMatchDocAllocatorPtr();
    auto inputDocs = table->getRows();

    auto outputAllocator = make_shared<matchdoc::MatchDocAllocator>(pool);
    auto outputDocs = outputAllocator->batchAllocate(rowCount);

    AR_REQUIRE_TRUE(!_param.outputFields.empty(),
                    RuntimeError::make("empty outputFields is not supported yet"));
    AR_REQUIRE_TRUE(_param.outputFields.size() == _param.outputFieldsType.size(),
                    RuntimeError::make("outputFields.size() != outputFieldsType.size()"));
    for (int i = 0; i < _param.outputFields.size(); ++i) {
        auto &name = _param.outputFields[i];
        auto &type = _param.outputFieldsType[i];

        bool isFromInputTable = !_outMap.count(name);

        bool isMulti = false;
        matchdoc::BuiltinType builtinType;

        matchdoc::ReferenceBase *inputRef;
        if (isFromInputTable) {
            inputRef = inputAllocator->findReferenceWithoutType(name);
            AR_REQUIRE_TRUE(inputRef,
                            RuntimeError::make("find reference [%s] failed", name.c_str()));
            auto valueType = inputRef->getValueType();
            builtinType = valueType.getBuiltinType();
            isMulti = valueType.isMultiValue();
        } else {
            builtinType = ExprUtil::transSqlTypeToVariableType(type).first;
        }

        auto func = [&](auto a) {
            using T = typename decltype(a)::value_type;
            auto ref = outputAllocator->declareWithConstructFlagDefaultGroup<T>(
                name, matchdoc::ConstructTypeTraits<T>::NeedConstruct(), SL_ATTRIBUTE);
            if (ref == nullptr) {
                AUTIL_LOG(ERROR, "declare reference [%s] failed", name.c_str());
                return false;
            }
            outputAllocator->extend();
            if (!isFromInputTable) {
                return true;
            }
            auto inputRef_ = dynamic_cast<matchdoc::Reference<T> *>(inputRef);
            if (!inputRef_) {
                AUTIL_LOG(ERROR, "dynamic_cast reference [%s] failed", name.c_str());
                return false;
            }
            for (int i = 0; i < rowCount; ++i) {
                ref->set(outputDocs[i], inputRef_->get(inputDocs[i]));
            }
            return true;
        };

        auto ok = table::ValueTypeSwitch::switchType(builtinType, isMulti, func, func);
        AR_REQUIRE_TRUE(ok, RuntimeError::make("unknown type [%s]", type.c_str()));
    }

    ResourceProvider rc {*pool};
    rc.len = rowCount;

    MatchDocsResource inputMatchDocs;
    inputMatchDocs.alloc = inputAllocator.get();
    inputMatchDocs.docs = inputDocs;
    AR_RET_IF_ERR(rc.Declare(MatchDocsResource::ID + 0, inputMatchDocs));

    MatchDocsResource outputMatchDocs = inputMatchDocs;
    outputMatchDocs.alloc = outputAllocator.get();
    outputMatchDocs.docs = outputDocs;
    AR_RET_IF_ERR(rc.Declare(MatchDocsResource::ID + 1, outputMatchDocs));

    SuezTuringResource suezTuring;
    suezTuring.function_provider = functionProvider;
    suezTuring.function_interface_creator = _bizResource->getFunctionInterfaceCreator();
    AR_RET_IF_ERR(rc.Declare(suezTuring));

    auto idxs = AR_RET_IF_ERR(_program->Execute(rc));
    if (_program->HasFilter()) {
        for (int i = 0; i < idxs.size(); ++i) {
            outputDocs[i] = outputDocs[idxs[i]];
        }
        outputDocs.resize(idxs.size());
    }

    auto outputTable = make_unique<table::Table>(outputDocs, outputAllocator);
    outputTable->mergeDependentPools(table);
    table = std::move(outputTable);

    return {};
}

} // namespace isearch::sql::turbojet
