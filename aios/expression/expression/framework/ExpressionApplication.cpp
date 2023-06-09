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
#include "expression/function/FunctionInterfaceManager.h"
#include "expression/framework/ExpressionApplication.h"

using namespace std;
using namespace autil;

namespace expression {
AUTIL_LOG_SETUP(expression, ExpressionApplication);

ExpressionApplication::ExpressionApplication(
       std::function<AtomicAttributeExpressionCreatorBase *(AttributeExpressionPool*, autil::mem_pool::Pool*, bool)> atomicCreator)
    : _atomicCreator(atomicCreator)
    , _virtualAttributes(NULL)
    , _functionInterfaceManager(NULL)
{
}

ExpressionApplication::~ExpressionApplication() {
    DELETE_AND_SET_NULL(_functionInterfaceManager);
    _virtualAttributes = NULL;
}

bool ExpressionApplication::registerVirtualAttributes(const SimpleValue *va) {
    if (!va->IsObject()) {
        AUTIL_LOG(ERROR, "virtual attribute invalid");
        return false;
    }
    _virtualAttributes = va;
    return true;
}

bool ExpressionApplication::registerFunctions(
        const FunctionConfig &funcConfig,
        const resource_reader::ResourceReaderPtr &resourceReaderPtr,
        FunctionInterfaceFactory *functionFactory)
{
    if (_functionInterfaceManager != NULL) {
        AUTIL_LOG(ERROR, "can not register twice");
        return false;
    }

    unique_ptr<FunctionInterfaceManager> fiManager(new FunctionInterfaceManager());
    if (!fiManager->init(funcConfig, resourceReaderPtr, functionFactory)) {
        AUTIL_LOG(ERROR, "init function failed");
        return false;
    }

    _functionInterfaceManager = fiManager.release();
    return true;
}

AttributeExpressionCreator *ExpressionApplication::create(
        bool useSub, autil::mem_pool::Pool* pool) const {
    unique_ptr<AttributeExpressionCreator> attrExprCreator(
        new AttributeExpressionCreator(_functionInterfaceManager, useSub, _atomicCreator, pool));

    if (_virtualAttributes == NULL) {
        return attrExprCreator.release();
    }

    assert(_virtualAttributes->IsObject());
    for (SimpleValue::ConstMemberIterator it = _virtualAttributes->MemberBegin();
         it != _virtualAttributes->MemberEnd(); it++)
    {
        const SimpleValue &name = it->name;
        const SimpleValue &value = it->value;

        string vaName(name.GetString(), name.GetStringLength());
        string vaExprStr(value.GetString(), value.GetStringLength());

        if (!attrExprCreator->registerVirtualAttribute(vaName, vaExprStr)) {
            AUTIL_LOG(WARN, "register virtual attribute [%s:%s] failed",
                      vaName.c_str(), vaExprStr.c_str());
            return NULL;
        }
    }
    return attrExprCreator.release();
}



}
