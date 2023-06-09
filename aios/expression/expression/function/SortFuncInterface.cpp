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
#include "expression/function/SortFuncInterface.h"
#include "expression/function/ArgumentAttributeExpression.h"

using namespace std;

namespace expression {
AUTIL_LOG_SETUP(expression, SortFuncInterface);
AUTIL_LOG_SETUP(expression, SortFuncInterfaceCreator);

bool SortFuncInterfaceCreator::init(
        const KeyValueMap &funcParameter,
        const resource_reader::ResourceReaderPtr &resourceReader)
{
    return true;
}

FunctionInterface *SortFuncInterfaceCreator::createFunction(
        const AttributeExpressionVec& funcSubExprVec,
        AttributeExpressionCreator *creator)
{
    if (funcSubExprVec.size() != 1) {
        AUTIL_LOG(WARN, "argument invalid");
        return NULL;
    }

    ArgumentAttributeExpression *arg =
        dynamic_cast<ArgumentAttributeExpression*>(funcSubExprVec[0]);
    if (!arg) {
        AUTIL_LOG(WARN, "argument invalid, expr type is: %d", funcSubExprVec[0]->getType());
        return NULL;
    }

    string sortFieldName;
    if (!arg->getConstValue<string>(sortFieldName)) {
        AUTIL_LOG(WARN, "get sortFieldName failed");
        return NULL;
    }
    if (sortFieldName.empty()) {
        AUTIL_LOG(WARN, "sortFieldName should not empty");
        return NULL;
    }

    string tableName;
    string fieldName = sortFieldName;
    size_t pos = sortFieldName.find(':');
    if (pos != string::npos) {
        tableName = sortFieldName.substr(0, pos);
        fieldName = sortFieldName.substr(pos + 1);
    }
    char flag = fieldName[0];
    bool asc = flag == '+';
    if (flag == '+' || flag == '-') {
        fieldName = fieldName.substr(1);
    }
    if (!tableName.empty()) {
        sortFieldName = tableName + ':' + fieldName;
    } else {
        sortFieldName = fieldName;
    }
    
    return new SortFuncInterface(asc, sortFieldName);
}

}
