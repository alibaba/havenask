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
#ifndef ISEARCH_EXPRESSION_FUNCTIONINFO_H
#define ISEARCH_EXPRESSION_FUNCTIONINFO_H

#include "expression/common.h"
#include "autil/legacy/jsonizable.h"

namespace expression {

class FunctionInfo : public autil::legacy::Jsonizable
{
public:
    FunctionInfo();
    ~FunctionInfo();
public:
    const std::string& getFuncName() const {
        return _funcName;
    }
    void setFuncName(const std::string &funcName) {
        _funcName = funcName;
    }

    const KeyValueMap &getParams() const;
    void setParams(const KeyValueMap &params);

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool operator==(const FunctionInfo &other) const;
private:
    std::string _funcName;
    KeyValueMap _params;
private:
    AUTIL_LOG_DECLARE();
};

}

#endif //ISEARCH_EXPRESSION_FUNCTIONINFO_H
