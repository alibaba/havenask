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
#ifndef UDF_PLUGINS__H
#define UDF_PLUGINS__H

#include <ha3/isearch.h>
#include <autil/Log.h>
#include <suez/turing/expression/function/SyntaxExpressionFactory.h>

namespace pluginplatform {
namespace udf_plugins {

class FunctionPluginFactory : public suez::turing::SyntaxExpressionFactory
{
public:
    FunctionPluginFactory();
    ~FunctionPluginFactory();
public:
    /* override */ bool init(const KeyValueMap &parameters);
    /* override */ bool registeFunctions();
private:
    std::set<std::string> _funcNames;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FunctionPluginFactory> FunctionPluginFactoryPtr;

}}

#endif //UDF_PLUGINS__H