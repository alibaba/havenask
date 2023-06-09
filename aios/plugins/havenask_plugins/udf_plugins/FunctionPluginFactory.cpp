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
#include "SumFuncInterface.h"
#include <autil/StringTokenizer.h>
#include "FunctionPluginFactory.h"

using namespace std;
using namespace autil;
using namespace suez::turing;

namespace pluginplatform {
namespace udf_plugins {
AUTIL_LOG_SETUP(udf_plugins, FunctionPluginFactory);

FunctionPluginFactory::FunctionPluginFactory() {
}

FunctionPluginFactory::~FunctionPluginFactory() {
}

bool FunctionPluginFactory::init(const KeyValueMap &parameters) {
    REGISTE_FUNCTION_CREATOR(SumFuncInterfaceCreatorImpl);
    return true;
}

bool FunctionPluginFactory::registeFunctions() {
    return true;
}

// for sql functions
extern "C"
build_service::plugin::ModuleFactory* createFactory() {
    return new FunctionPluginFactory;
}

extern "C"
void destroyFactory(build_service::plugin::ModuleFactory *factory) {
    factory->destroy();
}

}}