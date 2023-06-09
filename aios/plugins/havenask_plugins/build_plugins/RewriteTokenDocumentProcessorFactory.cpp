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
#include "RewriteTokenDocumentProcessorFactory.h"

using namespace std;

namespace pluginplatform {
namespace processor_plugins {

bool RewriteTokenDocumentProcessorFactory::init(const build_service::KeyValueMap& parameters)
{
    return true;
}

void RewriteTokenDocumentProcessorFactory::destroy()
{
    delete this;
}
 
RewriteTokenDocumentProcessor* RewriteTokenDocumentProcessorFactory::createDocumentProcessor(
        const string& processorName)
{
    RewriteTokenDocumentProcessor* processor = new RewriteTokenDocumentProcessor;
    return processor;
}


extern "C"
build_service::util::ModuleFactory* createFactory() 
{
    return new RewriteTokenDocumentProcessorFactory;
}

extern "C"
void destroyFactory(build_service::util::ModuleFactory* factory) 
{
    factory->destroy();
}

}}
