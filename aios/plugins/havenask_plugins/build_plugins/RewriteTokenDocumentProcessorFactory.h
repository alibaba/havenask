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
#include <tr1/memory>
#include <build_service/processor/DocumentProcessorFactory.h>
#include "RewriteTokenDocumentProcessor.h"

namespace pluginplatform {
namespace processor_plugins {

class RewriteTokenDocumentProcessorFactory : public build_service::processor::DocumentProcessorFactory
{
public:
    virtual ~RewriteTokenDocumentProcessorFactory() {}
public:
    virtual bool init(const build_service::KeyValueMap& parameters);
    virtual void destroy();
    virtual RewriteTokenDocumentProcessor* createDocumentProcessor(const std::string& processorName);
};

typedef std::tr1::shared_ptr<RewriteTokenDocumentProcessorFactory> RewriteTokenDocumentProcessorFactoryPtr;

}}
