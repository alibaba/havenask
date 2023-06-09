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
#ifndef ISEARCH_BS_READERMODULEFACTORY_H
#define ISEARCH_BS_READERMODULEFACTORY_H

#include "build_service/plugin/ModuleFactory.h"
#include "build_service/util/Log.h"

namespace build_service { namespace reader {

class RawDocumentReader;

class ReaderModuleFactory : public plugin::ModuleFactory
{
public:
    ReaderModuleFactory() {}
    virtual ~ReaderModuleFactory() {}

private:
    ReaderModuleFactory(const ReaderModuleFactory&);
    ReaderModuleFactory& operator=(const ReaderModuleFactory&);

public:
    virtual bool init(const KeyValueMap& parameters) = 0;
    virtual void destroy() = 0;
    virtual RawDocumentReader* createRawDocumentReader(const std::string& readerName) = 0;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ReaderModuleFactory);
}} // namespace build_service::reader

#endif // ISEARCH_BS_TOKENIZERMODULEFACTORY_H
