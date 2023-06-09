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
#include "CustomReaderModuleFactory.h"
#include "KafkaRawDocumentReader.h"

using namespace build_service::reader;

namespace pluginplatform {
namespace reader_plugins {

BS_LOG_SETUP(reader_plugins, CustomReaderModuleFactory);

RawDocumentReader* CustomReaderModuleFactory::createRawDocumentReader(
        const std::string &readerName)
{
    if ( readerName == "kafka" ){
        BS_LOG(INFO, "create reader[%s] succuss", readerName.c_str());
        return new KafkaRawDocumentReader;
    } else {
        BS_LOG(ERROR, "not supported reader[%s]", readerName.c_str());
        return nullptr;
    }
}

extern "C" build_service::plugin::ModuleFactory* createFactory_Reader()
{
    return (build_service::plugin::ModuleFactory*) new (std::nothrow) pluginplatform::reader_plugins::CustomReaderModuleFactory();
}

extern "C" void destroyFactory_Reader(build_service::plugin::ModuleFactory *factory)
{
    factory->destroy();
}

}
}
