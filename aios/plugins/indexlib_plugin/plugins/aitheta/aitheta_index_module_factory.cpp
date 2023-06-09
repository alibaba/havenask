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
#include "indexlib_plugin/plugins/aitheta/aitheta_index_module_factory.h"
#include "indexlib_plugin/plugins/aitheta/aitheta_index_reducer.h"
#include "indexlib_plugin/plugins/aitheta/aitheta_index_retriever.h"
#include "indexlib_plugin/plugins/aitheta/aitheta_index_segment_retriever.h"
#include "indexlib_plugin/plugins/aitheta/aitheta_indexer.h"

using namespace std;
using namespace indexlib::index;
using namespace indexlib::util;

namespace indexlib {
namespace aitheta_plugin {

void AithetaIndexModuleFactory::destroy() { delete this; }
IndexRetriever *AithetaIndexModuleFactory::createIndexRetriever(const KeyValueMap &parameters) {
    return new AithetaIndexRetriever(parameters);
}

Indexer *AithetaIndexModuleFactory::createIndexer(const KeyValueMap &parameters) {
    return new AithetaIndexer(parameters);
}

IndexReducer *AithetaIndexModuleFactory::createIndexReducer(const KeyValueMap &parameters) {
    return new AithetaIndexReducer(parameters);
}

IndexSegmentRetriever *AithetaIndexModuleFactory::createIndexSegmentRetriever(const KeyValueMap &parameters) {
    return new AithetaIndexSegmentRetriever(parameters);
}

extern "C" plugin::ModuleFactory *createFactory_Index() { return new AithetaIndexModuleFactory(); }

extern "C" void destroyFactory_Index(plugin::ModuleFactory *factory) { factory->destroy(); }

}
}
