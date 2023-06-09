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
#ifndef __INDEXLIB_AITHETA_INDEX_MODULE_FACTORY_H
#define __INDEXLIB_AITHETA_INDEX_MODULE_FACTORY_H

#include "indexlib/index/normal/inverted_index/customized_index/index_module_factory.h"

namespace indexlib {
namespace aitheta_plugin {

class AithetaIndexModuleFactory : public indexlib::index::IndexModuleFactory {
 public:
    AithetaIndexModuleFactory() = default;
    ~AithetaIndexModuleFactory() = default;

 public:
    void destroy();
    indexlib::index::IndexSegmentRetriever *createIndexSegmentRetriever(const util::KeyValueMap &parameters);
    indexlib::index::Indexer *createIndexer(const util::KeyValueMap &parameters);
    indexlib::index::IndexReducer *createIndexReducer(const util::KeyValueMap &parameters);
    indexlib::index::IndexRetriever *createIndexRetriever(const util::KeyValueMap &parameters);

 private:
    util::KeyValueMap _parameters;
};

DEFINE_SHARED_PTR(AithetaIndexModuleFactory);

}
}

#endif  //__INDEXLIB_AITHETA_INDEX_MODULE_FACTORY_H
