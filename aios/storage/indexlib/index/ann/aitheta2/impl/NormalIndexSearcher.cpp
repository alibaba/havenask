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
#include "indexlib/index/ann/aitheta2/impl/NormalIndexSearcher.h"

#include "indexlib/index/ann/aitheta2/util/AithetaFactoryWrapper.h"
#include "indexlib/index/ann/aitheta2/util/EmbeddingDataExtractor.h"
#include "indexlib/index/ann/aitheta2/util/params_initializer/ParamsInitializerFactory.h"
using namespace std;
using namespace autil;

namespace indexlibv2::index::ann {

NormalIndexSearcher::NormalIndexSearcher(const IndexMeta& meta, const AithetaIndexConfig& config,
                                         const IndexDataReaderPtr& reader, const AiThetaContextHolderPtr& holder)
    : IndexSearcher(config, meta.searcherName, holder)
    , _indexMeta(meta)
    , _indexDataReader(reader)
{
}

NormalIndexSearcher::~NormalIndexSearcher()
{
    if (_indexSearcher) {
        _indexSearcher->unload();
    }
}

bool NormalIndexSearcher::Init()
{
    ANN_CHECK(AiThetaFactoryWrapper::CreateSearcher(_indexConfig, _indexMeta, _indexDataReader, _indexSearcher),
              "create normal index searcher failed");
    return InitMeasure(_indexSearcher->meta().measure_name());
}

bool NormalIndexSearcher::ParseQueryParameter(const std::string& searchParams, AiThetaParams& aiThetaParams) const
{
    auto initializer = ParamsInitializerFactory::Create(_searcherName, _indexMeta.docCount);
    ANN_CHECK(initializer, "create param initializer factory failed");

    AithetaIndexConfig newConfig = _indexConfig;
    newConfig.searchConfig.indexParams = searchParams;
    return initializer->InitSearchParams(newConfig, aiThetaParams);
}

bool NormalIndexSearcher::Search(const AithetaQuery& query, const std::shared_ptr<AithetaAuxSearchInfoBase>& searchInfo,
                                 ResultHolder& resultHolder)
{
    return SearchImpl(_indexSearcher, query, searchInfo, resultHolder);
}

AUTIL_LOG_SETUP(indexlib.index, NormalIndexSearcher);
} // namespace indexlibv2::index::ann
