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
#ifndef __INDEXLIB_AITHETA_INDEXER_H
#define __INDEXLIB_AITHETA_INDEXER_H

#include <map>
#include <memory>
#include <unordered_set>
#include <vector>

#include <aitheta/index_params.h>
#include "indexlib/common_define.h"
#include "indexlib/misc/log.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer.h"
#include "indexlib/indexlib.h"

#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/segment_builder.h"

namespace indexlib {
namespace aitheta_plugin {

class AithetaIndexer : public indexlib::index::Indexer {
 public:
    AithetaIndexer(const util::KeyValueMap &parameters) : Indexer(parameters), mSchemaParam(parameters) {}
    ~AithetaIndexer() = default;

 public:
    bool DoInit(const IndexerResourcePtr &resource) override;
    bool Build(const std::vector<const document::Field *> &fieldVec, docid_t docid) override;
    bool Dump(const indexlib::file_system::DirectoryPtr &indexDir) override;

    uint32_t GetDistinctTermCount() const override { return 0u; }
    // EstimateInitMemoryUse is called before mBuilder's initializtion
    size_t EstimateInitMemoryUse(size_t lastSegmentDistinctTermCount) const override { return 0ul; }
    int64_t EstimateCurrentMemoryUse() const override {
        if (!mBuilder) {
            return 0;
        }
        return mBuilder->EstimateCurrentMemoryUse();
    }
    int64_t EstimateExpandMemoryUseInDump() const override { return 0l; }
    int64_t EstimateTempMemoryUseInDump() const override {
        if (!mBuilder) {
            return 0;
        }
        return mBuilder->EstimateTempMemoryUseInDump();
    }
    int64_t EstimateDumpFileSize() const override {
        if (!mBuilder) {
            return 0;
        }
        return mBuilder->EstimateDumpFileSize();
    }

    indexlib::index::InMemSegmentRetrieverPtr CreateInMemSegmentRetriever() const override;

 private:
    bool ParseCatid(const document::Field *field, std::vector<int64_t> &catIds);
    bool ParseEmbedding(const document::Field *field, EmbeddingPtr &embedding);
    bool ParseKey(const document::Field *field, int64_t &key);

 private:
    SchemaParameter mSchemaParam;
    SegmentBuilderPtr mBuilder;
    MetricReporterPtr _metricReporter;
    IndexerResourcePtr mIndexerResource;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AithetaIndexer);

}
}

#endif  //__INDEXLIB_AITHETA_INDEXER_H
