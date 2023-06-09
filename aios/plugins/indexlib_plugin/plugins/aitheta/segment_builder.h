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
#ifndef __INDEXLIB_PLUGIN_PLUGINS_AITHETA_SEGMENT_BUILDER_H
#define __INDEXLIB_PLUGIN_PLUGINS_AITHETA_SEGMENT_BUILDER_H

#include <unordered_map>

#include "indexlib/common_define.h"
#include "indexlib/misc/log.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"
#include "indexlib_plugin/plugins/aitheta/util/metric_reporter.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_config.h"
#include "indexlib_plugin/plugins/aitheta/segment.h"

namespace indexlib {
namespace aitheta_plugin {

class SegmentBuilder {
 public:
    SegmentBuilder(const SchemaParameter& schemaParameter) : mSchemaParam(schemaParameter), mIsBuilt(false) {}
    virtual ~SegmentBuilder() = default;

 public:
    virtual bool Init() { return true; }
    virtual bool Build(int64_t categeory, docid_t docid, const EmbeddingPtr embedding) = 0;
    virtual bool Dump(const indexlib::file_system::DirectoryPtr& indexDir) = 0;

 public:
    virtual size_t EstimateCurrentMemoryUse() = 0;
    virtual size_t EstimateTempMemoryUseInDump() = 0;
    virtual size_t EstimateDumpFileSize() = 0;

 public:
    void SetBuildDirectory(const indexlib::file_system::DirectoryPtr& dir) {
        assert(dir);
        mBuildDir = dir;
    }
    void SetMetricReporter(const MetricReporterPtr& reporter) {
        assert(reporter);
        _metricReporter = reporter;
    }
    void SetIndexName(const std::string& indexName) {
        assert(!indexName.empty());
        mIndexName = indexName;
    }

 protected:
    SchemaParameter mSchemaParam;
    indexlib::file_system::DirectoryPtr mBuildDir;
    bool mIsBuilt;
    std::string mIndexName;
    MetricReporterPtr _metricReporter;

    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(SegmentBuilder);

}
}

#endif  //__INDEXLIB_PLUGIN_PLUGINS_AITHETA_SEGMENT_BUILDER_H
