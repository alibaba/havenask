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
#pragma once

#include <set>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "autil/mem_pool/Pool.h"
#include "build_service/common/Locator.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common/ResourceKeeper.h"
#include "build_service/common_define.h"
#include "build_service/config/ProcessorConfigReader.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/SrcNodeConfig.h"
#include "build_service/document/RawDocument.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/index_document/normal_document/serialized_source_document.h"
#include "indexlib/document/index_document/normal_document/source_document.h"
#include "indexlib/document/normal/SourceDocument.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/partition_group_resource.h"
#include "indexlib/util/metrics/Metric.h"

namespace build_service { namespace workflow {

class SrcDataNode
{
public:
    SrcDataNode(const config::SrcNodeConfig& config, indexlib::util::MetricProviderPtr metricProvider);
    virtual ~SrcDataNode();
    SrcDataNode(SrcDataNode&& rhs) = delete;
    SrcDataNode(const SrcDataNode&) = delete;
    SrcDataNode& operator=(const SrcDataNode&) = delete;

public:
    bool init(const config::ResourceReaderPtr& configReader, const proto::PartitionId& pid,
              const common::ResourceKeeperPtr& checkpointKeeper) noexcept;
    void updateResource(common::ResourceKeeperPtr& latest, bool& needRestart) noexcept;
    virtual indexlib::partition::IndexPartitionReaderPtr getReader() const noexcept;
    virtual docid_t search(const indexlib::partition::IndexPartitionReaderPtr& reader, const std::string& pkey) const;
    bool isDocNewerThanIndex(const indexlib::partition::IndexPartitionReaderPtr& partReader, docid_t indexDocId,
                             indexlib::document::DocumentPtr currentDoc) const;
    bool build(const indexlib::document::DocumentPtr& doc);
    bool updateSrcDoc(const indexlib::partition::IndexPartitionReaderPtr& reader, docid_t docid,
                      indexlib::document::Document* updateDoc, bool& needRetry);
    void toRawDocument(const indexlib::document::DocumentPtr& doc, document::RawDocumentPtr rawDoc) noexcept;

    static bool readSrcConfig(const config::ProcessorConfigReaderPtr& configReader,
                              config::SrcNodeConfig& srcNodeConfig, bool* isExist) noexcept;
    static bool readSrcConfig(const config::ProcessorConfigReaderPtr& configReader,
                              config::SrcNodeConfig& srcNodeConfig) noexcept;
    config::SrcNodeConfig* getConfig() noexcept { return &_config; }
    common::ResourceKeeperPtr getUsingResource() noexcept;
    virtual bool getLocator(common::Locator& locator) const;

private:
    indexlib::partition::IndexPartition::OpenStatus reopen(versionid_t targetVersionId) noexcept;

    void toSourceDocument(const indexlib::document::SerializedSourceDocumentPtr& serializedDoc,
                          indexlib::document::SourceDocument* sourceDoc);

    void toSerializedSourceDocument(const indexlib::document::SourceDocumentPtr& sourceDoc, autil::mem_pool::Pool* pool,
                                    indexlib::document::SerializedSourceDocumentPtr serializedDoc);

    bool calculateIndexPid(const config::ResourceReaderPtr& configReader, const proto::PartitionId& pid,
                           const std::string& indexRoot, proto::PartitionId& indexPid) noexcept;
    bool prepareLocalWorkDir(const proto::PartitionId& pid, std::string& localIndexRoot) noexcept;
    bool prepareIndexBuilder(const proto::PartitionId& indexPid) noexcept;
    bool rewriteSchemaAndLoadconfig(const config::ResourceReaderPtr& configReader,
                                    const std::string& localPartitionRoot) noexcept;
    bool loadFull(const std::string& localPartitionRoot) noexcept;
    void disableUselessFields(indexlib::config::IndexPartitionSchemaPtr& partitionSchema,
                              indexlib::config::FieldSchemaPtr& fieldSchema,
                              const std::set<std::string>& keepFieldNamesconst, bool disableSource) noexcept;
    void registerMetric() noexcept;
    void reopenLoop() noexcept;

    versionid_t getIndexVersion(const common::ResourceKeeperPtr& keeper) const noexcept;
    std::string getIndexCluster(const common::ResourceKeeperPtr& keeper) const noexcept;

    // virtual for test
    virtual std::string getSrcClusterName() noexcept;
    virtual std::string getRemoteIndexPath() const noexcept;
    virtual std::string getLocalWorkDir() const;

    virtual std::vector<std::string> getIndexRootFromResource(const common::ResourceKeeperPtr& resource) const noexcept;
    virtual common::ResourceKeeperPtr getLatestVersion() noexcept;

private:
    static const int64_t reopenDetectInterval;
    static const int64_t openRetryCount;

    config::SrcNodeConfig _config;
    FieldType _orderPreservingFieldType;
    fieldid_t _preserverFieldId; // preserving fieldid in original schema
    indexlib::util::MetricProviderPtr _metricProvider;
    indexlib::partition::IndexPartitionResourcePtr _indexResource;
    indexlib::partition::IndexPartitionPtr _partition;
    indexlib::partition::IndexBuilderPtr _indexBuilder;
    indexlib::config::SourceSchemaPtr _sourceSchema;

    autil::ThreadMutex _targetMutex;
    common::ResourceKeeperPtr _currentResource, _targetResource;
    autil::LoopThreadPtr _reopenThread;
    std::string _remoteIndexRoot;
    bool _hasGenerateLoadConfig;

    indexlib::util::MetricPtr _targetVersonMetric;
    indexlib::util::MetricPtr _currentVersionMetric;
    indexlib::util::MetricPtr _useIndexIdxMetric;
    size_t _useIndexIdx;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SrcDataNode);

}} // namespace build_service::workflow
