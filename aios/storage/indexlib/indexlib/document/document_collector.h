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

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/document.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlib { namespace document {

// Not thread safe.
// Not designed for multi-thread use as of 2021/10. But can be adapted easily for multi-thread use case in the
// future.
// DocumentCollector是在batch build时用来收集doc的类。batch build过程中各个模块会以DocumentCollector为单元进行操作。
class DocumentCollector
{
public:
    DocumentCollector(const config::IndexPartitionOptions& options);
    ~DocumentCollector();

    DocumentCollector(const DocumentCollector&) = delete;
    DocumentCollector& operator=(const DocumentCollector&) = delete;
    DocumentCollector(DocumentCollector&&) = delete;
    DocumentCollector& operator=(DocumentCollector&&) = delete;

public:
    size_t Size() const { return _documents.size(); }
    const std::vector<document::DocumentPtr>& GetDocuments() const { return _documents; }

    // 非线程安全
    void Add(const document::DocumentPtr& doc);
    // 非线程安全，标记删除，可以避免析构Document对象
    // 标记删除的应用场景举例：DocumentCollector中某个doc无效可以不构建，通过标记删除后在构建时就可以跳过。
    void LogicalDelete(size_t i);
    // 非线程安全，调用之后，GetDocuments()获得的vector中没有nullptr
    void RemoveNullDocuments();
    // 确认 this 不再使用之后可以调用
    void DestructDocumentsForParallel(size_t parallelNum, size_t parallelIdx);
    size_t GetSuggestDestructParallelNum(size_t threadCount);

    int64_t EstimateMemory() const;
    int32_t BatchSize() const { return _batchSize; }
    bool ShouldTriggerBuild() const;
    size_t GetTotalMemoryUse() const;

public:
    // This is for debug purposes and might hurt performance.
    static std::map<std::string, int> GetBatchStatistics(const std::vector<document::DocumentPtr>& documents,
                                                         config::IndexPartitionSchemaPtr schema);

private:
    static void StatisticsForUpdate(document::DocumentPtr document, config::IndexPartitionSchemaPtr schema,
                                    std::map<std::string, int>* indexNameToTokenCount);

private:
    std::vector<document::DocumentPtr> _documents;
    std::vector<document::DocumentPtr> _deletedDocuments;
    bool _isDirty = false;
    int32_t _batchSize;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentCollector);

}} // namespace indexlib::document
