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

#include <string>
#include <utility>
#include <vector>

#include "autil/HashFunctionBase.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/util/segment_directory_base.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/doc_filter.h"
#include "indexlib/merger/multi_part_segment_directory.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/partition/raw_document_field_extractor.h"

namespace build_service { namespace task_base {

class RepartitionDocFilter : public indexlib::merger::DocFilter
{
public:
    RepartitionDocFilter(const indexlib::config::IndexPartitionSchemaPtr& schema,
                         const indexlib::config::IndexPartitionOptions& options, const proto::Range& partitionRange);

    ~RepartitionDocFilter();

private:
    RepartitionDocFilter(const RepartitionDocFilter&);
    RepartitionDocFilter& operator=(const RepartitionDocFilter&);

public:
    bool Init(const indexlib::merger::SegmentDirectoryPtr& segDir, const std::string& hashField,
              const autil::HashFunctionBasePtr& hashFunc);
    bool Filter(docid_t docid);
    bool InitFilterDocidRange(const indexlib::index_base::PartitionDataPtr& partitionData);
    bool NeedFilter(docid_t docid);

private:
    indexlib::config::IndexPartitionSchemaPtr _schema;
    indexlib::config::IndexPartitionOptions _options;
    indexlib::merger::MultiPartSegmentDirectoryPtr _multiSegDirectory;
    proto::Range _partitionRange;
    autil::HashFunctionBasePtr _hashFunction;
    std::string _hashField;
    indexlib::partition::RawDocumentFieldExtractorPtr _fieldExtractor;
    typedef std::pair<docid_t, docid_t> DocidRange;
    std::vector<DocidRange> _filterDocidRange;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RepartitionDocFilter);

}} // namespace build_service::task_base
