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
#include "build_service_tasks/repartition/RepartitionDocFilter.h"

#include <assert.h>
#include <cstddef>
#include <memory>
#include <stdint.h>
#include <type_traits>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/IndexPathConstructor.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/config/offline_config.h"
#include "indexlib/config/offline_config_base.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/load_config/LoadStrategy.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/misc/common.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/offline_partition_reader.h"
#include "indexlib/partition/on_disk_partition_data.h"

using namespace std;
using namespace build_service::util;
using namespace autil;
using indexlib::index_base::PartitionDataPtr;
using indexlib::merger::MultiPartSegmentDirectory;
using indexlib::merger::SegmentDirectoryPtr;

using namespace indexlib::config;
using namespace indexlib::partition;
using namespace indexlib::file_system;
using namespace indexlib::index;

namespace build_service { namespace task_base {
BS_LOG_SETUP(task_base, RepartitionDocFilter);

RepartitionDocFilter::RepartitionDocFilter(const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options,
                                           const proto::Range& partitionRange)
    : _schema(schema)
    , _options(options)
    , _partitionRange(partitionRange)
{
}

RepartitionDocFilter::~RepartitionDocFilter() {}

bool RepartitionDocFilter::Init(const SegmentDirectoryPtr& segDir, const string& hashField,
                                const autil::HashFunctionBasePtr& hashFunc)
{
    _options.GetOfflineConfig().readerConfig.loadIndex = false;
    _hashField = hashField;
    _hashFunction = hashFunc;
    _multiSegDirectory = DYNAMIC_POINTER_CAST(MultiPartSegmentDirectory, segDir);
    if (!_multiSegDirectory) {
        BS_LOG(ERROR, "segDir only support multi partition segment dir");
        return false;
    }
    bool hasSubSchema = false;
    if (_schema->GetSubIndexPartitionSchema()) {
        hasSubSchema = true;
    }
    PartitionDataPtr partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(
        _multiSegDirectory->GetRootDirs(), _multiSegDirectory->GetVersions(), hasSubSchema, true);

    if (!InitFilterDocidRange(partitionData)) {
        return false;
    }
    OfflinePartitionReaderPtr offlineReader(new OfflinePartitionReader(_options, _schema));
    offlineReader->Open(partitionData);
    vector<string> fieldNames;
    fieldNames.push_back(_hashField);
    _fieldExtractor.reset(new RawDocumentFieldExtractor(fieldNames));
    if (!_fieldExtractor->Init(offlineReader)) {
        BS_LOG(ERROR, "init field extractor failed");
        return false;
    }
    return true;
}

bool RepartitionDocFilter::InitFilterDocidRange(const PartitionDataPtr& partitionData)
{
    auto iter = partitionData->Begin();
    for (; iter != partitionData->End(); iter++) {
        string partPath = fslib::util::FileUtil::getParentDir(iter->GetDirectory()->GetOutputPath());
        uint32_t rangeFrom, rangeTo;
        if (!IndexPathConstructor::parsePartitionRange(partPath, rangeFrom, rangeTo)) {
            return false;
        }
        if (rangeFrom >= _partitionRange.from() && rangeTo <= _partitionRange.to()) {
            continue;
        }
        docid_t baseDocId = iter->GetBaseDocId();
        uint64_t docCount = iter->GetSegmentInfo()->docCount;
        if (docCount == 0) {
            continue;
        }

        _filterDocidRange.push_back(make_pair(baseDocId, baseDocId + docCount - 1));
    }
    return true;
}

bool RepartitionDocFilter::NeedFilter(docid_t docid)
{
    for (size_t i = 0; i < _filterDocidRange.size(); i++) {
        if (docid >= _filterDocidRange[i].first && docid <= _filterDocidRange[i].second) {
            return true;
        }
    }
    return false;
}

bool RepartitionDocFilter::Filter(docid_t docid)
{
    if (!NeedFilter(docid)) {
        return false;
    }
    SeekStatus status = _fieldExtractor->Seek(docid);
    if (status == SS_DELETED) {
        return false;
    }
    if (status == SS_ERROR) {
        string errorMsg = string("get hash field from docid:") + StringUtil::toString(docid) + "failed";
        throw autil::legacy::ExceptionBase(errorMsg);
    }
    assert(status == SS_OK);
    auto fieldIter = _fieldExtractor->CreateIterator();
    assert(fieldIter.HasNext());
    string fieldValue;
    fieldIter.Next(_hashField, fieldValue);
    hashid_t hashId = _hashFunction->getHashId(fieldValue);
    if (hashId >= _partitionRange.from() && hashId <= _partitionRange.to()) {
        return false;
    }
    return true;
}

}} // namespace build_service::task_base
