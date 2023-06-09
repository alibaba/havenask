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
#include "indexlib/index/aggregation/AggregationIndexMerger.h"

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/aggregation/AggregationReader.h"
#include "indexlib/index/aggregation/Constants.h"
#include "indexlib/index/aggregation/IndexMeta.h"
#include "indexlib/index/aggregation/KeyMeta.h"
#include "indexlib/index/aggregation/ValueIteratorCreator.h"

namespace indexlibv2::index {
AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, AggregationIndexMerger);

AggregationIndexMerger::AggregationIndexMerger() = default;

AggregationIndexMerger::~AggregationIndexMerger() = default;

Status AggregationIndexMerger::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                    const std::map<std::string, std::any>& params)
{
    _indexConfig = std::dynamic_pointer_cast<config::AggregationIndexConfig>(indexConfig);
    if (!_indexConfig) {
        return Status::InternalError("[%s:%s] is not an aggregation index", indexConfig->GetIndexName().c_str(),
                                     indexConfig->GetIndexType().c_str());
    }
    if (_indexConfig->SupportDelete()) {
        auto it = params.find("drop_delete_key");
        if (it == params.end()) {
            return Status::InvalidArgs("no drop_delete_key in params");
        }
        std::string dropDeleteKey = std::any_cast<std::string>(it->second);
        if (!autil::StringUtil::fromString(dropDeleteKey, _dropDeletedData)) {
            return Status::InvalidArgs("drop_delete_key in params not bool");
        }
    }
    return Status::OK();
}

StatusOr<std::shared_ptr<indexlib::file_system::IDirectory>>
MakeDirectory(const std::shared_ptr<indexlib::file_system::IDirectory>& root, const std::string& dirName,
              bool removeExist)
{
    if (removeExist) {
        indexlib::file_system::RemoveOption removeOption;
        removeOption.mayNonExist = true;
        auto s = root->RemoveDirectory(dirName, removeOption).Status();
        if (!s.IsOK()) {
            AUTIL_LOG(ERROR, "remove %s failed, error: %s", dirName.c_str(), s.ToString().c_str());
            return {s.steal_error()};
        }
    }
    indexlib::file_system::DirectoryOption dirOption;
    auto result = root->MakeDirectory(dirName, dirOption);
    if (!result.OK()) {
        return {result.Status().steal_error()};
    } else {
        return {result.Value()};
    }
}

Status AggregationIndexMerger::Merge(const SegmentMergeInfos& segMergeInfos,
                                     const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager)
{
    if (segMergeInfos.targetSegments.empty()) {
        AUTIL_LOG(INFO, "no need to merge");
        return Status::OK();
    }
    if (segMergeInfos.targetSegments.size() > 1) {
        return Status::Unimplement("do not support merge source segments to multiple segments");
    }

    const auto& targetSegment = segMergeInfos.targetSegments[0];
    const auto& segmentRoot = targetSegment->segmentDir->GetIDirectory();
    indexlib::file_system::DirectoryOption dirOption;
    auto result = segmentRoot->MakeDirectory(AGGREGATION_INDEX_PATH, dirOption);
    if (!result.OK()) {
        AUTIL_LOG(ERROR, "make %s failed, error: %s", AGGREGATION_INDEX_PATH.c_str(),
                  result.Status().ToString().c_str());
        return result.Status();
    }
    auto indexRoot = result.Value();
    auto r = MakeDirectory(indexRoot, _indexConfig->GetIndexName(), true);
    if (!r.IsOK()) {
        return {r.steal_error()};
    }
    auto indexDir = std::move(r.steal_value());

    std::vector<std::shared_ptr<framework::Segment>> segments;
    for (auto it = segMergeInfos.srcSegments.rbegin(); it != segMergeInfos.srcSegments.rend(); ++it) {
        segments.push_back(it->segment);
    }
    auto reader = std::make_unique<AggregationReader>();
    auto s = reader->Init(_indexConfig, {}, segments);
    if (!s.IsOK()) {
        AUTIL_LOG(ERROR, "init reader failed for %s, error:%s", _indexConfig->GetIndexName().c_str(),
                  s.ToString().c_str());
        return s;
    }
    auto keyIter = reader->CreateKeyIterator();
    s = DoMerge(indexDir, _indexConfig, keyIter.get(), reader.get());
    if (!s.IsOK()) {
        AUTIL_LOG(ERROR, "%s merge failed, error: %s", _indexConfig->GetIndexName().c_str(), s.ToString().c_str());
        return s;
    }
    // merge deleted data
    if (_indexConfig->SupportDelete()) {
        const auto& deleteReader = reader->GetDeleteReader();
        s = MergeDeleteData(indexDir, deleteReader.get());
    }
    return s;
}

class EmptyKeyIterator final : public IKeyIterator
{
public:
    bool HasNext() const override { return false; }
    uint64_t Next() override { return 0; }
};

Status AggregationIndexMerger::MergeDeleteData(const std::shared_ptr<indexlib::file_system::IDirectory>& indexRoot,
                                               AggregationReader* deleteReader)
{
    const auto& deleteConfig = _indexConfig->GetDeleteConfig();
    auto r = MakeDirectory(indexRoot, deleteConfig->GetIndexName(), true);
    if (!r.IsOK()) {
        return {r.steal_error()};
    }
    auto indexDir = std::move(r.steal_value());
    if (!_dropDeletedData) {
        auto keyIter = deleteReader->CreateKeyIterator();
        return DoMerge(indexDir, deleteConfig, keyIter.get(), deleteReader);
    } else {
        EmptyKeyIterator keyIter;
        return DoMerge(indexDir, deleteConfig, &keyIter, deleteReader);
    }
}

Status AggregationIndexMerger::DoMerge(const std::shared_ptr<indexlib::file_system::IDirectory>& indexRoot,
                                       const std::shared_ptr<config::AggregationIndexConfig>& indexConfig,
                                       IKeyIterator* keyIter, AggregationReader* reader)
{
    std::vector<KeyMeta> keyMetas;
    auto [s, fileWriter] = CreateFileWriter(indexRoot, VALUE_FILE_NAME);
    RETURN_STATUS_DIRECTLY_IF_ERROR(s);

    bool needDedup = indexConfig->NeedDedup() && !indexConfig->GetSortDescriptions().empty();
    autil::mem_pool::UnsafePool pool;
    // dump values
    IndexMeta indexMeta;
    while (keyIter->HasNext()) {
        uint64_t key = keyIter->Next();
        ValueIteratorCreator creator(reader);
        auto r = creator.CreateIterator(key, &pool);
        if (!r.IsOK()) {
            return {r.steal_error()};
        }
        KeyMeta keyMeta;
        keyMeta.key = key;
        keyMeta.offset = fileWriter->GetLogicLength();
        std::vector<autil::StringView> values;
        auto valueIter = std::move(r.steal_value());
        ValueStat stat;
        while (valueIter->HasNext()) {
            autil::StringView value;
            auto s = valueIter->Next(&pool, value);
            if (s.IsOK()) {
                stat.valueBytes += value.size();
                stat.valueCount++;
                values.emplace_back(std::move(value));
            } else if (!s.IsEof()) {
                return s;
            } else {
                break;
            }
        }
        if (needDedup) {
            auto it = std::unique(values.begin(), values.end());
            values.erase(it, values.end());
            stat.valueCountAfterUnique += values.size();
        }
        for (const auto& value : values) {
            if (needDedup) {
                stat.valueBytesAfterUnique += value.size();
            }
            auto s = fileWriter->Write(value.data(), value.size()).Status();
            RETURN_STATUS_DIRECTLY_IF_ERROR(s);
        }
        indexMeta.Update(stat);
        if (needDedup && !indexConfig->SupportDelete()) {
            auto merged = reader->GetMergedValueMeta(key);
            keyMeta.valueMeta.valueCount = merged.valueCount;
        } else {
            keyMeta.valueMeta.valueCount = stat.valueCount;
        }
        keyMeta.valueMeta.valueCountAfterUnique = stat.valueCountAfterUnique;
        keyMetas.emplace_back(std::move(keyMeta));
        pool.reset();
    }
    RETURN_STATUS_DIRECTLY_IF_ERROR(fileWriter->Close().Status());

    // dump key
    std::tie(s, fileWriter) = CreateFileWriter(indexRoot, KEY_FILE_NAME);
    RETURN_STATUS_DIRECTLY_IF_ERROR(s);

    const char* data = reinterpret_cast<const char*>(keyMetas.data());
    RETURN_STATUS_DIRECTLY_IF_ERROR(fileWriter->Write(data, keyMetas.size() * sizeof(KeyMeta)).Status());
    RETURN_STATUS_DIRECTLY_IF_ERROR(fileWriter->Close().Status());

    // dump index meta
    std::tie(s, fileWriter) = CreateFileWriter(indexRoot, META_FILE_NAME);
    RETURN_STATUS_DIRECTLY_IF_ERROR(s);
    auto str = ToJsonString(indexMeta);
    RETURN_STATUS_DIRECTLY_IF_ERROR(fileWriter->Write(str.c_str(), str.size()).Status());
    return fileWriter->Close().Status();
}

std::pair<Status, std::shared_ptr<indexlib::file_system::FileWriter>>
AggregationIndexMerger::CreateFileWriter(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                         const std::string& fileName) const
{
    indexlib::file_system::WriterOption opts;
    opts.openType = indexlib::file_system::FSOT_BUFFERED;
    return directory->CreateFileWriter(fileName, opts).StatusWith();
}

} // namespace indexlibv2::index
