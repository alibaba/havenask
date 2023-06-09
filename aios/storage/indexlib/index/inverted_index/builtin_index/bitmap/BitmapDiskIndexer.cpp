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
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapDiskIndexer.h"

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapLeafReader.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryReader.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryReader.h"

namespace indexlib::index {
namespace {
} // namespace

AUTIL_LOG_SETUP(indexlib.index, BitmapDiskIndexer);

BitmapDiskIndexer::BitmapDiskIndexer(uint64_t docCount) : _docCount(docCount) {}

Status BitmapDiskIndexer::Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                               const std::shared_ptr<file_system::IDirectory>& indexDirectory)
{
    try {
        auto config = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
        if (config == nullptr) {
            auto status = Status::InvalidArgs("fail to cast to legacy index config, indexName[%s] indexType[%s]",
                                              indexConfig->GetIndexName().c_str(), indexConfig->GetIndexType().c_str());
            AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
            return status;
        }
        if (_docCount == 0) {
            AUTIL_LOG(INFO, "doc count is zero in index [%s], just do nothing", indexConfig->GetIndexName().c_str());
            return Status::OK();
        }
        auto subDir = file_system::IDirectory::ToLegacyDirectory(indexDirectory)
                          ->GetDirectory(config->GetIndexName(), /*throwExceptionIfNotExist=*/false);
        if (!subDir) {
            auto status =
                Status::InternalError("fail to get subDir, indexName[%s] indexType[%s]",
                                      indexConfig->GetIndexName().c_str(), indexConfig->GetIndexType().c_str());
            AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
            return status;
        }
        std::shared_ptr<DictionaryReader> dictReader;
        std::shared_ptr<file_system::FileReader> postingReader;
        // TODO(makuo.mnb) use IDirectory
        if (subDir->IsExist(BITMAP_DICTIONARY_FILE_NAME)) {
            dictReader = std::make_shared<TieredDictionaryReader>();
            auto status = dictReader->Open(subDir, BITMAP_DICTIONARY_FILE_NAME, /*supportFileCompress=*/false);
            RETURN_IF_STATUS_ERROR(status, "dictionary reader open fail");
            postingReader = subDir->CreateFileReader(BITMAP_POSTING_FILE_NAME,
                                                     file_system::ReaderOption::Writable(file_system::FSOT_MEM_ACCESS));
        }
        std::shared_ptr<file_system::ResourceFile> expandFile;
        if (config->IsIndexUpdatable()) {
            auto resource = subDir->GetResourceFile(BITMAP_POSTING_EXPAND_FILE_NAME);
            if (!resource || resource->Empty()) {
                auto resourceTable = new ExpandDataTable();
                resource = subDir->CreateResourceFile(BITMAP_POSTING_EXPAND_FILE_NAME);
                assert(resource);
                resource->Reset(resourceTable);
            }
            expandFile = resource;
        }
        _leafReader = std::make_shared<BitmapLeafReader>(config, dictReader, postingReader, expandFile, _docCount);
        return Status::OK();
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "bitmap disk indexer open failed, exception[%s]", e.what());
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "bitmap disk  indexer open failed, exception[%s]", e.what());
    } catch (...) {
        AUTIL_LOG(ERROR, "bitmap disk indexer open failed, unknown exception");
    }
    AUTIL_LOG(ERROR, "bitmap disk indexer open failed");
    return Status::Corruption("bitmap disk indexer open failed");
}

void BitmapDiskIndexer::Update(docid_t docId, const DictKeyInfo& key, bool isDelete)
{
    if (_leafReader) {
        return _leafReader->Update(docId, key, isDelete);
    }
}

size_t BitmapDiskIndexer::EstimateMemUsed(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                          const std::shared_ptr<file_system::IDirectory>& indexDirectory)
{
    auto subDir = file_system::IDirectory::ToLegacyDirectory(indexDirectory)
                      ->GetDirectory(indexConfig->GetIndexName(), /*throwExceptionIfNotExist=*/false);
    if (subDir == nullptr) {
        AUTIL_LOG(ERROR, "get inverted index field dir [%s] failed", indexConfig->GetIndexName().c_str());
        return 0;
    }
    size_t totalMemUse = subDir->EstimateFileMemoryUse(BITMAP_DICTIONARY_FILE_NAME, file_system::FSOT_LOAD_CONFIG) +
                         subDir->EstimateFileMemoryUse(BITMAP_POSTING_FILE_NAME, file_system::FSOT_MEM_ACCESS);
    return totalMemUse;
}

size_t BitmapDiskIndexer::EvaluateCurrentMemUsed()
{
    if (_leafReader) {
        return _leafReader->EvaluateCurrentMemUsed();
    }
    return 0;
}

} // namespace indexlib::index
