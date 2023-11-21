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
#include "autil/Log.h"
#include "indexlib/index/DiskIndexerParameter.h"
#include "indexlib/index/IDiskIndexer.h"
#include "indexlib/index/attribute/AttributeDiskIndexer.h"
#include "indexlib/index/attribute/AttributeIndexFactory.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/primary_key/BlockArrayPrimaryKeyDiskIndexer.h"
#include "indexlib/index/primary_key/Constant.h"
#include "indexlib/index/primary_key/HashTablePrimaryKeyDiskIndexer.h"
#include "indexlib/index/primary_key/SortArrayPrimaryKeyDiskIndexer.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlibv2::index {

template <typename Key>
class PrimaryKeyDiskIndexer : public autil::NoCopyable, public IDiskIndexer
{
public:
    PrimaryKeyDiskIndexer(const DiskIndexerParameter& parameter) : _indexerParam(parameter) {}
    ~PrimaryKeyDiskIndexer() {}

public:
    static std::unique_ptr<PrimaryKeyLeafIterator<Key>>
    CreatePrimaryKeyLeafIterator(const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& indexConfig,
                                 const indexlib::file_system::FileReaderPtr& reader) noexcept
    {
        return PrimaryKeyDiskIndexerTyped<Key>::CreatePrimaryKeyLeafIterator(indexConfig, reader);
    }

    std::unique_ptr<PrimaryKeyLeafIterator<Key>> CreatePrimaryKeyLeafIterator()
    {
        return PrimaryKeyDiskIndexerTyped<Key>::CreatePrimaryKeyLeafIterator(_pkIndexType, GetFileReader());
    }

    bool OpenWithSliceFile(const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& indexConfig,
                           const std::shared_ptr<indexlib::file_system::IDirectory>& dir, const std::string& fileName)
    {
        // OpenWithSliceFile only support open with hashTableReader
        _pkIndexType = pk_hash_table;
        _hashTablePrimaryKeyDiskIndexer = std::make_unique<HashTablePrimaryKeyDiskIndexer<Key>>();
        return _hashTablePrimaryKeyDiskIndexer->Open(indexConfig, dir, fileName, indexlib::file_system::FSOT_SLICE);
    }

    Status Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override
    {
        if (_indexerParam.docCount == 0) {
            return Status::OK();
        }
        assert(indexDirectory);
        auto pkIndexConfig = std::dynamic_pointer_cast<indexlibv2::index::PrimaryKeyIndexConfig>(indexConfig);
        auto [st, pkDataDir] = indexDirectory->GetDirectory(pkIndexConfig->GetIndexName()).StatusWith();
        RETURN_IF_STATUS_ERROR(st, "get pk data dir failed");

        bool ret = InnerOpen(pkIndexConfig, pkDataDir);

        if (!ret) {
            AUTIL_LOG(ERROR, "open disk indexer failed");
            return Status::IOError("open disk indexer failed");
        }

        return Status::OK();
    }

    Status OpenPKAttribute(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                           const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
    {
        auto pkIndexConfig = std::dynamic_pointer_cast<indexlibv2::index::PrimaryKeyIndexConfig>(indexConfig);
        if (!pkIndexConfig->HasPrimaryKeyAttribute()) {
            return Status::OK();
        }
        AttributeIndexFactory factory;
        auto indexer = factory.CreateDiskIndexer(pkIndexConfig->GetPKAttributeConfig(), _indexerParam);
        auto pkAttrDiskIndexer = std::dynamic_pointer_cast<AttributeDiskIndexer>(indexer);
        assert(pkAttrDiskIndexer);
        auto status = pkAttrDiskIndexer->Open(pkIndexConfig->GetPKAttributeConfig(), indexDirectory);
        RETURN_IF_STATUS_ERROR(status, "open pk attribute failed");
        _pkAttrDiskIndexer = pkAttrDiskIndexer;
        return Status::OK();
    }
    void InhertPkAttributeDiskIndexer(PrimaryKeyDiskIndexer<Key>* other)
    {
        if (other) {
            _pkAttrDiskIndexer = other->_pkAttrDiskIndexer;
        }
    }
    std::shared_ptr<AttributeDiskIndexer> GetPKAttributeDiskIndexer() const { return _pkAttrDiskIndexer; }

    bool InnerOpen(const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& indexConfig,
                   const std::shared_ptr<indexlib::file_system::IDirectory>& dir)
    {
        _pkIndexType = indexConfig->GetPrimaryKeyIndexType();
        switch (_pkIndexType) {
        case pk_sort_array: {
            _sortArrayPrimaryKeyDiskIndexer = std::make_unique<SortArrayPrimaryKeyDiskIndexer<Key>>();
            return _sortArrayPrimaryKeyDiskIndexer->Open(indexConfig, dir, PRIMARY_KEY_DATA_FILE_NAME,
                                                         indexlib::file_system::FSOT_MEM_ACCESS);
        }
        case pk_hash_table: {
            _hashTablePrimaryKeyDiskIndexer = std::make_unique<HashTablePrimaryKeyDiskIndexer<Key>>();
            return _hashTablePrimaryKeyDiskIndexer->Open(indexConfig, dir, PRIMARY_KEY_DATA_FILE_NAME,
                                                         indexlib::file_system::FSOT_MEM_ACCESS);
        }
        case pk_block_array: {
            _blockArrayPrimaryKeyDiskIndexer = std::make_unique<BlockArrayPrimaryKeyDiskIndexer<Key>>();
            return _blockArrayPrimaryKeyDiskIndexer->Open(indexConfig, dir, PRIMARY_KEY_DATA_FILE_NAME,
                                                          indexlib::file_system::FSOT_LOAD_CONFIG);
        }
        default: {
            AUTIL_LOG(ERROR, "unsupport pk index type!");
            return false;
        }
        }
    }

    static indexlib::file_system::FileReaderPtr
    OpenPKDataFile(const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& indexConfig,
                   const indexlib::file_system::DirectoryPtr& segmentDirectory)
    {
        auto segIDir = segmentDirectory->GetIDirectory();
        assert(segIDir);

        auto [status, indexDirectory] = segIDir->GetDirectory(INDEX_DIR_NAME).StatusWith();
        if (!status.IsOK() || !indexDirectory) {
            AUTIL_LOG(ERROR, "Failed to get direcotry [%s]. ", segIDir->DebugString().c_str());
            return nullptr;
        }
        auto [pkStatus, pkDirectory] = indexDirectory->GetDirectory(indexConfig->GetIndexName()).StatusWith();
        if (!status.IsOK() || !pkDirectory) {
            AUTIL_LOG(ERROR, "Failed to get direcotry [%s]. ", indexDirectory->DebugString().c_str());
            return nullptr;
        }
        auto [readStatus, fileReader] =
            pkDirectory
                ->CreateFileReader(PRIMARY_KEY_DATA_FILE_NAME,
                                   indexlib::file_system::ReaderOption::NoCache(indexlib::file_system::FSOT_BUFFERED))
                .StatusWith();
        if (!readStatus.IsOK() || !fileReader) {
            AUTIL_LOG(ERROR, "Failed to create pk file reader for directory [%s]. ",
                      pkDirectory->DebugString().c_str());
            return nullptr;
        }
        return fileReader;
    }

    future_lite::coro::Lazy<indexlib::index::Result<docid_t>>
    LookupAsync(const Key& hashKey, future_lite::Executor* executor) const noexcept
    {
        switch (_pkIndexType) {
        case pk_hash_table: {
            co_return _hashTablePrimaryKeyDiskIndexer->Lookup(hashKey);
        }
        case pk_sort_array: {
            co_return _sortArrayPrimaryKeyDiskIndexer->Lookup(hashKey);
        }
        case pk_block_array: {
            co_return co_await _blockArrayPrimaryKeyDiskIndexer->LookupAsync(hashKey, executor);
        }
        default: {
            AUTIL_LOG(ERROR, "unsupport pk index type!");
            co_return INVALID_DOCID;
        }
        }
    }

    indexlib::index::Result<docid_t> Lookup(const Key& hashKey) const noexcept __ALWAYS_INLINE
    {
        switch (_pkIndexType) {
        case pk_hash_table: {
            return _hashTablePrimaryKeyDiskIndexer->Lookup(hashKey);
        }
        case pk_sort_array: {
            return _sortArrayPrimaryKeyDiskIndexer->Lookup(hashKey);
        }
        case pk_block_array: {
            return _blockArrayPrimaryKeyDiskIndexer->Lookup(hashKey);
        }
        default: {
            AUTIL_LOG(ERROR, "unsupport pk index type!");
            return INVALID_DOCID;
        }
        }
    }

    static size_t CalculateLoadSize(const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& indexConfig,
                                    const std::shared_ptr<indexlib::file_system::IDirectory>& dir,
                                    const std::string& fileName)
    {
        PrimaryKeyIndexType pkIndexType = indexConfig->GetPrimaryKeyIndexType();
        switch (pkIndexType) {
        case pk_sort_array:
        case pk_hash_table: {
            auto [status, size] =
                dir->EstimateFileMemoryUse(fileName, indexlib::file_system::FSOT_MEM_ACCESS).StatusWith();
            if (!status.IsOK()) {
                AUTIL_LEGACY_THROW(indexlib::util::FileIOException, "failed to estimate file memory used");
                return 0;
            }
            return size;
        }
        case pk_block_array: {
            auto [status, memLockSize] =
                dir->EstimateFileMemoryUse(fileName, indexlib::file_system::FSOT_LOAD_CONFIG).StatusWith();
            if (!status.IsOK()) {
                AUTIL_LEGACY_THROW(indexlib::util::FileIOException, "failed to estimate file memory used");
                return 0;
            }
            if (memLockSize > 0) {
                return memLockSize;
            }

            bool disableSliceMemLock = autil::EnvUtil::getEnv<bool>("INDEXLIB_DISABLE_SLICE_MEM_LOCK", false);
            if (disableSliceMemLock) {
                auto [innerStatus, innerSize] =
                    dir->EstimateFileMemoryUse(fileName, indexlib::file_system::FSOT_MEM_ACCESS).StatusWith();
                if (!innerStatus.IsOK()) {
                    AUTIL_LEGACY_THROW(indexlib::util::FileIOException, "failed to estimate file memory used");
                    return 0;
                }
                if (innerSize == 0) {
                    // mem-nolock && disable slice memlock
                    return 0;
                }
            }
            auto [readerStatus, fileReader] =
                dir->CreateFileReader(fileName, indexlib::file_system::FSOT_BUFFERED).StatusWith();
            if (!readerStatus.IsOK()) {
                AUTIL_LEGACY_THROW(indexlib::util::BadParameterException, "failed to open file");
                return 0;
            }
            indexlibv2::index::BlockArrayReader<Key, docid_t> reader;
            auto pairRet = reader.Init(fileReader, indexlib::file_system::IDirectory::ToLegacyDirectory(dir),
                                       fileReader->GetLength(), false);
            THROW_IF_STATUS_ERROR(pairRet.first);
            if (!pairRet.second) {
                return 0;
            }
            return reader.EstimateMetaSize();
        }
        default: {
            AUTIL_LEGACY_THROW(indexlib::util::BadParameterException, "unsupport pk index type!");
            return 0;
        }
        }
    }

    // pkIndexer is special so frameWork will'n call this interface instead of calling estimate function of
    // primaryKeyReader
    // only estimate pkAttribute
    size_t EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                           const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override
    {
        if (_indexerParam.docCount == 0) {
            return 0;
        }
        auto pkIndexConfig = std::dynamic_pointer_cast<indexlibv2::index::PrimaryKeyIndexConfig>(indexConfig);
        if (!pkIndexConfig->HasPrimaryKeyAttribute()) {
            return 0;
        }
        AttributeIndexFactory factory;
        auto indexer = factory.CreateDiskIndexer(pkIndexConfig->GetPKAttributeConfig(), _indexerParam);
        auto pkAttrDiskIndexer = std::dynamic_pointer_cast<AttributeDiskIndexer>(indexer);
        assert(pkAttrDiskIndexer);
        auto [status, pkDirectory] = indexDirectory->GetDirectory(indexConfig->GetIndexName()).StatusWith();
        if (!status.IsOK() || !pkDirectory) {
            AUTIL_LOG(ERROR, "failed to get pkAttribute direcotry [%s]", indexDirectory->DebugString().c_str());
            return 0;
        }
        return pkAttrDiskIndexer->EstimateMemUsed(pkIndexConfig->GetPKAttributeConfig(), pkDirectory);
    }

    size_t EvaluateCurrentMemUsed() override
    {
        size_t totalMemUse = 0;
        if (_hashTablePrimaryKeyDiskIndexer) {
            totalMemUse += _hashTablePrimaryKeyDiskIndexer->EvaluateCurrentMemUsed();
        } else if (_sortArrayPrimaryKeyDiskIndexer) {
            totalMemUse += _sortArrayPrimaryKeyDiskIndexer->EvaluateCurrentMemUsed();
        } else if (_blockArrayPrimaryKeyDiskIndexer) {
            totalMemUse += _blockArrayPrimaryKeyDiskIndexer->EvaluateCurrentMemUsed();
        }
        if (_pkAttrDiskIndexer) {
            totalMemUse += _pkAttrDiskIndexer->EvaluateCurrentMemUsed();
        }
        return totalMemUse;
    }

private:
    const indexlib::file_system::FileReaderPtr& GetFileReader() const
    {
        switch (_pkIndexType) {
        case pk_hash_table: {
            return _hashTablePrimaryKeyDiskIndexer->GetFileReader();
        }
        case pk_sort_array: {
            return _sortArrayPrimaryKeyDiskIndexer->GetFileReader();
        }
        case pk_block_array: {
            return _blockArrayPrimaryKeyDiskIndexer->GetFileReader();
        }
        default: {
            AUTIL_LOG(ERROR, "unsupport pk index type!");
            static indexlib::file_system::FileReaderPtr emptyReader;
            return emptyReader;
        }
        }
    }

private:
    PrimaryKeyIndexType _pkIndexType = pk_sort_array;
    std::unique_ptr<HashTablePrimaryKeyDiskIndexer<Key>> _hashTablePrimaryKeyDiskIndexer;
    std::unique_ptr<SortArrayPrimaryKeyDiskIndexer<Key>> _sortArrayPrimaryKeyDiskIndexer;
    std::unique_ptr<BlockArrayPrimaryKeyDiskIndexer<Key>> _blockArrayPrimaryKeyDiskIndexer;
    DiskIndexerParameter _indexerParam;
    std::shared_ptr<AttributeDiskIndexer> _pkAttrDiskIndexer;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, PrimaryKeyDiskIndexer, T);
} // namespace indexlibv2::index
