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
#ifndef __INDEXLIB_PRIMARY_KEY_LOADER_H
#define __INDEXLIB_PRIMARY_KEY_LOADER_H

#include <memory>

#include "autil/TimeUtility.h"
#include "indexlib/common_define.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/normal/primarykey/combine_segments_primary_key_iterator.h"
#include "indexlib/index/normal/primarykey/primary_key_formatter.h"
#include "indexlib/index/normal/primarykey/primary_key_formatter_creator.h"
#include "indexlib/index/normal/primarykey/primary_key_load_plan.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename Key>
class PrimaryKeyLoader
{
public:
    PrimaryKeyLoader() {}
    ~PrimaryKeyLoader() {}

public:
    static file_system::FileReaderPtr Load(const PrimaryKeyLoadPlanPtr& plan,
                                           const config::PrimaryKeyIndexConfigPtr& indexConfig);
    static size_t EstimateLoadSize(const PrimaryKeyLoadPlanPtr& plan,
                                   const config::PrimaryKeyIndexConfigPtr& indexConfig,
                                   const index_base::Version& lastLoadVersion);
    static file_system::DirectoryPtr GetPrimaryKeyDirectory(const file_system::DirectoryPtr& segmentDir,
                                                            const config::IndexConfigPtr& indexConfig);

    static file_system::FileReaderPtr OpenPKDataFile(const file_system::DirectoryPtr& segmentDir,
                                                     const PrimaryKeyIndexType& pkType);

private:
    static file_system::FileReaderPtr DirectLoad(const PrimaryKeyLoadPlanPtr& plan,
                                                 const config::PrimaryKeyIndexConfigPtr& indexConfig);

    static file_system::FileReaderPtr CreateTargetSliceFileReader(const PrimaryKeyLoadPlanPtr& plan,
                                                                  const config::PrimaryKeyIndexConfigPtr& indexConfig);

private:
    typedef std::shared_ptr<PrimaryKeyFormatter<Key>> PrimaryKeyFormatterPtr;
    typedef typename PrimaryKeyFormatter<Key>::PKPair PKPair;

private:
    IE_LOG_DECLARE();
};
IE_LOG_SETUP_TEMPLATE(index, PrimaryKeyLoader);
///////////////////////////////////////////////////////////

template <typename Key>
file_system::FileReaderPtr PrimaryKeyLoader<Key>::DirectLoad(const PrimaryKeyLoadPlanPtr& plan,
                                                             const config::PrimaryKeyIndexConfigPtr& indexConfig)
{
    assert(plan);
    assert(indexConfig);
    file_system::DirectoryPtr pkDir = plan->GetTargetFileDirectory();
    autil::ScopedTime2 timer;
    IE_LOG(INFO, "Begin direct load pk in [%s]", pkDir->DebugString().c_str());
    // block_array : we support both memory access and block cache, which depends on load config
    // sort_array & hash_table : only support memory access
    file_system::FileReaderPtr fileReader =
        indexConfig->GetPrimaryKeyIndexType() == pk_block_array
            ? pkDir->CreateFileReader(plan->GetTargetFileName(), file_system::FSOT_LOAD_CONFIG)
            : pkDir->CreateFileReader(plan->GetTargetFileName(), file_system::FSOT_MEM_ACCESS);

    IE_LOG(INFO, "end direct load pk in [%s], used[%.3f]s", pkDir->DebugString().c_str(), timer.done_sec());
    return fileReader;
}

template <typename Key>
inline file_system::FileReaderPtr PrimaryKeyLoader<Key>::Load(const PrimaryKeyLoadPlanPtr& plan,
                                                              const config::PrimaryKeyIndexConfigPtr& indexConfig)
{
    assert(plan);
    if (plan->CanDirectLoad()) {
        return DirectLoad(plan, indexConfig);
    }

    file_system::DirectoryPtr directory = plan->GetTargetFileDirectory();
    std::string targetFileName = plan->GetTargetFileName();
    file_system::FileReaderPtr sliceFileReader = directory->CreateFileReader(targetFileName, file_system::FSOT_SLICE);
    if (sliceFileReader) {
        return sliceFileReader;
    }

    return CreateTargetSliceFileReader(plan, indexConfig);
}

template <typename Key>
inline file_system::FileReaderPtr
PrimaryKeyLoader<Key>::CreateTargetSliceFileReader(const PrimaryKeyLoadPlanPtr& plan,
                                                   const config::PrimaryKeyIndexConfigPtr& indexConfig)
{
    IE_LOG(INFO, "Begin load pk files: [%s]", plan->GetTargetFileName().c_str());
    typedef std::shared_ptr<PrimaryKeyIterator<Key>> PrimaryKeyIteratorPtr;
    PrimaryKeyIteratorPtr pkIterator(new CombineSegmentsPrimaryKeyIterator<Key>(indexConfig));
    pkIterator->Init(plan->GetLoadSegmentDatas());

    PrimaryKeyFormatterPtr pkFormatter = PrimaryKeyFormatterCreator<Key>::CreatePKFormatterByLoadMode(indexConfig);

    // we assert pkFormatter can only use hash table load mode in this case
    assert(indexConfig->GetPKLoadStrategyParam()->GetPrimaryKeyLoadMode() ==
           config::PrimaryKeyLoadStrategyParam::HASH_TABLE);
    size_t sliceFileLen = pkFormatter->CalculatePkSliceFileLen(pkIterator->GetPkCount(), plan->GetDocCount());
    std::string targetFileName = plan->GetTargetFileName();
    file_system::DirectoryPtr directory = plan->GetTargetFileDirectory();
    file_system::FileWriterPtr sliceFileWriter =
        directory->CreateFileWriter(targetFileName, file_system::WriterOption::Slice(sliceFileLen, 1));
    pkFormatter->DeserializeToSliceFile(pkIterator, sliceFileWriter, sliceFileLen);
    auto reader = directory->CreateFileReader(targetFileName, file_system::FSOT_SLICE);
    sliceFileWriter->Close().GetOrThrow();
    IE_LOG(INFO, "End load pk files: [%s]", targetFileName.c_str());
    return reader;
}

template <typename Key>
size_t PrimaryKeyLoader<Key>::EstimateLoadSize(const PrimaryKeyLoadPlanPtr& plan,
                                               const config::PrimaryKeyIndexConfigPtr& indexConfig,
                                               const index_base::Version& lastLoadVersion)
{
    file_system::DirectoryPtr directory = plan->GetTargetFileDirectory();
    std::string targetFileName = plan->GetTargetFileName();
    // when last load version is invalid may be force reopen
    if (lastLoadVersion != index_base::Version(INVALID_VERSION) && directory->IsExistInCache(targetFileName)) {
        // already load
        return 0;
    }
    PrimaryKeyFormatterPtr pkFormatter = PrimaryKeyFormatterCreator<Key>::CreatePKFormatterByLoadMode(indexConfig);
    if (plan->CanDirectLoad()) {
        return pkFormatter->EstimateDirectLoadSize(plan);
    }
    size_t docCount = 0;
    size_t pkCount = 0;
    index_base::SegmentDataVector segmentDatas = plan->GetLoadSegmentDatas();

    for (size_t i = 0; i < segmentDatas.size(); i++) {
        file_system::DirectoryPtr directory = GetPrimaryKeyDirectory(segmentDatas[i].GetDirectory(), indexConfig);

        uint32_t segDocCount = segmentDatas[i].GetSegmentInfo()->docCount;
        pkCount += pkFormatter->EstimatePkCount(directory->GetFileLength(PRIMARY_KEY_DATA_FILE_NAME), segDocCount);

        docCount += segDocCount;
    }
    return pkFormatter->CalculatePkSliceFileLen(pkCount, docCount);
}

template <typename Key>
file_system::FileReaderPtr PrimaryKeyLoader<Key>::OpenPKDataFile(const file_system::DirectoryPtr& pkDir,
                                                                 const PrimaryKeyIndexType& pkType)
{
    file_system::FileReaderPtr fileReader = pkDir->CreateFileReader(
        PRIMARY_KEY_DATA_FILE_NAME, file_system::ReaderOption::NoCache(file_system::FSOT_BUFFERED));
    assert(fileReader);
    if (pkType == pk_block_array) {
        // needn't check pk data size, block_array_reader will check
        return fileReader;
    }
    size_t keyValueLen = sizeof(PKPair);
    if (fileReader->GetLength() % keyValueLen != 0) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "invalid pk data file, fileLength[%lu], keyValueLen[%lu]",
                             fileReader->GetLength(), sizeof(PKPair));
    }
    return fileReader;
}

template <typename Key>
file_system::DirectoryPtr
PrimaryKeyLoader<Key>::GetPrimaryKeyDirectory(const file_system::DirectoryPtr& segmentDirectory,
                                              const config::IndexConfigPtr& indexConfig)
{
    assert(segmentDirectory);
    file_system::DirectoryPtr indexDirectory = segmentDirectory->GetDirectory(INDEX_DIR_NAME, true);
    assert(indexDirectory);
    file_system::DirectoryPtr pkDirectory = indexDirectory->GetDirectory(indexConfig->GetIndexName(), true);
    return pkDirectory;
}
}} // namespace indexlib::index

#endif //__INDEXLIB_PRIMARY_KEY_LOADER_H
