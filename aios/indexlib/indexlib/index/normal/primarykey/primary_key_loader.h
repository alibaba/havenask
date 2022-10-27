#ifndef __INDEXLIB_PRIMARY_KEY_LOADER_H
#define __INDEXLIB_PRIMARY_KEY_LOADER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/index/normal/primarykey/primary_key_load_plan.h"
#include "indexlib/index/normal/primarykey/primary_key_formatter_creator.h"
#include "indexlib/index/normal/primarykey/primary_key_formatter.h"
#include "indexlib/index/normal/primarykey/primary_key_segment_reader_typed.h"
#include "indexlib/index/normal/primarykey/combine_segments_primary_key_iterator.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(index);

template<typename Key>
class PrimaryKeyLoader
{
public:
    PrimaryKeyLoader(){}
    ~PrimaryKeyLoader(){}

public:
    static file_system::FileReaderPtr Load(const PrimaryKeyLoadPlanPtr& plan,
            const config::PrimaryKeyIndexConfigPtr& indexConfig);
    static size_t EstimateLoadSize(const PrimaryKeyLoadPlanPtr& plan,
                                   const config::PrimaryKeyIndexConfigPtr& indexConfig,
                                   const index_base::Version& lastLoadVersion);
    static file_system::DirectoryPtr GetPrimaryKeyDirectory(
            const file_system::DirectoryPtr& segmentDir,
            const config::IndexConfigPtr& indexConfig);

    static file_system::FileReaderPtr OpenPKDataFile(
            const file_system::DirectoryPtr& segmentDir);

private:
    static file_system::FileReaderPtr DirectLoad(
        const PrimaryKeyLoadPlanPtr& plan,
        const config::PrimaryKeyIndexConfigPtr& indexConfig);
    
    static file_system::FileReaderPtr CreateTargetSliceFileReader(
            const PrimaryKeyLoadPlanPtr& plan,
            const config::PrimaryKeyIndexConfigPtr& indexConfig);

private:
    typedef std::tr1::shared_ptr<PrimaryKeyFormatter<Key> > PrimaryKeyFormatterPtr;
    typedef typename PrimaryKeyFormatter<Key>::PKPair PKPair;

private:
    IE_LOG_DECLARE();
};
IE_LOG_SETUP_TEMPLATE(index, PrimaryKeyLoader);
///////////////////////////////////////////////////////////

template <typename Key>
file_system::FileReaderPtr PrimaryKeyLoader<Key>::DirectLoad(
    const PrimaryKeyLoadPlanPtr& plan,
    const config::PrimaryKeyIndexConfigPtr& indexConfig)
{
    assert(plan);
    assert(indexConfig);
    file_system::DirectoryPtr pkDir = plan->GetTargetFileDirectory();
    IE_LOG(INFO, "Begin direct load pk in [%s]", pkDir->GetPath().c_str());
    file_system::FileReaderPtr fileReader = pkDir->CreateIntegratedFileReader(
            plan->GetTargetFileName());
    IE_LOG(INFO, "end direct load pk in [%s]", pkDir->GetPath().c_str());
    return fileReader;
}

template<typename Key>
inline file_system::FileReaderPtr PrimaryKeyLoader<Key>::Load(
        const PrimaryKeyLoadPlanPtr& plan,
        const config::PrimaryKeyIndexConfigPtr& indexConfig)
{
    assert(plan);
    if (plan->CanDirectLoad())
    {
        return DirectLoad(plan, indexConfig);
    }
    
    file_system::DirectoryPtr directory = plan->GetTargetFileDirectory();
    std::string targetFileName = plan->GetTargetFileName();
    file_system::SliceFilePtr sliceFile 
        = directory->GetSliceFile(targetFileName);
    if (sliceFile)
    {
        return sliceFile->CreateSliceFileReader();
    }

    return CreateTargetSliceFileReader(plan, indexConfig);
}

template<typename Key>
inline file_system::FileReaderPtr 
PrimaryKeyLoader<Key>::CreateTargetSliceFileReader(
        const PrimaryKeyLoadPlanPtr& plan,
        const config::PrimaryKeyIndexConfigPtr& indexConfig)
{
    IE_LOG(INFO, "Begin load pk files: [%s]", plan->GetTargetFileName().c_str());
    typedef std::tr1::shared_ptr<PrimaryKeyIterator<Key> > PrimaryKeyIteratorPtr;
    PrimaryKeyIteratorPtr pkIterator(
            new CombineSegmentsPrimaryKeyIterator<Key>(indexConfig));
    pkIterator->Init(plan->GetLoadSegmentDatas());
    
    PrimaryKeyFormatterPtr pkFormatter =
        PrimaryKeyFormatterCreator<Key>::CreatePrimaryKeyFormatter(indexConfig);
    
    size_t sliceFileLen = pkFormatter->CalculatePkSliceFileLen(
        pkIterator->GetPkCount(), plan->GetDocCount());
    std::string targetFileName = plan->GetTargetFileName();
        file_system::DirectoryPtr directory = plan->GetTargetFileDirectory();
    file_system::SliceFilePtr sliceFile = directory->CreateSliceFile(
        targetFileName, sliceFileLen, 1);
    pkFormatter->DeserializeToSliceFile(pkIterator, sliceFile, sliceFileLen);
    
    IE_LOG(INFO, "End load pk files: [%s]", targetFileName.c_str());
    return sliceFile->CreateSliceFileReader();
}

template<typename Key>
size_t PrimaryKeyLoader<Key>::EstimateLoadSize(const PrimaryKeyLoadPlanPtr& plan,
                                               const config::PrimaryKeyIndexConfigPtr& indexConfig,
                                               const index_base::Version& lastLoadVersion)
{
    file_system::DirectoryPtr directory = plan->GetTargetFileDirectory();
    std::string targetFileName = plan->GetTargetFileName();
    //when last load version is invalid may be force reopen
    if (lastLoadVersion != INVALID_VERSION &&
        directory->IsExistInCache(targetFileName))
    {
        //already load
        return 0;
    }
    if (plan->CanDirectLoad())
    {
        file_system::DirectoryPtr pkDir = plan->GetTargetFileDirectory();
        assert(pkDir);
        return pkDir->EstimateIntegratedFileMemoryUse(plan->GetTargetFileName());
    }
    size_t docCount = 0;
    size_t pkCount = 0;    
    index_base::SegmentDataVector segmentDatas = plan->GetLoadSegmentDatas();
    PrimaryKeyFormatterPtr pkFormatter = 
        PrimaryKeyFormatterCreator<Key>::CreatePrimaryKeyFormatter(indexConfig);
    
    for (size_t i = 0; i < segmentDatas.size(); i++)
    {
        file_system::DirectoryPtr directory = GetPrimaryKeyDirectory(
                    segmentDatas[i].GetDirectory(), indexConfig);

        uint32_t segDocCount = segmentDatas[i].GetSegmentInfo().docCount;
        pkCount += pkFormatter->EstimatePkCount(
            directory->GetFileLength(PRIMARY_KEY_DATA_FILE_NAME), segDocCount);
        
        docCount += segDocCount;
    }
    return pkFormatter->CalculatePkSliceFileLen(pkCount, docCount);
}

template<typename Key>
file_system::FileReaderPtr PrimaryKeyLoader<Key>::OpenPKDataFile(
        const file_system::DirectoryPtr& pkDir)
{
    file_system::FileReaderPtr fileReader = pkDir->CreateFileReaderWithoutCache(
            PRIMARY_KEY_DATA_FILE_NAME, file_system::FSOT_BUFFERED);
    assert(fileReader);

    size_t keyValueLen = sizeof(PKPair);
    if (fileReader->GetLength() % keyValueLen != 0)
    {
        INDEXLIB_FATAL_ERROR(
                IndexCollapsed,
                "invalid pk data file, fileLength[%lu], keyValueLen[%lu]",
                fileReader->GetLength(), sizeof(PKPair));
    }
    return fileReader;
}

template<typename Key>
file_system::DirectoryPtr PrimaryKeyLoader<Key>::GetPrimaryKeyDirectory(
        const file_system::DirectoryPtr& segmentDirectory,
        const config::IndexConfigPtr& indexConfig)
{
    assert(segmentDirectory);
    file_system::DirectoryPtr indexDirectory =
        segmentDirectory->GetDirectory(INDEX_DIR_NAME, true);
    assert(indexDirectory);
    file_system::DirectoryPtr pkDirectory = 
        indexDirectory->GetDirectory(indexConfig->GetIndexName(), true);
    return pkDirectory;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_LOADER_H
