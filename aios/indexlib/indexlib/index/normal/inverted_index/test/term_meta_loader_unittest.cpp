#include "indexlib/index/normal/inverted_index/test/term_meta_loader_unittest.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index_define.h"
#include "indexlib/index/normal/inverted_index/format/term_meta_loader.h"
#include "indexlib/index/normal/inverted_index/format/term_meta_dumper.h"
#include "indexlib/common/byte_slice_reader.h"

using namespace std;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);


IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TermMetaLoaderTest);

TermMetaLoaderTest::TermMetaLoaderTest()
{
}

TermMetaLoaderTest::~TermMetaLoaderTest()
{
}

void TermMetaLoaderTest::CaseSetUp()
{
    mRootPath = GET_TEST_DATA_PATH();
    mRootDirectory = DirectoryCreator::Create(mRootPath);
}

void TermMetaLoaderTest::CaseTearDown()
{
}

void TermMetaLoaderTest::TestStoreSize()
{
    {
        TermMeta termMeta(1, 2, 3);
        TermMetaDumper tmDumper;
        //docFreq < 127 compressSize = 1,
        //totalTermFreq < 127 compressSize = 1
        ASSERT_EQ((uint32_t)6, tmDumper.CalculateStoreSize(termMeta));
    }
    
    {
        TermMeta termMeta(128, 2, 3);
        TermMetaDumper tmDumper;
        //docFreq > 127 compressSize = 2,
        //totalTermFreq < 127 compressSize = 1
        ASSERT_EQ((uint32_t)7, tmDumper.CalculateStoreSize(termMeta));
    }
}

void TermMetaLoaderTest::TestDump()
{
    TermMeta termMeta(1, 2, 3);
    CheckDump(termMeta);
}

void TermMetaLoaderTest::TestLoad()
{
    TermMeta oldTermMeta(1, 2, 3);
    DumpTermMeta(oldTermMeta, mRootDirectory, POSTING_FILE_NAME);
    
    {
        file_system::DirectoryPtr partitionDirectory = 
            GET_PARTITION_DIRECTORY();
        file_system::FileReaderPtr reader = 
            partitionDirectory->CreateFileReader(
                    POSTING_FILE_NAME, file_system::FSOT_BUFFERED);

        TermMeta newTermMeta;
        TermMetaLoader tmLoader;
        tmLoader.Load(reader, newTermMeta);
        ASSERT_EQ(oldTermMeta.mDocFreq, newTermMeta.mDocFreq);
        ASSERT_EQ(oldTermMeta.mTotalTermFreq, newTermMeta.mTotalTermFreq);
        ASSERT_EQ(oldTermMeta.mPayload, newTermMeta.mPayload);
        reader->Close();
    }

    //test cursor load
    string fileContent;
    mRootDirectory->Load(POSTING_FILE_NAME, fileContent);
    {
        uint8_t* dataCursor = (uint8_t*)fileContent.c_str();
        size_t leftSize = fileContent.size();

        TermMeta newTermMeta;
        TermMetaLoader tmLoader;
        tmLoader.Load(dataCursor, leftSize, newTermMeta);
        ASSERT_TRUE(oldTermMeta == newTermMeta);
        
    }

    {
        uint8_t* dataCursor = (uint8_t*)fileContent.c_str();
        size_t leftSize = fileContent.size();

        TermMeta newTermMeta;
        TermMetaLoader tmLoader;
        tmLoader.Load(dataCursor, leftSize, newTermMeta);
        
        ASSERT_EQ(oldTermMeta.mDocFreq, newTermMeta.mDocFreq);
        ASSERT_EQ(oldTermMeta.mTotalTermFreq, newTermMeta.mTotalTermFreq);
        ASSERT_EQ(oldTermMeta.mPayload, newTermMeta.mPayload);
    }

    // {
    //     file_system::DirectoryPtr partitionDirectory = 
    //         GET_PARTITION_DIRECTORY();        
    //     auto reader
    //         = partitionDirectory->CreateFileReader(POSTING_FILE_NAME, file_system::FSOT_CACHE);
    //     auto sliceList = reader->Read(reader->GetLength(), 0);
    //     ByteSliceReader sliceReader(sliceList);

    //     TermMeta newTermMeta;
    //     TermMetaLoader tmLoader;
    //     tmLoader.Load(&sliceReader, newTermMeta);
    //     ASSERT_TRUE(oldTermMeta == newTermMeta);
    //     sliceReader.Close();
    //     delete sliceList;
    // }

    // {
    //     file_system::DirectoryPtr partitionDirectory = 
    //         GET_PARTITION_DIRECTORY();        
    //     auto reader
    //         = partitionDirectory->CreateFileReader(POSTING_FILE_NAME, file_system::FSOT_CACHE);
    //     auto sliceList = reader->Read(reader->GetLength(), 0);
    //     ByteSliceReader sliceReader(sliceList);
        
    //     TermMeta newTermMeta;
    //     TermMetaLoader tmLoader;
    //     tmLoader.Load(&sliceReader, newTermMeta);
    //     ASSERT_EQ(oldTermMeta.mDocFreq, newTermMeta.mDocFreq);
    //     ASSERT_EQ(oldTermMeta.mTotalTermFreq, newTermMeta.mTotalTermFreq);
    //     ASSERT_EQ(oldTermMeta.mPayload, newTermMeta.mPayload);
    //     sliceReader.Close();
    //     delete sliceList;
    // }
}

void TermMetaLoaderTest::CheckDump(TermMeta& termMeta)
{
    FileWriterPtr postingFileWriter = mRootDirectory->CreateFileWriter(POSTING_FILE_NAME);
    
    TermMetaDumper tmDumper;
    tmDumper.Dump(postingFileWriter, termMeta);
    
    ASSERT_EQ((int64_t)tmDumper.CalculateStoreSize(termMeta), postingFileWriter->GetLength());
    postingFileWriter->Close();
    mRootDirectory->RemoveFile(POSTING_FILE_NAME, false);
}

void TermMetaLoaderTest::DumpTermMeta(
    const TermMeta& termMeta, const DirectoryPtr& directory, const string& fileName)
{
    FileWriterPtr postingFileWriter = directory->CreateFileWriter(fileName);
    TermMetaDumper tmDumper;
    tmDumper.Dump(postingFileWriter, termMeta);
    postingFileWriter->Close();
}
IE_NAMESPACE_END(index);

