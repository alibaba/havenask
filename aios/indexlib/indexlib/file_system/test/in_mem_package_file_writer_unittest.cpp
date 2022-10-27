#include "indexlib/file_system/test/in_mem_package_file_writer_unittest.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"

using namespace std;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, InMemPackageFileWriterTest);

InMemPackageFileWriterTest::InMemPackageFileWriterTest()
{
}

InMemPackageFileWriterTest::~InMemPackageFileWriterTest()
{
}

void InMemPackageFileWriterTest::CaseSetUp()
{
    mMemoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController();
}

void InMemPackageFileWriterTest::CaseTearDown()
{
}

void InMemPackageFileWriterTest::TestSimpleProcess()
{
    string basePath = GET_TEST_DATA_PATH() + "in_mem_dir/";
    InMemStoragePtr inMemStorage(new InMemStorage(basePath, mMemoryController, true, true));
    FileSystemOptions options;
    inMemStorage->Init(options);

    string packageFilePath = basePath + "package_file";
    InMemPackageFileWriterPtr packageFileWriter(new InMemPackageFileWriter(
                    packageFilePath, inMemStorage.get(), mMemoryController));

    WriteData(packageFileWriter, "file1", "abc");
    WriteData(packageFileWriter, "file2", "bdc");

    packageFileWriter->Close();

    CheckData(inMemStorage, basePath + "file1", "abc");
    CheckData(inMemStorage, basePath + "file2", "bdc");

    inMemStorage->Sync(true);

    string packageFileDataPath = packageFilePath + 
                                 PACKAGE_FILE_DATA_SUFFIX + "0";
    string packageFileMetaPath = packageFilePath + PACKAGE_FILE_META_SUFFIX;
    ASSERT_TRUE(FileSystemWrapper::IsExist(packageFileDataPath));
    ASSERT_TRUE(FileSystemWrapper::IsExist(packageFileMetaPath));
}

void InMemPackageFileWriterTest::TestCloseException()
{
    string basePath = GET_TEST_DATA_PATH() + "in_mem_dir/";
    InMemStoragePtr inMemStorage(new InMemStorage(basePath, mMemoryController, true, true));
    FileSystemOptions options;
    inMemStorage->Init(options);
    
    string packageFilePath = basePath + "package_file";
    InMemPackageFileWriterPtr packageFileWriter(new InMemPackageFileWriter(
                    packageFilePath, inMemStorage.get(), mMemoryController));

    FileWriterPtr fileWriter = packageFileWriter->CreateInnerFileWriter("file1");
    fileWriter->Write("abcd", 4);

    // innerWriter not call close
    ASSERT_THROW(packageFileWriter->Close(), InconsistentStateException);

    fileWriter->Close();
    ASSERT_NO_THROW(packageFileWriter->Close());
}

void InMemPackageFileWriterTest::TestCreateInnerFileWriter()
{
    string basePath = GET_TEST_DATA_PATH() + "in_mem_dir/";
    InMemStoragePtr inMemStorage(new InMemStorage(basePath, mMemoryController, true, true));
    FileSystemOptions options;
    inMemStorage->Init(options);

    string packageFilePath = basePath + "package_file";
    InMemPackageFileWriterPtr packageFileWriter(new InMemPackageFileWriter(
                    packageFilePath, inMemStorage.get(), mMemoryController));

    FileWriterPtr fileWriter = packageFileWriter->CreateInnerFileWriter("file1");
    ASSERT_ANY_THROW(packageFileWriter->CreateInnerFileWriter("/file1"));
    ASSERT_ANY_THROW(packageFileWriter->CreateInnerFileWriter("//file1"));

    fileWriter->Close();
    fileWriter = packageFileWriter->CreateInnerFileWriter("index//file1");

    ASSERT_ANY_THROW(packageFileWriter->CreateInnerFileWriter("index/file1"));
    ASSERT_ANY_THROW(packageFileWriter->CreateInnerFileWriter("index///file1"));
    
    fileWriter->Close();
    packageFileWriter->Close();
}

void InMemPackageFileWriterTest::TestCreateInnerFileWithDuplicateFilePath()
{
    string basePath = GET_TEST_DATA_PATH() + "in_mem_dir/";
    InMemStoragePtr inMemStorage(new InMemStorage(basePath, mMemoryController,true, true));
    FileSystemOptions options;
    inMemStorage->Init(options);

    {
        string packageFilePath = basePath + "package_file";
        InMemPackageFileWriterPtr packageFileWriter(new InMemPackageFileWriter(
                        packageFilePath, inMemStorage.get(), mMemoryController));

        FileWriterPtr innerFileWriter = packageFileWriter->CreateInnerFileWriter("file1");
        innerFileWriter->Close();
        packageFileWriter->Close();

        string filePath = basePath + "file1";
        FileWriterPtr normFileWriter1 = inMemStorage->CreateFileWriter(filePath); 
        ASSERT_THROW(normFileWriter1->Close(), ExistException);
    }

    {
        string packageFilePath = basePath + "package_file_1";
        InMemPackageFileWriterPtr packageFileWriter(new InMemPackageFileWriter(
                        packageFilePath, inMemStorage.get(), mMemoryController));

        string filePath = basePath + "file2";
        FileWriterPtr normFileWriter2 = inMemStorage->CreateFileWriter(filePath); 
        normFileWriter2->Close();

        FileWriterPtr innerFileWriter = packageFileWriter->CreateInnerFileWriter("file2");
        innerFileWriter->Close();
        ASSERT_THROW(packageFileWriter->Close(), ExistException);
    }
}

void InMemPackageFileWriterTest::TestCreateEmptyPackage()
{
    string basePath = GET_TEST_DATA_PATH() + "in_mem_dir/";
    InMemStoragePtr inMemStorage(new InMemStorage(basePath, mMemoryController, true, true));
    FileSystemOptions options;
    inMemStorage->Init(options);
    
    string packageFilePath = basePath + "package_file";
    InMemPackageFileWriterPtr packageFileWriter(new InMemPackageFileWriter(
                    packageFilePath, inMemStorage.get(), mMemoryController));

    packageFileWriter->Close();
    inMemStorage->Sync(true);

    string packageFileDataPath = packageFilePath + 
                                 PACKAGE_FILE_DATA_SUFFIX + "0";
    string packageFileMetaPath = packageFilePath + PACKAGE_FILE_META_SUFFIX;
    ASSERT_TRUE(FileSystemWrapper::IsExist(packageFileDataPath));
    ASSERT_TRUE(FileSystemWrapper::IsExist(packageFileMetaPath));   
}

void InMemPackageFileWriterTest::WriteData(
        const PackageFileWriterPtr& writer, 
        const string& fileName, const string& value)
{
    FileWriterPtr fileWriter = writer->CreateInnerFileWriter(fileName);
    fileWriter->Write(value.c_str(), value.size());
    fileWriter->Close();
}

void InMemPackageFileWriterTest::CheckData(
        const InMemStoragePtr& inMemStorage,
        const string& filePath, const string& expectValue)
{
    FileReaderPtr fileReader = inMemStorage->CreateFileReader(
            filePath, FSOT_IN_MEM);
    ASSERT_TRUE(fileReader);
    ASSERT_EQ(expectValue.size(), fileReader->GetLength());

    string value;
    value.resize(expectValue.size());
    fileReader->Read((void*)value.data(), expectValue.size());
    ASSERT_EQ(expectValue, value);

    fileReader->Close();
}

IE_NAMESPACE_END(file_system);

