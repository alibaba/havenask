#include "indexlib/index/normal/inverted_index/truncate/bucket_map.h"
#include <math.h>
#include "indexlib/file_system/buffered_file_reader.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/file_system/directory.h"

using namespace std;
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BucketMap);

const std::string BucketMap::BUKCET_MAP_FILE_NAME_PREFIX = "bucket_map_";

BucketMap::BucketMap() 
{
    mSortValues = NULL;
    mSize = 0;
    mBucketCount = 0;
    mBucketSize = 0;
}

BucketMap::~BucketMap() 
{
    if (mSortValues)
    {
        delete[] mSortValues;
        mSortValues = NULL;
    }
}

bool BucketMap::Init(uint32_t size)
{
    assert(!mSortValues);
    if (0 == size)
    {
        return false;
    }
    mSortValues = new uint32_t[size];
    mSize = size;

    mBucketCount = GetBucketCount(size);
    mBucketSize = (mSize + mBucketCount - 1) / mBucketCount;
    assert(mBucketSize * mBucketCount >= size);
    return true;
}

uint32_t BucketMap::GetBucketCount(uint32_t size)
{
    return (uint32_t)sqrt(size);
}

void BucketMap::Store(const std::string &filePath) const
{
    file_system::BufferedFileWriter bufferedFileWriter;
    bufferedFileWriter.Open(filePath);
    bufferedFileWriter.Write(&mSize, sizeof(mSize));
    bufferedFileWriter.Write(mSortValues, sizeof(mSortValues[0]) * mSize);
    bufferedFileWriter.Close();
}

void BucketMap::Store(const DirectoryPtr &directory, const string &fileName) const
{
    FileWriterPtr writer = directory->CreateFileWriter(fileName);
    writer->Write(&mSize, sizeof(mSize));
    writer->Write(mSortValues, sizeof(mSortValues[0]) * mSize);
    writer->Close();
}

bool BucketMap::Load(const std::string &filePath)
{
    BufferedFileReader* reader = new BufferedFileReader;
    reader->Open(filePath);
    FileReaderPtr readerPtr(reader);
    return InnerLoad(readerPtr);
}

bool BucketMap::Load(const DirectoryPtr &directory, const string &fileName)
{
    FileReaderPtr reader = directory->CreateFileReader(fileName, FSOpenType::FSOT_BUFFERED);
    reader->Open();
    return InnerLoad(reader);
}

bool BucketMap::InnerLoad(file_system::FileReaderPtr& reader)
{
    uint32_t expectSize = sizeof(mSize);
    if (reader->Read(&mSize, expectSize) != expectSize)
    {
        reader->Close();
        return false;
    }
    if (!Init(mSize))
    {
        reader->Close();        
        return false;
    }
    expectSize = sizeof(mSortValues[0]) * mSize;
    if (reader->Read(mSortValues, expectSize) != expectSize)
    {
        reader->Close();
        return false;
    }
    reader->Close();    
    return true;
}

int64_t BucketMap::EstimateMemoryUse() const
{
    return mSize * sizeof(uint32_t);
}

IE_NAMESPACE_END(index);

