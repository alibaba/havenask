#include "indexlib/index/normal/inverted_index/builtin_index/range/range_info.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/index_define.h"
#include <autil/StringUtil.h>

using namespace std;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, RangeInfo);

RangeInfo::RangeInfo()
    : mMinNumber(std::numeric_limits<uint64_t>::max())
    , mMaxNumber(0)
{
}

RangeInfo::~RangeInfo() 
{
}

void RangeInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    uint64_t minNumber = mMinNumber;
    uint64_t maxNumber = mMaxNumber;
    if (json.GetMode() == TO_JSON)
    {
        json.Jsonize("min_number", minNumber);
        json.Jsonize("max_number", maxNumber);
    }
    else
    {
        json.Jsonize("min_number", minNumber, minNumber);
        json.Jsonize("max_number", maxNumber, maxNumber);
        mMinNumber = minNumber;
        mMaxNumber = maxNumber;
    }
}

bool RangeInfo::Load(const DirectoryPtr& directory)
{
    if (!directory->IsExist(RANGE_INFO_FILE_NAME))
    {
        return false;
    }

    FileReaderPtr fileReader = directory->CreateFileReader(
            RANGE_INFO_FILE_NAME, FSOT_IN_MEM);

    int64_t fileLen = fileReader->GetLength();
    string content;
    content.resize(fileLen);
    char *data = const_cast<char*>(content.c_str());
    fileReader->Read(data, fileLen, 0);
    autil::legacy::FromJsonString(*this, content);
    return true;
}

void RangeInfo::Store(const DirectoryPtr& directory)
{
    string content = autil::legacy::ToJsonString(*this);
    directory->Store(RANGE_INFO_FILE_NAME, content, false);
}




IE_NAMESPACE_END(index);

