#include "indexlib/index/normal/inverted_index/builtin_index/date/time_range_info.h"
#include "indexlib/common/field_format/date/date_term.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/index_define.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TimeRangeInfo);

TimeRangeInfo::TimeRangeInfo()
    : mMinTime(4102416000000)
    , mMaxTime(0)
{
}

TimeRangeInfo::~TimeRangeInfo() 
{
}

void TimeRangeInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    string minTimeStr, maxTimeStr;
    if (json.GetMode() == FROM_JSON)
    {
        json.Jsonize("min_time", minTimeStr, "130-1-1-0-0-0-0");
        json.Jsonize("max_time", maxTimeStr, "0-0-0-0-0-0-0");
        mMinTime = DateTerm::FromString(minTimeStr).GetKey();
        mMaxTime = DateTerm::FromString(maxTimeStr).GetKey();
    }
    else
    {
        minTimeStr = DateTerm::ToString(DateTerm(mMinTime));
        maxTimeStr = DateTerm::ToString(DateTerm(mMaxTime));
        json.Jsonize("min_time", minTimeStr);
        json.Jsonize("max_time", maxTimeStr);
    }
}

bool TimeRangeInfo::Load(const DirectoryPtr& directory)
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

void TimeRangeInfo::Store(const DirectoryPtr& directory)
{
    string content = autil::legacy::ToJsonString(*this);
    directory->Store(RANGE_INFO_FILE_NAME, content, false);
}




IE_NAMESPACE_END(index);

