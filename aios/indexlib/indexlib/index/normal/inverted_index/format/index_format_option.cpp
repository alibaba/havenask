#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/index_define.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, IndexFormatOption);

IndexFormatOption::IndexFormatOption() 
{
    mHasSectionAttribute = false;
    mHasBitmapIndex = false;
    mIsNumberIndex = false;
}

IndexFormatOption::~IndexFormatOption() 
{
}

bool IndexFormatOption::IsNumberIndex(const config::IndexConfigPtr& indexConfigPtr)
{
    IndexType indexType = indexConfigPtr->GetIndexType();
    switch (indexType)
    {
        // number index with origin key may cause postingTable confict
    case it_number:
    case it_number_int8:
    case it_number_uint8:
    case it_number_int16:
    case it_number_uint16:
    case it_number_int32:
    case it_number_uint32:
    case it_number_int64:
    case it_number_uint64:
        return true;

    default:
        return false;
    }
    return false;
}

void IndexFormatOption::Init(const config::IndexConfigPtr& indexConfigPtr) 
{
    assert(indexConfigPtr);
    optionflag_t optionFlag = indexConfigPtr->GetOptionFlag();
    //TODO: 
    if (indexConfigPtr->GetIndexType() == it_expack)
    {
        optionFlag |= of_fieldmap;
    }

    mIsNumberIndex = IndexFormatOption::IsNumberIndex(indexConfigPtr);
    //TODO: IndexConfig and IndexType should match, need add some testcase
    //TODO: no position in number and string index, assert(!(optionFlag & of_position_list))
    //TODO: why section attr depends on position list?

    mPostingFormatOption.Init(indexConfigPtr);

    PackageIndexConfigPtr packageIndexConfig = 
        std::tr1::dynamic_pointer_cast<config::PackageIndexConfig>(indexConfigPtr);     
    if (packageIndexConfig
        && packageIndexConfig->HasSectionAttribute())
    {
        mHasSectionAttribute = true;
    }        

    if (indexConfigPtr->GetHighFreqVocabulary())
    {
        mHasBitmapIndex = true;
    }
}

std::string IndexFormatOption::ToString(const IndexFormatOption& option)
{
    JsonizableIndexFormatOption jsonizableOption(option);
    return autil::legacy::ToJsonString(jsonizableOption);
}

IndexFormatOption IndexFormatOption::FromString(const std::string& str)
{
    JsonizableIndexFormatOption jsonizableOption;
    autil::legacy::FromJsonString(jsonizableOption, str); 
    return jsonizableOption.GetIndexFormatOption();
}

bool IndexFormatOption::Load(const file_system::DirectoryPtr& directory)
{
    if (!directory->IsExist(INDEX_FORMAT_OPTION_FILE_NAME))
    {
        return false;
    }

    file_system::FileReaderPtr fileReader = directory->CreateFileReader(
            INDEX_FORMAT_OPTION_FILE_NAME, file_system::FSOT_IN_MEM);

    int64_t fileLen = fileReader->GetLength();
    string content;
    content.resize(fileLen);
    char *data = const_cast<char*>(content.c_str());
    fileReader->Read(data, fileLen, 0);
    *this = FromString(content);
    return true;
}

void IndexFormatOption::Store(const file_system::DirectoryPtr& directory)
{
    string content = ToString(*this);
    directory->Store(INDEX_FORMAT_OPTION_FILE_NAME, content, false);
}

JsonizableIndexFormatOption::JsonizableIndexFormatOption(IndexFormatOption option)
    : mOption(option)
    , mPostingFormatOption(option.mPostingFormatOption)
{
}

const IndexFormatOption& JsonizableIndexFormatOption::GetIndexFormatOption() const
{
    return mOption;
}

void JsonizableIndexFormatOption::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("has_section_attribute", mOption.mHasSectionAttribute);
    json.Jsonize("has_bitmap_index", mOption.mHasBitmapIndex);
    json.Jsonize("posting_format_option", mPostingFormatOption);
    if (json.GetMode() == FROM_JSON)
    {
        mOption.mPostingFormatOption = mPostingFormatOption.GetPostingFormatOption();
    }
}

IE_NAMESPACE_END(index);
