#include "indexlib/index/normal/inverted_index/format/position_list_format_option.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PositionListFormatOption);

PositionListFormatOption::PositionListFormatOption(optionflag_t optionFlag) 
{
    Init(optionFlag);
}

void PositionListFormatOption::Init(optionflag_t optionFlag)
{
    //we think string and number etc has no position list 
    //they are checked in schema_configurator

    mHasPositionList = optionFlag & of_position_list ? 1 : 0;
    if ((optionFlag & of_position_list)
        && (optionFlag & of_position_payload))
    {
        mHasPositionPayload = 1;
    }
    else
    {
        mHasPositionPayload = 0;
    }

    if ((optionFlag & of_term_frequency)
        && (optionFlag & of_tf_bitmap))
    {
        mHasTfBitmap = 1;
    }
    else
    {
        mHasTfBitmap = 0;
    }

    mUnused = 0;
}

bool PositionListFormatOption::operator == (
        const PositionListFormatOption& right) const
{
    return mHasPositionList == right.mHasPositionList
        && mHasPositionPayload == right.mHasPositionPayload
        && mHasTfBitmap == right.mHasTfBitmap;
}


void JsonizablePositionListFormatOption::Jsonize(
        autil::legacy::Jsonizable::JsonWrapper& json)
{
    bool hasPositionList;
    bool hasPositionPayload;
    bool hasTfBitmap;

    if (json.GetMode() == FROM_JSON)
    {
        json.Jsonize("has_position_list", hasPositionList);
        json.Jsonize("has_position_payload", hasPositionPayload);
        json.Jsonize("has_term_frequency_bitmap", hasTfBitmap);

        mPositionListFormatOption.mHasPositionList = hasPositionList ? 1 : 0;
        mPositionListFormatOption.mHasPositionPayload = hasPositionPayload ? 1 : 0;
        mPositionListFormatOption.mHasTfBitmap = hasTfBitmap ? 1 : 0;
    }
    else
    {
        hasPositionList = mPositionListFormatOption.mHasPositionList == 1;
        hasPositionPayload = mPositionListFormatOption.mHasPositionPayload == 1;
        hasTfBitmap = mPositionListFormatOption.mHasTfBitmap == 1;

        json.Jsonize("has_position_list", hasPositionList);
        json.Jsonize("has_position_payload", hasPositionPayload);
        json.Jsonize("has_term_frequency_bitmap", hasTfBitmap);
    }
}

IE_NAMESPACE_END(index);

