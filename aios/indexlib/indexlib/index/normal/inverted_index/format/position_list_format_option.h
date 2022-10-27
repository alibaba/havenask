#ifndef __INDEXLIB_POSITION_LIST_FORMAT_OPTION_H
#define __INDEXLIB_POSITION_LIST_FORMAT_OPTION_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"

IE_NAMESPACE_BEGIN(index);

class PositionListFormatOption
{
public:
    PositionListFormatOption(optionflag_t optionFlag = OPTION_FLAG_ALL);

public:
    void Init(optionflag_t optionFlag);
    bool HasPositionList() const { return mHasPositionList == 1; }
    bool HasPositionPayload() const { return mHasPositionPayload == 1; }
    bool HasTfBitmap() const { return mHasTfBitmap == 1; }
    bool operator == (const PositionListFormatOption& right) const;
private:
    uint8_t mHasPositionList:1;
    uint8_t mHasPositionPayload:1;
    uint8_t mHasTfBitmap:1;
    uint8_t mUnused:5;

    friend class PositionListEncoderTest;
    friend class JsonizablePositionListFormatOption;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PositionListFormatOption);

class JsonizablePositionListFormatOption : public autil::legacy::Jsonizable
{
public:
    JsonizablePositionListFormatOption(PositionListFormatOption option = PositionListFormatOption())
        : mPositionListFormatOption(option)
    {}

    const PositionListFormatOption& GetPositionListFormatOption() const
    { return mPositionListFormatOption; }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

private:
    PositionListFormatOption mPositionListFormatOption;
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSITION_LIST_FORMAT_OPTION_H
