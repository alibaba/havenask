#ifndef __INDEXLIB_DOC_LIST_FORMAT_OPTION_H
#define __INDEXLIB_DOC_LIST_FORMAT_OPTION_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"

IE_NAMESPACE_BEGIN(index);

class DocListFormatOption
{
public:
    DocListFormatOption(optionflag_t optionFlag = OPTION_FLAG_ALL);

public:
    void Init(optionflag_t optionFlag);
    bool HasTermFrequency() const { return mHasTf == 1; }
    bool HasTfList() const { return mHasTfList == 1; }
    bool HasTfBitmap() const { return mHasTfBitmap == 1; }
    bool HasDocPayload() const { return mHasDocPayload == 1; }
    bool HasFieldMap() const { return mHasFieldMap == 1; }
    bool operator == (const DocListFormatOption& right) const;
private:
    uint8_t mHasTf:1;
    uint8_t mHasTfList:1;
    uint8_t mHasTfBitmap:1;
    uint8_t mHasDocPayload:1;
    uint8_t mHasFieldMap:1;
    uint8_t mUnused:3;

    friend class DocListEncoderTest;
    friend class DocListMemoryBufferTest;
    friend class JsonizableDocListFormatOption;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocListFormatOption);

class JsonizableDocListFormatOption : public autil::legacy::Jsonizable
{
public:
    JsonizableDocListFormatOption(DocListFormatOption option = DocListFormatOption())
        : mDocListFormatOption(option)
    {}
    const DocListFormatOption& GetDocListFormatOption() const
    { return mDocListFormatOption; }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

private:
    DocListFormatOption mDocListFormatOption;
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DOC_LIST_FORMAT_OPTION_H
