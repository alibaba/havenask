#include "indexlib/index/normal/inverted_index/format/doc_list_format_option.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DocListFormatOption);

DocListFormatOption::DocListFormatOption(optionflag_t optionFlag) 
{
    Init(optionFlag);
}

void DocListFormatOption::Init(optionflag_t optionFlag)
{
    mHasDocPayload = optionFlag & of_doc_payload ? 1 : 0;
    mHasFieldMap = optionFlag & of_fieldmap ? 1 : 0;
    if (optionFlag & of_term_frequency) 
    {
        mHasTf = 1;
        if (optionFlag & of_tf_bitmap) 
        {
            mHasTfBitmap = 1;
            mHasTfList = 0;
        }
        else
        {
            mHasTfBitmap = 0;
            mHasTfList = 1;
        }
    }
    else 
    {
        mHasTf = 0;
        mHasTfList = 0;
        mHasTfBitmap = 0;
    }
    mUnused = 0;
}

bool DocListFormatOption::operator == (const DocListFormatOption& right) const
{
    return mHasTf == right.mHasTf
        && mHasTfList == right.mHasTfList
        && mHasTfBitmap == right.mHasTfBitmap
        && mHasDocPayload == right.mHasDocPayload
        && mHasFieldMap == right.mHasFieldMap;
}

void JsonizableDocListFormatOption::Jsonize(
        autil::legacy::Jsonizable::JsonWrapper& json)
{
    bool hasTf;
    bool hasTfList;
    bool hasTfBitmap;
    bool hasDocPayload;
    bool hasFieldMap;

    if (json.GetMode() == FROM_JSON)
    {
        json.Jsonize("has_term_frequency", hasTf);
        json.Jsonize("has_term_frequency_list", hasTfList);
        json.Jsonize("has_term_frequency_bitmap", hasTfBitmap);
        json.Jsonize("has_doc_payload", hasDocPayload);
        json.Jsonize("has_field_map", hasFieldMap);

        mDocListFormatOption.mHasTf = hasTf ? 1 : 0;
        mDocListFormatOption.mHasTfList = hasTfList ? 1 : 0;
        mDocListFormatOption.mHasTfBitmap = hasTfBitmap ? 1 : 0;
        mDocListFormatOption.mHasDocPayload = hasDocPayload ? 1 : 0;
        mDocListFormatOption.mHasFieldMap = hasFieldMap ? 1 : 0;
    }
    else
    {
        hasTf = mDocListFormatOption.mHasTf == 1;
        hasTfList = mDocListFormatOption.mHasTfList == 1;
        hasTfBitmap = mDocListFormatOption.mHasTfBitmap == 1;
        hasDocPayload = mDocListFormatOption.mHasDocPayload == 1;
        hasFieldMap = mDocListFormatOption.mHasFieldMap == 1;

        json.Jsonize("has_term_frequency", hasTf);
        json.Jsonize("has_term_frequency_list", hasTfList);
        json.Jsonize("has_term_frequency_bitmap", hasTfBitmap);
        json.Jsonize("has_doc_payload", hasDocPayload);
        json.Jsonize("has_field_map", hasFieldMap);
    }
}

IE_NAMESPACE_END(index);

