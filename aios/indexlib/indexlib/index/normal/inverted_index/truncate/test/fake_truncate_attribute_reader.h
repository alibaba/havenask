#ifndef __INDEXLIB_FAKE_TRUNCATE_ATTRIBUTE_READER_H
#define __INDEXLIB_FAKE_TRUNCATE_ATTRIBUTE_READER_H

#include <tr1/memory>
#include <autil/StringUtil.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_attribute_reader.h"

IE_NAMESPACE_BEGIN(index);

template<typename T>
class FakeTruncateAttributeReader : public TruncateAttributeReader
{
public:
    typedef std::vector<T> AttrValueVec;

public:
    FakeTruncateAttributeReader(){};
    ~FakeTruncateAttributeReader(){};

public:
    bool Read(docid_t docId, uint8_t* buf, uint32_t bufLen);

    // attrvalue seprate by ";"
    void SetAttrValue(std::string attrValues);
    void SetAttrValue(const AttrValueVec& attrValues);
    void clear();

private:
    AttrValueVec mValueVec;

private:
    IE_LOG_DECLARE();
};

template<typename T>
inline bool FakeTruncateAttributeReader<T>::Read(docid_t docId, uint8_t* buf, uint32_t bufLen)
{
    assert((size_t)docId < mValueVec.size());
    *(T*)buf = mValueVec[(size_t)docId];
    return true;
}

template<typename T>
inline void FakeTruncateAttributeReader<T>::SetAttrValue(std::string attrValues)
{
    autil::StringUtil::fromString(attrValues, mValueVec, ";");
}

template<typename T>
inline void FakeTruncateAttributeReader<T>::SetAttrValue(const AttrValueVec& attrValues)
{
    for (size_t i = 0; i < attrValues.size(); ++i)
    {
        mValueVec.push_back(attrValues[i]);
    }
}

template<typename T>
inline void FakeTruncateAttributeReader<T>::clear()
{
    mValueVec.clear();
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_FAKE_TRUNCATE_ATTRIBUTE_READER_H
