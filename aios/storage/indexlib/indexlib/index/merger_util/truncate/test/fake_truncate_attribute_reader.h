#pragma once

#include <memory>

#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/index/merger_util/truncate/truncate_attribute_reader.h"
#include "indexlib/indexlib.h"

namespace indexlib::index::legacy {

template <typename T>
class FakeTruncateAttributeReader : public TruncateAttributeReader
{
public:
    typedef std::vector<T> AttrValueVec;
    using typename AttributeSegmentReader::ReadContextBasePtr;

public:
    FakeTruncateAttributeReader() {};
    ~FakeTruncateAttributeReader() {};

public:
    bool Read(docid_t docId, const ReadContextBasePtr&, uint8_t* buf, uint32_t bufLen, bool& isNull) override;

    // attrvalue seprate by ";"
    void SetAttrValue(const std::string& attrValues, const std::string& isNullVec = "");
    void SetAttrValue(const AttrValueVec& attrValues, const std::vector<bool>& isNullVec);
    void clear();

private:
    AttrValueVec mValueVec;
    std::vector<bool> mNullVec;

private:
    IE_LOG_DECLARE();
};

template <typename T>
inline bool FakeTruncateAttributeReader<T>::Read(docid_t docId, const ReadContextBasePtr&, uint8_t* buf,
                                                 uint32_t bufLen, bool& isNull)
{
    assert((size_t)docId < mValueVec.size());
    *(T*)buf = mValueVec[(size_t)docId];
    isNull = mNullVec[(size_t)docId];
    return true;
}

template <typename T>
inline void FakeTruncateAttributeReader<T>::SetAttrValue(const std::string& attrValues, const std::string& isNullVec)
{
    autil::StringUtil::fromString(attrValues, mValueVec, ";");
    if (isNullVec.empty()) {
        for (size_t i = 0; i < mValueVec.size(); i++) {
            mNullVec.push_back(false);
        }
        return;
    }
    autil::StringUtil::fromString(isNullVec, mNullVec, ";");
}

template <typename T>
inline void FakeTruncateAttributeReader<T>::SetAttrValue(const AttrValueVec& attrValues,
                                                         const std::vector<bool>& isNullVec)
{
    for (size_t i = 0; i < attrValues.size(); ++i) {
        mValueVec.push_back(attrValues[i]);
    }
    for (size_t i = 0; i < isNullVec.size(); ++i) {
        mNullVec.push_back(isNullVec[i]);
    }
}

template <typename T>
inline void FakeTruncateAttributeReader<T>::clear()
{
    mValueVec.clear();
    mNullVec.clear();
}
} // namespace indexlib::index::legacy
