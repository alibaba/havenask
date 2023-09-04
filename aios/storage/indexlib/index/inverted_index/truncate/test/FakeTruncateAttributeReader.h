#include <memory>

#include "autil/StringUtil.h"
#include "indexlib/index/inverted_index/truncate/TruncateAttributeReader.h"

namespace indexlib::index {

template <typename T>
class FakeTruncateAttributeReader : public TruncateAttributeReader
{
public:
    FakeTruncateAttributeReader(const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper,
                                const std::shared_ptr<indexlibv2::index::AttributeConfig>& attrConfig)
        : TruncateAttributeReader(docMapper, attrConfig) {};
    ~FakeTruncateAttributeReader() {};

public:
    bool Read(docid_t docId, const std::shared_ptr<AttributeDiskIndexer::ReadContextBase>&, uint8_t* buf,
              uint32_t bufLen, uint32_t& dataLen, bool& isNull) override;
    void SetAttrValue(const std::string& attrValues, const std::string& isNullVec = "");
    void SetAttrValue(const std::vector<T>& attrValues, const std::vector<bool>& isNullVec);
    void clear();

private:
    std::vector<T> _valueVec;
    std::vector<bool> _nullVec;
};

template <typename T>
inline bool FakeTruncateAttributeReader<T>::Read(docid_t docId,
                                                 const std::shared_ptr<AttributeDiskIndexer::ReadContextBase>&,
                                                 uint8_t* buf, uint32_t bufLen, uint32_t& dataLen, bool& isNull)
{
    assert((size_t)docId < _valueVec.size());
    *(T*)buf = _valueVec[(size_t)docId];
    isNull = _nullVec[(size_t)docId];
    dataLen = 0;
    if (!isNull) {
        dataLen = sizeof(T);
    }
    return true;
}

template <typename T>
inline void FakeTruncateAttributeReader<T>::SetAttrValue(const std::string& attrValues, const std::string& isNullVec)
{
    autil::StringUtil::fromString(attrValues, _valueVec, ";");
    if (isNullVec.empty()) {
        for (size_t i = 0; i < _valueVec.size(); ++i) {
            _nullVec.push_back(false);
        }
        return;
    }
    autil::StringUtil::fromString(isNullVec, _nullVec, ";");
}

template <typename T>
inline void FakeTruncateAttributeReader<T>::SetAttrValue(const std::vector<T>& attrValues,
                                                         const std::vector<bool>& isNullVec)
{
    for (size_t i = 0; i < attrValues.size(); ++i) {
        _valueVec.push_back(attrValues[i]);
    }
    for (size_t i = 0; i < isNullVec.size(); ++i) {
        _nullVec.push_back(isNullVec[i]);
    }
}

template <typename T>
inline void FakeTruncateAttributeReader<T>::clear()
{
    _valueVec.clear();
    _nullVec.clear();
}

} // namespace indexlib::index
