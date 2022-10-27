#ifndef ISEARCH_LAYERDESCRIPTION_H
#define ISEARCH_LAYERDESCRIPTION_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/search/LayerMetas.h>
#include <autil/StringUtil.h>

BEGIN_HA3_NAMESPACE(common);
class LayerAttrRange;

class LayerKeyRange {
public:
    void serialize(autil::DataBuffer &dataBuffer) const {
        dataBuffer.write(ranges);
        dataBuffer.write(values);
        dataBuffer.write(attrName);
    }
    void deserialize(autil::DataBuffer &dataBuffer) {
        dataBuffer.read(ranges);
        dataBuffer.read(values);
        dataBuffer.read(attrName);
    }
    std::string toString() const;
    bool isRangeKeyWord() const {
        if (attrName == LAYERKEY_DOCID 
            || attrName == LAYERKEY_OTHER 
            || attrName == LAYERKEY_SEGMENTID
            || attrName == LAYERKEY_SORTED
            || attrName == LAYERKEY_UNSORTED
            || attrName == LAYERKEY_PERCENT) 
        {
            return true;
        }
        return false;
    }
public:
    std::vector<LayerAttrRange> ranges;
    std::vector<std::string> values;
    std::string attrName;
};

class LayerDescription
{
public:
    static const std::string INFINITE;
public:
    LayerDescription();
    ~LayerDescription();
private:
    LayerDescription(const LayerDescription &);
    LayerDescription& operator=(const LayerDescription &);
public:
    void setLayerRanges(const std::vector<LayerKeyRange*> &ranges) {
        resetLayerRange();
        _ranges = ranges;
    }
    void addLayerRange(LayerKeyRange *range) {
        _ranges.push_back(range);
    }
    void insertKeyRange(size_t pos, LayerKeyRange *keyRange) {
        _ranges.insert(_ranges.begin() + pos, keyRange);
    }
    void setQuota(uint32_t quota) {
        _quota = quota;
    }
    uint32_t getQuota() const {
        return _quota;
    }
    void setQuotaMode(QuotaMode mode) {
        _quotaMode = mode;
    }
    QuotaMode getQuotaMode() const {
        return _quotaMode;
    }
    void setQuotaType(QuotaType type) {
        _quotaType = type;
    }
    QuotaType getQuotaType() const {
        return _quotaType;
    }
    LayerKeyRange* getLayerRange(size_t pos) const {
        if (pos >= _ranges.size()) {
            return NULL;
        }
        return _ranges[pos];
    }
    size_t getLayerRangeCount() const {
        return _ranges.size();
    }

    void resizeLayerRange(size_t n) {
        for (size_t i = n; i < _ranges.size(); ++i) {
            DELETE_AND_SET_NULL(_ranges[i]);
        }
        _ranges.resize(n);
    }
public:
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
    std::string toString() const;
private:
    void resetLayerRange();
private:
    std::vector<LayerKeyRange*> _ranges;
    uint32_t _quota;
    QuotaMode _quotaMode;
    QuotaType _quotaType;
private:
    HA3_LOG_DECLARE();
};

class LayerAttrRange {
public:
    LayerAttrRange(){}
    LayerAttrRange(std::string from, std::string to) {
        this->from = from;
        this->to = to;
    }
public:
    void serialize(autil::DataBuffer &dataBuffer) const {
        dataBuffer.write(from);
        dataBuffer.write(to);
    }
    void deserialize(autil::DataBuffer &dataBuffer) {
        dataBuffer.read(from);
        dataBuffer.read(to);
    }
public:
    template <class T>
    static bool convert(const LayerAttrRange& range, T& from, T& to) {
        if (range.from == LayerDescription::INFINITE) {
            from = std::numeric_limits<T>::min();
        } else if (!autil::StringUtil::fromString(range.from, from)) {
            return false;
        }
        if (range.to == LayerDescription::INFINITE) {
            to = std::numeric_limits<T>::max();
        } else if (!autil::StringUtil::fromString(range.to, to)) {
            return false;
        }
        return true;
    }
public:
    std::string from;
    std::string to;
};

HA3_TYPEDEF_PTR(LayerDescription);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_LAYERDESCRIPTION_H
