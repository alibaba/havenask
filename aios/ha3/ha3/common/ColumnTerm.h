#ifndef ISEARCH_COLUMNTERM_H
#define ISEARCH_COLUMNTERM_H

#include <vector>
#include <ha3/common.h>
#include <ha3/util/Log.h>
#include <matchdoc/ValueType.h>
#include <autil/DataBuffer.h>
#include <autil/MultiValueType.h>

BEGIN_HA3_NAMESPACE(common);

class ColumnTerm
{
public:
    ColumnTerm(const std::string &indexName = std::string());
    virtual ~ColumnTerm() = default;
public:
    bool getEnableCache() const {
        return _enableCache;
    }

    void setEnableCache(bool enableCache) {
        _enableCache = enableCache;
    }

    const std::string& getIndexName() const {
        return _indexName;
    }

    std::vector<size_t>& getOffsets() {
        return _offsets;
    }

    const std::vector<size_t>& getOffsets() const {
        return _offsets;
    }

    bool isMultiValueTerm() const {
        return ! _offsets.empty();
    }

    std::string offsetsToString() const;
public:
    static void serialize(const ColumnTerm *p, autil::DataBuffer &dataBuffer);
    static ColumnTerm* deserialize(autil::DataBuffer &dataBuffer);
    static ColumnTerm* createColumnTerm(matchdoc::BuiltinType type);
public:
    virtual bool operator == (const ColumnTerm &term) const = 0;
    virtual ColumnTerm *clone() const = 0;
    virtual std::string toString() const = 0;
    virtual matchdoc::BuiltinType getValueType() const = 0;
protected:
    virtual void doSave(autil::DataBuffer &dataBuffer) const = 0;
    virtual void doLoad(autil::DataBuffer &dataBuffer) = 0;
private:
    void save(autil::DataBuffer &dataBuffer) const;
    void load(autil::DataBuffer &dataBuffer);
protected:
    bool _enableCache{true};
    std::string _indexName;
    std::vector<size_t> _offsets;
protected:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ColumnTerm);

template <class T>
class ColumnTermTyped : public ColumnTerm
{
public:
    using ColumnTerm::ColumnTerm;
public:
    std::vector<T>& getValues();
    const std::vector<T>& getValues() const;
public:
    bool operator == (const ColumnTerm& term) const override;
    ColumnTerm *clone() const override;
    std::string toString() const override;
    matchdoc::BuiltinType getValueType() const override;
protected:
    void doSave(autil::DataBuffer &dataBuffer) const override;
    void doLoad(autil::DataBuffer &dataBuffer) override;
private:
    std::vector<T> _values;
};

extern template class ColumnTermTyped<uint64_t>;
extern template class ColumnTermTyped<int64_t>;
extern template class ColumnTermTyped<uint32_t>;
extern template class ColumnTermTyped<int32_t>;
extern template class ColumnTermTyped<uint16_t>;
extern template class ColumnTermTyped<int16_t>;
extern template class ColumnTermTyped<uint8_t>;
extern template class ColumnTermTyped<int8_t>;
extern template class ColumnTermTyped<bool>;
extern template class ColumnTermTyped<float>;
extern template class ColumnTermTyped<double>;
extern template class ColumnTermTyped<autil::MultiChar>;

END_HA3_NAMESPACE(common);

#endif //ISEARCH_COLUMNTERM_H
