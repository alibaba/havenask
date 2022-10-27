#ifndef __INDEXLIB_SPECIAL_VALUE_BUCKET_H
#define __INDEXLIB_SPECIAL_VALUE_BUCKET_H

IE_NAMESPACE_BEGIN(common);

// bucket
#pragma pack(push)
#pragma pack(4)
template <typename _KT, typename _VT>
class SpecialValueBucket
{
public:
    SpecialValueBucket() : mKey(), mValue() {}
    ~SpecialValueBucket() {}

public:
    const _KT& Key() const { return mKey; }
    const _VT& Value() const { return mValue; }

    bool IsEmpty() const { return mValue.IsEmpty(); }
    bool IsEqual(const _KT& key) const { return mKey == key; }
    bool IsDeleted() const { return mValue.IsDeleted(); }

    void UpdateValue(const _VT& value)
    {
        mValue.SetValue(value);
    }
    
    void Set(const _KT& key, const _VT& value)
    {
        mKey = key;
        mValue.SetValue(value);
    }
    void Set(const SpecialValueBucket<_KT, _VT>& other)
    {
        mKey = other.Key();
        mValue.SetValue(other.Value());
    }
    void SetDelete(const _KT& key, const _VT& value)
    {
        mKey = key;
        mValue.SetDelete(value);
    }
    void SetEmpty()
    {
        mValue.SetEmpty();
    }

private:
    _KT mKey;
    _VT mValue;
};
#pragma pack(pop)

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SPECIAL_VALUE_BUCKET_H
