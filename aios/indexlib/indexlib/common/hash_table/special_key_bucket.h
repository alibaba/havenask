#ifndef __INDEXLIB_SPECIAL_KEY_BUCKET_H
#define __INDEXLIB_SPECIAL_KEY_BUCKET_H

IE_NAMESPACE_BEGIN(common);

#pragma pack(push)
#pragma pack(4)

template <typename _KT, typename _VT,
          _KT _EmptyKey = (_KT)0xFFFFFFFFFFFFFEFFUL,
          _KT _DeleteKey = (_KT)0xFFFFFFFFFFFFFDFFUL>
class SpecialKeyBucket
{
    static_assert(std::is_same<_KT, uint64_t>::value || std::is_same<_KT, uint32_t>::value,
                  "key type only support uint64_t or uint32_t,"
                  "due to the default _EmptyKey and _DeleteKey");
    // ATTENTION: can not guarantee concurrent read and write
public:
    SpecialKeyBucket(): mKey(_EmptyKey), mValue() {}
    ~SpecialKeyBucket() {}

public:
    typedef SpecialKeyBucket<_KT, _VT, 0, 1> SpecialBucket;
    static bool IsEmptyKey(const _KT& key) { return key == _EmptyKey; }
    static bool IsDeleteKey(const _KT& key) { return key == _DeleteKey; }
    static _KT EmptyKey() { return _EmptyKey; }
    static _KT DeleteKey() { return _DeleteKey; }

public:
    const _KT& Key() const { return IsDeleted() ? mKeyInValue : mKey; }
    const _VT& Value() const { return mValue; }

    bool IsEmpty() const
    { return mKey == _EmptyKey; }
    bool IsEqual(const _KT& key) const
    {
        assert(key != _DeleteKey);
        assert(key != _EmptyKey);
        if (mKey == _DeleteKey)
        {
            return mKeyInValue == key;
        }
        return mKey == key;
    }
    bool IsDeleted() const
    { return (mKey == _DeleteKey); }

    void UpdateValue(const _VT& value)
    {
        mValue = value;
    }
    void Set(const _KT& key, const _VT& value)
    {
        assert(key != _DeleteKey);
        assert(key != _EmptyKey);
        mValue = value;
        mKey = key;
    }
    void Set(const SpecialKeyBucket<_KT, _VT, _EmptyKey, _DeleteKey>& other)
    {
        *this = other; // thread unsafe
    }
    void SetDelete(const _KT& key, const _VT& value)
    {
        assert(mKey == key || mKey == _EmptyKey || mKey == _DeleteKey);
        mKeyInValue = key;
        mKey = _DeleteKey; // thread unsafe
    }
    void SetEmpty()
    {
        mKey = _EmptyKey;
        new (&mValue) _VT;
    }

private:
    _KT mKey;
    union
    {
        _VT mValue;
        _KT mKeyInValue;
    };
};
#pragma pack(pop)

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SPECIAL_KEY_BUCKET_H
