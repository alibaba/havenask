#ifndef __INDEXLIB_ANALYZER_TOKEN_H
#define __INDEXLIB_ANALYZER_TOKEN_H

#include <autil/DataBuffer.h>
#include <autil/HashAlgorithm.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(testlib, DocumentCreator);
DECLARE_REFERENCE_CLASS(test, PartitionStateMachine);

IE_NAMESPACE_BEGIN(document);

class AnalyzerToken
{
public:
    struct TokenProperty {
        uint8_t _isStopWord : 1;
        uint8_t _isSpace : 1;
        uint8_t _isBasicRetrieve : 1;
        uint8_t _isDelimiter : 1;
        uint8_t _isRetrieve : 1;
        uint8_t _needSetText : 1;
        uint8_t _unused : 2;

        TokenProperty() {
            *(uint8_t *)this = 0;
            _isBasicRetrieve = 1;
            _isRetrieve = 1;
            _needSetText = 1;
        }

        inline bool isStopWord() const { return _isStopWord == 1; }
        inline bool isSpace() const { return _isSpace == 1; }
        inline bool isBasicRetrieve() const { return _isBasicRetrieve == 1; }
        inline bool isDelimiter() const { return _isDelimiter == 1; }
        inline bool isRetrieve() const { return _isRetrieve == 1; }
        inline bool needSetText() const { return _needSetText == 1; }

        inline void setStopWord(bool flag) { _isStopWord = flag ? 1 : 0;}
        inline void setSpace(bool flag) { _isSpace = flag ? 1 : 0;}
        inline void setBasicRetrieve(bool flag) { _isBasicRetrieve = flag ? 1 : 0;}
        inline void setDelimiter(bool flag) { _isDelimiter = flag ? 1 : 0;}
        inline void setRetrieve(bool flag) { _isRetrieve = flag ? 1 : 0;}
        inline void setNeedSetText(bool flag) { _needSetText = flag ? 1 : 0;}

        inline uint8_t& getValue() {
            return *(uint8_t *)this;
        }
        inline uint8_t getValue() const {
            return *(uint8_t *)this;
        }

        void serialize(autil::DataBuffer &dataBuffer) const {
            dataBuffer.write(getValue());
        }
        void deserialize(autil::DataBuffer &dataBuffer) {
            dataBuffer.read(getValue());
        }

        bool operator==(const TokenProperty &other) const {
            return getValue() == other.getValue();
        }
    };
public:
    AnalyzerToken();
    AnalyzerToken(const std::string &text, uint32_t pos);
    AnalyzerToken(const std::string &text, uint32_t pos,
                  const std::string &normalizedText, bool isStopWord = false);
    virtual ~AnalyzerToken();

public:
    const std::string& getNormalizedText() const {return _normalizedText;}
    const std::string& getText() const {return _text;}

    std::string& getNormalizedText() {return _normalizedText;}
    std::string& getText() {return _text;}

    void setNormalizedText(const std::string &normalizedText) {
        _normalizedText = normalizedText;
    }

    void setText(const std::string &text) {
        _text = text;
    }

    static uint64_t getHashKey(const char* value)
    {
        if (NULL != value) {
            return autil::HashAlgorithm::hashString64(value);
        } else {
            return 0;
        }
    }

    uint64_t getHashKey( ) const {
        return getHashKey(_normalizedText.c_str());
    }

    bool isStopWord() const {return _tokenProperty.isStopWord();}
    void setIsStopWord(bool isStopWord) {_tokenProperty.setStopWord(isStopWord);}

    bool isSpace() const {return _tokenProperty.isSpace();}
    void setIsSpace(bool isSpace) {_tokenProperty.setSpace(isSpace);}

    bool isBasicRetrieve() const {return _tokenProperty.isBasicRetrieve();}
    void setIsBasicRetrieve(bool isBasicRetrieve) {_tokenProperty.setBasicRetrieve(isBasicRetrieve);}

    bool isDelimiter() const {return _tokenProperty.isDelimiter();}
    void setIsDelimiter(bool isDelimiter) {_tokenProperty.setDelimiter(isDelimiter);}

    bool isRetrieve() const { return _tokenProperty.isRetrieve();}
    void setIsRetrieve(bool isRetrieve) { _tokenProperty.setRetrieve(isRetrieve);}

    bool needSetText() const { return _tokenProperty.needSetText(); }
    void setNeedSetText(bool flag) { _tokenProperty.setNeedSetText(flag);}

    void setPosition(uint32_t pos) {_pos = pos;}
    uint32_t getPosition() const {return _pos;}

    void setPosPayLoad(pospayload_t posPayLoad) { _posPayLoad = posPayLoad; }
    pospayload_t getPosPayLoad() const { return _posPayLoad; }

    friend bool operator == (const AnalyzerToken& left, const AnalyzerToken& right);

    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);

private:
    std::string _text;
    std::string _normalizedText;
    uint32_t _pos;
    TokenProperty _tokenProperty;
    pospayload_t _posPayLoad;
private:
    friend class TokenizeHelper;
    friend class testlib::DocumentCreator;
    friend class test::PartitionStateMachine;
};

///////////////////////////////////////////////////////////////////
DEFINE_SHARED_PTR(AnalyzerToken);

inline bool operator == (const AnalyzerToken& left, const AnalyzerToken& right) {
    return left._text == right._text
        && left. _normalizedText == right._normalizedText
        && left._pos == right._pos
        && left._tokenProperty == right._tokenProperty
        && left. _posPayLoad == right._posPayLoad;
}

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_ANALYZER_TOKEN_H
