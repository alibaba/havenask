#ifndef __INDEXLIB_DEFAULT_RAW_DOCUMENT_H
#define __INDEXLIB_DEFAULT_RAW_DOCUMENT_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/document/raw_document/key_map_manager.h"
#include "indexlib/document/raw_document.h"

IE_NAMESPACE_BEGIN(document);

class DefaultRawDocument : public RawDocument
{
public:
    typedef std::vector<autil::ConstString> FieldVec;
    typedef FieldVec::const_iterator Iterator;

    static std::string RAW_DOC_IDENTIFIER;
public:
    // Default Constructor is not recommended and inefficient.
    // Because it will create a hash map manager instead of sharing one.
    DefaultRawDocument()
        : RawDocument()
        , mHashMapManager(new KeyMapManager())
        , mHashMapIncrement(new KeyMap())
        , mFieldCount(0)
    {
        mHashMapPrimary = mHashMapManager->getHashMapPrimary();
    }

    DefaultRawDocument(const KeyMapManagerPtr &hashMapManager)
        : RawDocument()
        , mHashMapManager(hashMapManager)
        , mHashMapIncrement(new KeyMap())
        , mFieldCount(0)
    {
        if(mHashMapManager==NULL){
            mHashMapManager.reset(new KeyMapManager());
        }
        mHashMapPrimary = mHashMapManager->getHashMapPrimary();
        if (mHashMapPrimary) {
            mFieldsPrimary.resize(mHashMapPrimary->size());
        }
    }
    
    DefaultRawDocument(const DefaultRawDocument &other);
    
    virtual ~DefaultRawDocument() {
        mHashMapManager->updatePrimary(mHashMapIncrement);
    }
    
public:
    bool Init(const DocumentInitParamPtr& initParam) override { return true; }
    
    void setFieldNoCopy(const autil::ConstString &fieldName,
                        const autil::ConstString &fieldValue) override;

    void setField(const autil::ConstString &fieldName,
                  const autil::ConstString &fieldValue) override;

    const autil::ConstString &getField(
            const autil::ConstString &fieldName) const override;

    bool exist(const autil::ConstString &fieldName) const override;
    
    void eraseField(const autil::ConstString &fieldName) override;
    
    uint32_t getFieldCount() const override;
    
    void clear() override;
    
    DefaultRawDocument *clone() const override;

    DefaultRawDocument *createNewDocument() const override;
    
    DocOperateType getDocOperateType() override;

    std::string getIdentifier() const override { return RAW_DOC_IDENTIFIER; }

    uint32_t GetDocBinaryVersion() const override { return DOCUMENT_BINARY_VERSION; }

    size_t EstimateMemory() const override;

public:
    using RawDocument::setField;
    using RawDocument::getField;
    using RawDocument::exist;
    using RawDocument::eraseField;
    
    KeyMapManagerPtr getHashMapManager() const { return mHashMapManager; }

private:
    void addNewField(const autil::ConstString &fieldName, const autil::ConstString &fieldValue);
    const autil::ConstString *search(const autil::ConstString &fieldName) const;
    autil::ConstString *search(const autil::ConstString &fieldName);
    
public:
    // for log out
    std::string toString() const override;
    
    // public for test
    static DocOperateType getDocOperateType(const autil::ConstString &cmdString);
    
private:
    // to manager the version of primary hash map.
    KeyMapManagerPtr mHashMapManager; 
    // hash map: keep the map from key to id.
    // primary: A sharing and Read-Only hash map among documents.
    KeyMapPtr mHashMapPrimary;
    // increment: A private and incremental hash map.
    KeyMapPtr mHashMapIncrement;
    // fields: keep the map from id to value.
    FieldVec mFieldsPrimary;
    FieldVec mFieldsIncrement;
    uint32_t mFieldCount;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DefaultRawDocument);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_DEFAULT_RAW_DOCUMENT_H
