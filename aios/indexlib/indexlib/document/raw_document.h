#ifndef __INDEXLIB_RAW_DOCUMENT_H
#define __INDEXLIB_RAW_DOCUMENT_H

#include <tr1/memory>
#include <autil/ConstString.h>
#include <autil/HashAlgorithm.h>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/index_locator.h"
#include "indexlib/document/document_init_param.h"
#include "indexlib/document/document.h"

IE_NAMESPACE_BEGIN(document);

class RawDocument : public Document
{
public:
    RawDocument();
    RawDocument(const RawDocument &other);
    virtual ~RawDocument();
    
public:
    virtual bool Init(const DocumentInitParamPtr& initParam) = 0;

    virtual void setField(const autil::ConstString &fieldName,
                          const autil::ConstString &fieldValue) = 0;
    
    virtual void setFieldNoCopy(const autil::ConstString &fieldName,
                                const autil::ConstString &fieldValue) = 0;

    virtual const autil::ConstString &getField(
            const autil::ConstString &fieldName) const = 0;

    virtual bool exist(const autil::ConstString &fieldName) const  = 0;

    virtual void eraseField(const autil::ConstString &fieldName) = 0;

    virtual uint32_t getFieldCount() const = 0;

    virtual void clear() = 0;

    virtual RawDocument *clone() const = 0;
    virtual RawDocument *createNewDocument() const = 0;
    virtual DocOperateType getDocOperateType() { return mOpType; }
    virtual void setDocOperateType(DocOperateType type) { mOpType = type; }

    virtual std::string getIdentifier() const = 0;

public:
    void DoSerialize(autil::DataBuffer &dataBuffer,
                     uint32_t serializedVersion) const override;
    void DoDeserialize(autil::DataBuffer &dataBuffer,
                       uint32_t serializedVersion) override;
    void SetDocId(docid_t docId) override;
public:
    void setField(const char *fieldName, size_t nameLen,
                  const char *fieldValue, size_t valueLen);

    void setField(const std::string &fieldName,
                  const std::string &fieldValue);

    std::string getField(const std::string &fieldName) const;

    bool exist(const std::string &fieldName) const;

    void eraseField(const std::string &fieldName);
    
    bool NeedTrace() const;
    std::string GetTraceId() const;

public:
    int64_t getDocTimestamp() const { return mTimestamp; }
    void setDocTimestamp(int64_t timestamp) { mTimestamp = timestamp; }

    const std::string &getDocSource() const { return Document::GetSource(); }
    void setDocSource(const std::string &source) { Document::SetSource(source); }

    void setLocator(const common::IndexLocator &locator);
    common::IndexLocator getLocator() const;
    
    autil::mem_pool::Pool *getPool() { return mPool; }

public:
    // for log out
    virtual std::string toString() const = 0;
    
protected:
    autil::mem_pool::Pool *mPool;
    
protected:
    static const autil::ConstString EMPTY_STRING;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RawDocument);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_RAW_DOCUMENT_H
