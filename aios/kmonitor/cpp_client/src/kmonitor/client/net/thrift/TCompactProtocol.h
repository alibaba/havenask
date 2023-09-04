/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-04-19 15:42
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#ifndef KMONITOR_CLIENT_NET_THRIFT_TCOMPACTPROTOCOL_H_
#define KMONITOR_CLIENT_NET_THRIFT_TCOMPACTPROTOCOL_H_

#include <stack>
#include <string>

#include "autil/Log.h"
#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/net/thrift/TFastFramedTransport.h"
#include "kmonitor/client/net/thrift/ThriftType.h"
BEGIN_KMONITOR_NAMESPACE(kmonitor);

class TCompactProtocol {
public:
    TCompactProtocol(TFastFramedTransport *transport);
    ~TCompactProtocol();

public:
    uint32_t WriteMessageBegin(const std::string &name, const TMessageType messageType, const int32_t seqid);

    uint32_t WriteStructBegin(const char *name);
    uint32_t WriteStructEnd();
    uint32_t WriteFieldBegin(const char *name, const TType fieldType, const int16_t fieldId);
    uint32_t WriteFieldStop();
    uint32_t WriteListBegin(const TType elemType, const uint32_t size);
    uint32_t WriteSetBegin(const TType elemType, const uint32_t size);
    uint32_t WriteMapBegin(const TType keyType, const TType valType, const uint32_t size);
    uint32_t WriteBool(const bool value);
    uint32_t WriteByte(const int8_t byte);
    uint32_t WriteI16(const int16_t i16);
    uint32_t WriteI32(const int32_t i32);
    uint32_t WriteI64(const int64_t i64);
    uint32_t WriteDouble(const double dub);
    uint32_t WriteString(const std::string &str);
    uint32_t WriteBinary(const std::string &str);
    /**
     * These methods are called by structs, but don't actually have any wired
     * output or purpose
     */
    uint32_t WriteMessageEnd() { return 0; }
    uint32_t WriteMapEnd() { return 0; }
    uint32_t WriteListEnd() { return 0; }
    uint32_t WriteSetEnd() { return 0; }
    uint32_t WriteFieldEnd() { return 0; }

    uint32_t ReadMessageBegin(std::string &name, TMessageType &messageType, int32_t &seqid);
    uint32_t ReadStructBegin(std::string &name);
    uint32_t ReadStructEnd();
    uint32_t ReadFieldBegin(std::string &name, TType &fieldType, int16_t &fieldId);

    uint32_t ReadBool(bool &value);
    uint32_t ReadByte(int8_t &byte);
    uint32_t ReadI16(int16_t &i16);
    uint32_t ReadI32(int32_t &i32);
    uint32_t ReadI64(int64_t &i64);
    uint32_t ReadDouble(double &dub);
    uint32_t ReadString(std::string &str);
    uint32_t ReadBinary(std::string &str);

    uint32_t ReadMapBegin(TType &keyType, TType &valType, uint32_t &size);
    uint32_t ReadListBegin(TType &elemType, uint32_t &size);
    uint32_t ReadSetBegin(TType &elemType, uint32_t &size);

    uint32_t ReadMapEnd() { return 0; }
    uint32_t ReadListEnd() { return 0; }
    uint32_t ReadSetEnd() { return 0; }
    /*
     *These methods are here for the struct to call, but don't have any wire
     * encoding.
     */
    uint32_t ReadMessageEnd() { return 0; }
    uint32_t ReadFieldEnd() { return 0; }
    uint32_t Skip(TType type);

    TFastFramedTransport *GetTransport() { return transport_; }

protected:
    int32_t
    WriteFieldBeginInternal(const char *name, const TType fieldType, const int16_t fieldId, int8_t typeOverride);
    uint32_t WriteCollectionBegin(const TType elemType, int32_t size);
    uint32_t WriteVarint32(uint32_t n);
    uint32_t WriteVarint64(uint64_t n);
    uint64_t i64ToZigzag(const int64_t l);
    uint32_t i32ToZigzag(const int32_t n);
    int8_t GetCompactType(const TType ttype);
    TType GetTType(int8_t type) const;

    uint32_t ReadVarint32(int32_t &i32);
    uint32_t ReadVarint64(int64_t &i64);
    int32_t ZigzagToI32(uint32_t n) const;
    int64_t ZigzagToI64(uint64_t n) const;

private:
    TCompactProtocol(const TCompactProtocol &);
    TCompactProtocol &operator=(const TCompactProtocol &);
    /**
     * (Writing) If we encounter a boolean field begin, save the TField here
     * so it can have the value incorporated.
     */
    struct {
        const char *name;
        TType fieldType;
        int16_t fieldId;
    } booleanField_;

    /**
     * (Reading) If we read a field header, and it's a boolean field, save
     * the boolean value here so that readBool can use it.
     */
    struct {
        bool hasBoolValue;
        bool boolValue;
    } boolValue_;

    /**
     * Used to keep track of the last field for the current and previous structs,
     * so we can do the delta stuff.
     */

    std::stack<int16_t> lastField_;
    int16_t lastFieldId_;

public:
    static const int8_t PROTOCOL_ID = (int8_t)0x82u;
    static const int8_t VERSION_N = 1;
    static const int8_t VERSION_MASK = 0x1f; // 0001 1111

protected:
    static const int8_t TYPE_MASK = (int8_t)0xE0u; // 1110 0000
    static const int8_t TYPE_BITS = 0x07;          // 0000 0111
    static const int32_t TYPE_SHIFT_AMOUNT = 5;

    TFastFramedTransport *transport_;

    // Buffer for reading strings, save for the lifetime of the protocol to
    // avoid memory churn allocating memory on every string read
    int32_t string_limit_;
    uint8_t *string_buf_;
    int32_t string_buf_size_;
    int32_t container_limit_;

private:
    AUTIL_LOG_DECLARE();
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif // KMONITOR_CLIENT_NET_THRIFT_TCOMPACTPROTOCOL_H_
