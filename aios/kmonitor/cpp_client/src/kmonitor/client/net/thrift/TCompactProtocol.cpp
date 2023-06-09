/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-04-19 15:42
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#include "autil/Log.h"
#include "kmonitor/client/net/thrift/ThriftType.h"
#include "kmonitor/client/net/thrift/TCompactProtocol.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);
AUTIL_LOG_SETUP(kmonitor, TCompactProtocol);

TCompactProtocol::TCompactProtocol(TFastFramedTransport *transport) {
    transport_ = transport;
    lastFieldId_ = 0;
    string_limit_ = 0;
    string_buf_ = NULL;
    string_buf_size_ = 0;
    container_limit_ = 0;
    booleanField_.name = NULL;
    boolValue_.hasBoolValue = false;
}

TCompactProtocol::~TCompactProtocol() {
    if (string_buf_ != NULL) {
        free(string_buf_);
        string_buf_ = NULL;
    }
}

uint32_t TCompactProtocol::WriteMessageBegin(const std::string& name,
                           const TMessageType messageType,
                           const int32_t seqid) {
    uint32_t wsize = 0;
    wsize += WriteByte(PROTOCOL_ID);
    wsize += WriteByte((VERSION_N & VERSION_MASK) | (((int32_t)messageType << TYPE_SHIFT_AMOUNT) & TYPE_MASK));
    wsize += WriteVarint32(seqid);
    wsize += WriteString(name);
    return wsize;
}

uint32_t TCompactProtocol::WriteStructBegin(const char* name) {
    (void) name;
    lastField_.push(lastFieldId_);
    lastFieldId_ = 0;
    return 0;
}

uint32_t TCompactProtocol::WriteStructEnd() {
    lastFieldId_ = lastField_.top();
    lastField_.pop();
    return 0;
}

uint32_t TCompactProtocol::WriteFieldBegin(const char* name,
                                           const TType fieldType,
                                           const int16_t fieldId) {
    if (fieldType == T_BOOL) {
        booleanField_.name = name;
        booleanField_.fieldType = fieldType;
        booleanField_.fieldId = fieldId;
    } else {
        return WriteFieldBeginInternal(name, fieldType, fieldId, -1);
    }
    return 0;
}

uint32_t TCompactProtocol::WriteFieldStop() {
    return WriteByte(T_STOP);
}

uint32_t TCompactProtocol::WriteListBegin(const TType elemType, const uint32_t size) {
    return WriteCollectionBegin(elemType, size);
}

uint32_t TCompactProtocol::WriteSetBegin(const TType elemType, const uint32_t size) {
    return WriteCollectionBegin(elemType, size);
}

uint32_t TCompactProtocol::WriteMapBegin(const TType keyType, const TType valType, const uint32_t size) {
    uint32_t wsize = 0;
    if (size == 0) {
        wsize += WriteByte(0);
    } else {
        wsize += WriteVarint32(size);
        wsize += WriteByte(GetCompactType(keyType) << 4 | GetCompactType(valType));
    }
    return wsize;
}

uint32_t TCompactProtocol::WriteBool(const bool value) {

    uint32_t wsize = 0;
    if (booleanField_.name != NULL) {
        // we haven't written the field header yet
        wsize += WriteFieldBeginInternal(booleanField_.name,
                                    booleanField_.fieldType,
                                    booleanField_.fieldId,
                                    static_cast<int8_t>(value
                                                        ? CT_BOOLEAN_TRUE
                                                        : CT_BOOLEAN_FALSE));
        booleanField_.name = NULL;
    } else {
       // we're not part of a field, so just write the value
       wsize += WriteByte(static_cast<int8_t>(value
                                          ? CT_BOOLEAN_TRUE
                                          : CT_BOOLEAN_FALSE));
    }
    return wsize;
}

uint32_t TCompactProtocol::WriteByte(const int8_t byte) {
    if (transport_->Write((uint8_t*)&byte, 1) < 0) {
        return 0;
    }
    return 1;
}

uint32_t TCompactProtocol::WriteI16(const int16_t i16) {
    return WriteVarint32(i32ToZigzag(i16));
}

uint32_t TCompactProtocol::WriteI32(const int32_t i32) {
    return WriteVarint32(i32ToZigzag(i32));
}

uint32_t TCompactProtocol::WriteI64(const int64_t i64) {
    return WriteVarint64(i64ToZigzag(i64));
}

uint32_t TCompactProtocol::WriteDouble(const double dub) {
    uint64_t bits = bitwise_cast<uint64_t>(dub);
    // bits = THRIFT_htolell(bits);
    if (transport_->Write((uint8_t*)&bits, 8) < 0) {
        return 0;
    }
    return sizeof(dub);
}

uint32_t TCompactProtocol::WriteString(const std::string& str) {
    return WriteBinary(str);
}

uint32_t TCompactProtocol::WriteBinary(const std::string& str) {
    uint32_t ssize = static_cast<uint32_t>(str.size());
    uint32_t wsize = WriteVarint32(ssize);
    if (transport_->Write((uint8_t*)str.data(), ssize) < 0) {
        return 0;
    }
    
    return wsize += ssize;
}

int32_t TCompactProtocol::WriteFieldBeginInternal(const char* name,
                                const TType fieldType,
                                const int16_t fieldId,
                                int8_t typeOverride) {

    (void) name;
    uint32_t wsize = 0;

    // if there's a type override, use that.
    int8_t typeToWrite = (typeOverride == -1 ? GetCompactType(fieldType) : typeOverride);
    // check if we can use delta encoding for the field id
    if (fieldId > lastFieldId_ && fieldId - lastFieldId_ <= 15) {
        // write them together
        wsize += WriteByte(static_cast<int8_t>((fieldId - lastFieldId_)
                                               << 4 | typeToWrite));
    } else {
        // write them separate
        wsize += WriteByte(typeToWrite);
        wsize += WriteI16(fieldId);
    }

    lastFieldId_ = fieldId;
    return wsize;
}

uint32_t TCompactProtocol::WriteCollectionBegin(const TType elemType, int32_t size) {
    uint32_t wsize = 0;
    if (size <= 14) {
      wsize += WriteByte(static_cast<int8_t>(size
                                             << 4 | GetCompactType(elemType)));
    } else {
      wsize += WriteByte(0xf0 | GetCompactType(elemType));
      wsize += WriteVarint32(size);
    }
    return wsize;
}

uint32_t TCompactProtocol::WriteVarint32(uint32_t n) {
    uint8_t buf[5];
    uint32_t wsize = 0;

    while (true) {
      if ((n & ~0x7F) == 0) {
          buf[wsize++] = (int8_t)n;
          break;
      } else {
          buf[wsize++] = (int8_t)((n & 0x7F) | 0x80);
          n >>= 7;
      }
    }
    
    if (transport_->Write(buf, wsize) < 0) {
        return 0;
    }
        
    return wsize;
}

uint32_t TCompactProtocol::WriteVarint64(uint64_t n) {
    uint8_t buf[10];
    uint32_t wsize = 0;

    while (true) {
      if ((n & ~0x7FL) == 0) {
        buf[wsize++] = (int8_t)n;
        break;
      } else {
        buf[wsize++] = (int8_t)((n & 0x7F) | 0x80);
        n >>= 7;
      }
    }
    if (transport_->Write(buf, wsize) < 0) {
        return 0;
    }
    return wsize;
}

uint64_t TCompactProtocol::i64ToZigzag(const int64_t l) {
    return (static_cast<uint64_t>(l) << 1) ^ (l >> 63);
}

uint32_t TCompactProtocol::i32ToZigzag(const int32_t n) {
    return (static_cast<uint32_t>(n) << 1) ^ (n >> 31);
}

int8_t TCompactProtocol::GetCompactType(const TType ttype) {
    return TTypeToCType[ttype];
}

uint32_t TCompactProtocol::ReadMessageBegin(std::string& name, TMessageType& messageType, int32_t& seqid) {
    uint32_t rsize = 0;
    int8_t protocolId;
    int8_t versionAndType;
    int8_t version;

    int32_t msg_size = transport_->ReadAll();
    if (msg_size <= 0) {
        AUTIL_LOG(WARN, "read thrift reply size failed: %d", msg_size);
        return 0;
    }
    AUTIL_LOG(DEBUG, "thrift message size: %d", msg_size);

    rsize += ReadByte(protocolId);
    if (protocolId != PROTOCOL_ID) {
        AUTIL_LOG(WARN, "thrift reply protocolid:%d not PROTOCOL_ID[%d]", protocolId, PROTOCOL_ID);
        return 0;
    }

    rsize += ReadByte(versionAndType);
    version = (int8_t)(versionAndType & VERSION_MASK);
    if (version != VERSION_N) {
        AUTIL_LOG(WARN, "thrift reply version:%d not VERSION_N[%d]", version, VERSION_N);
        return 0;
    }

    messageType = (TMessageType)((versionAndType >> TYPE_SHIFT_AMOUNT) & TYPE_BITS);
    rsize += ReadVarint32(seqid);
    rsize += ReadString(name);
    return rsize;
}

uint32_t TCompactProtocol::ReadStructBegin(std::string& name) {
    name = "";
    lastField_.push(lastFieldId_);
    lastFieldId_ = 0;
    return 0;
}

uint32_t TCompactProtocol::ReadStructEnd() {
    lastFieldId_ = lastField_.top();
    lastField_.pop();
    return 0;
}

uint32_t TCompactProtocol::ReadFieldBegin(std::string& name, TType& fieldType, int16_t& fieldId) {
    (void) name;
    uint32_t rsize = 0;
    int8_t byte = 0;
    int8_t type = 0;

    rsize += ReadByte(byte);
    type = (byte & 0x0f);

    // if it's a stop, then we can return immediately, as the struct is over.
    if (type == T_STOP) {
      fieldType = T_STOP;
      fieldId = 0;
      return rsize;
    }

    // mask off the 4 MSB of the type header. it could contain a field id delta.
    int16_t modifier = (int16_t)(((uint8_t)byte & 0xf0) >> 4);
    if (modifier == 0) {
      // not a delta, look ahead for the zigzag varint field id.
      rsize += ReadI16(fieldId);
    } else {
      fieldId = (int16_t)(lastFieldId_ + modifier);
    }

    fieldType = GetTType(type);
    // if this happens to be a boolean field, the value is encoded in the type
    if (type == CT_BOOLEAN_TRUE || type == CT_BOOLEAN_FALSE) {
      // save the boolean value in a special instance variable.
      boolValue_.hasBoolValue = true;
      boolValue_.boolValue =
        (type == CT_BOOLEAN_TRUE ? true : false);
    }

    // push the new field onto the field stack so we can keep the deltas going.
    lastFieldId_ = fieldId;
    return rsize;
}

uint32_t TCompactProtocol::ReadMapBegin(TType& keyType, TType& valType, uint32_t& size) {
    uint32_t rsize = 0;
    int8_t kvType = 0;
    int32_t msize = 0;

    rsize += ReadVarint32(msize);
    if (msize != 0)
      rsize += ReadByte(kvType);

    if (msize < 0) {
      // throw TProtocolException(TProtocolException::NEGATIVE_SIZE);
      return 0;
    } else if (container_limit_ && msize > container_limit_) {
      // throw TProtocolException(TProtocolException::SIZE_LIMIT);
        return 0;
    }

    keyType = GetTType((int8_t)((uint8_t)kvType >> 4));
    valType = GetTType((int8_t)((uint8_t)kvType & 0xf));
    size = (uint32_t)msize;

    return rsize;
}

uint32_t TCompactProtocol::ReadListBegin(TType& elemType, uint32_t& size) {
    int8_t size_and_type = 0;
    uint32_t rsize = 0;
    int32_t lsize = 0;

    rsize += ReadByte(size_and_type);

    lsize = ((uint8_t)size_and_type >> 4) & 0x0f;
    if (lsize == 15) {
      rsize += ReadVarint32(lsize);
    }

    if (lsize < 0) {
      // throw TProtocolException(TProtocolException::NEGATIVE_SIZE);
      return 0;
    } else if (container_limit_ && lsize > container_limit_) {
      // throw TProtocolException(TProtocolException::SIZE_LIMIT);
        return 0;
    }

    elemType = GetTType((int8_t)(size_and_type & 0x0f));
    size = (uint32_t)lsize;

    return rsize;
}

uint32_t TCompactProtocol::ReadSetBegin(TType& elemType, uint32_t& size) {
    return ReadListBegin(elemType, size);
}

uint32_t TCompactProtocol::ReadBool(bool& value) {
    if (boolValue_.hasBoolValue == true) {
      value = boolValue_.boolValue;
      boolValue_.hasBoolValue = false;
      return 0;
    } else {
      int8_t val = 0;
      ReadByte(val);
      value = (val == CT_BOOLEAN_TRUE);
      return 1;
    }
}

uint32_t TCompactProtocol::ReadDouble(double& dub) {
    union {
      uint64_t bits;
      uint8_t b[8];
    } u; // TODO default value
    transport_->Read(u.b, 8);
    // u.bits = THRIFT_letohll(u.bits);
    dub = bitwise_cast<double>(u.bits); // TODO
    return sizeof(dub);
}

uint32_t TCompactProtocol::ReadByte(int8_t& byte) {
    // assign an initial value
    uint8_t b[1] = {0};
    auto ret = transport_->Read(b, 1);
    byte = *(int8_t*)b;
    return ret;
}

uint32_t TCompactProtocol::ReadI16(int16_t& i16) {
    int32_t value = 0;
    uint32_t rsize = ReadVarint32(value);
    i16 = (int16_t)ZigzagToI32(value);
    return rsize;
}

uint32_t TCompactProtocol::ReadI32(int32_t& i32) {
    int32_t value = 0;
    uint32_t rsize = ReadVarint32(value);
    i32 = ZigzagToI32(value);
    return rsize;
}

uint32_t TCompactProtocol::ReadI64(int64_t& i64) {
    int64_t value = 0;
    uint32_t rsize = ReadVarint64(value);
    i64 = ZigzagToI64(value);
    return rsize;
}

uint32_t TCompactProtocol::ReadString(std::string& str) {
    return ReadBinary(str);
}

uint32_t TCompactProtocol::ReadBinary(std::string& str) {
    int32_t rsize = 0;
    int32_t size = 0;

    rsize += ReadVarint32(size);
    // Catch empty string case
    if (size == 0) {
      str = "";
      return rsize;
    }

    // Catch error cases
    if (size < 0) {
      return 0;
    }

    // Use the heap here to prevent stack overflow for v. large strings
    if (size > string_buf_size_ || string_buf_ == NULL) {
      void* new_string_buf = std::realloc(string_buf_, (uint32_t)size);
      string_buf_ = (uint8_t*)new_string_buf;
      string_buf_size_ = size;
    }
    auto ret = transport_->Read(string_buf_, size);
    if (ret > 0) {
        str.assign((char*)string_buf_, size);
    }
    return rsize + (uint32_t)size;
}

uint32_t TCompactProtocol::ReadVarint32(int32_t& i32) {
    int64_t val = 0;
    uint32_t rsize = ReadVarint64(val);
    i32 = (int32_t)val;
    return rsize;
}

uint32_t TCompactProtocol::ReadVarint64(int64_t& i64) {
    uint32_t rsize = 0;
    uint64_t val = 0;
    int shift = 0;

    while (true) {
        uint8_t byte = 0;
        rsize += transport_->Read(&byte, 1);
        val |= (uint64_t)(byte & 0x7f) << shift;
        shift += 7;
        if (!(byte & 0x80)) {
          i64 = val;
          return rsize;
        }
    }
}

int32_t TCompactProtocol::ZigzagToI32(uint32_t n) const {
    return (n >> 1) ^ static_cast<uint32_t>(-static_cast<int32_t>(n & 1));
}

int64_t TCompactProtocol::ZigzagToI64(uint64_t n) const {
    return (n >> 1) ^ static_cast<uint64_t>(-static_cast<int64_t>(n & 1));
}

TType TCompactProtocol::GetTType(int8_t type) const {
    switch (type) {
      case T_STOP:
        return T_STOP;
      case CT_BOOLEAN_FALSE:
      case CT_BOOLEAN_TRUE:
        return T_BOOL;
      case CT_BYTE:
        return T_BYTE;
      case CT_I16:
        return T_I16;
      case CT_I32:
        return T_I32;
      case CT_I64:
        return T_I64;
      case CT_DOUBLE:
        return T_DOUBLE;
      case CT_BINARY:
        return T_STRING;
      case CT_LIST:
        return T_LIST;
      case CT_SET:
        return T_SET;
      case CT_MAP:
        return T_MAP;
      case CT_STRUCT:
        return T_STRUCT;
      default:
        return T_UNKNOWN;
    }
}

uint32_t TCompactProtocol::Skip(TType type) {
    switch (type) {
      case T_BOOL: {
        bool boolv = false;
        return ReadBool(boolv);
      }
      case T_BYTE: {
        int8_t bytev = 0;
        return ReadByte(bytev);
      }
      case T_I16: {
        int16_t i16 = 0;
        return ReadI16(i16);
      }
      case T_I32: {
        int32_t i32 = 0;
        return ReadI32(i32);
      }
      case T_I64: {
        int64_t i64 = 0;
        return ReadI64(i64);
      }
      case T_DOUBLE: {
        double dub = 0.0f;
        return ReadDouble(dub);
      }
      case T_STRING: {
        std::string str;
        return ReadBinary(str);
      }
      case T_STRUCT: {
        std::string name;
        int16_t fid = 0;
        TType ftype = T_STOP;
        uint32_t result = ReadStructBegin(name);
        while (true) {
          result += ReadFieldBegin(name, ftype, fid);
          if (ftype == T_STOP || ftype == T_UNKNOWN) {
            break;
          }
          result += Skip(ftype);
          result += ReadFieldEnd();
        }
        result += ReadStructEnd();
        return result;
      }
      case T_MAP: {
        TType keyType = T_STOP;
        TType valType = T_STOP;
        uint32_t size = 0;
        uint32_t result = ReadMapBegin(keyType, valType, size);
        for (uint32_t i = 0; i < size; i++) {
          result += Skip(keyType);
          result += Skip(valType);
        }
        result += ReadMapEnd();
        return result;
      }
      case T_SET: {
        TType elemType = T_STOP;
        uint32_t size = 0;
        uint32_t result = ReadSetBegin(elemType, size);
        for (uint32_t i = 0; i < size; i++) {
          result += Skip(elemType);
        }
        result += ReadSetEnd();
        return result;
      }
      case T_LIST: {
        TType elemType = T_STOP;
        uint32_t size = 0;
        uint32_t result = ReadListBegin(elemType, size);
        for (uint32_t i = 0; i < size; i++) {
          result += Skip(elemType);
        }
        result += ReadListEnd();
        return result;
      }
      case T_STOP:
      case T_VOID:
      case T_U64:
      case T_UTF8:
      case T_UTF16:
      case T_UNKNOWN:
        break;
      }
      return 0;
 }

END_KMONITOR_NAMESPACE(kmonitor);

