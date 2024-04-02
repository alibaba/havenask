/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "http_client/PBJson.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#define RETURN_ERR(id, cause)  do{\
    err = cause; \
    return id;   \
    }while(0)

using namespace std;
using namespace google::protobuf;

BEGIN_HIPPO_NAMESPACE(http_client);

HIPPO_LOG_SETUP(http_client, PBJson);

PBJson::PBJson() {
}

PBJson::~PBJson() {
}

int PBJson::json2pb(const std::string& json,
                    google::protobuf::Message* msg,
                    std::string& err) {
    rapidjson::Document d;
    d.Parse<0>(json.c_str());
    if (d.HasParseError()){
        err += d.GetParseError();
        return ERR_INVALID_ARG;
    }
    int ret = jsonobject2pb(&d, msg, err);
    return ret;
}

int PBJson::jsonobject2pb(const rapidjson::Value* json,
                          google::protobuf::Message* msg,
                          std::string& err){
    return parse_json(json, msg, err);
}

int PBJson::parse_json(const rapidjson::Value* json,
                       google::protobuf::Message* msg,
                       std::string& err) {
    if (NULL == json || json->GetType() != rapidjson::kObjectType){
        return ERR_INVALID_ARG;
    }
    const Descriptor *d = msg->GetDescriptor();
    const Reflection *ref = msg->GetReflection();
    if (!d || !ref){
        RETURN_ERR(ERR_INVALID_PB, "invalid pb object");
    }
    for (rapidjson::Value::ConstMemberIterator itr = json->MemberBegin();
         itr != json->MemberEnd(); ++itr){
        const char* name = itr->name.GetString();
        const FieldDescriptor *field = d->FindFieldByName(name);
        if (!field)
            field = ref->FindKnownExtensionByName(name);
        if (!field)
            continue;
        // TODO: we should not fail here,
        // instead write this value into an unknown field
        if (itr->value.GetType() == rapidjson::kNullType) {
            ref->ClearField(msg, field);
            continue;
        }
        if (field->is_repeated()){
            if (itr->value.GetType() != rapidjson::kArrayType)
                RETURN_ERR(ERR_INVALID_JSON, "Not array");
            for (rapidjson::Value::ConstValueIterator ait = itr->value.Begin();
                 ait != itr->value.End(); ++ait){
                int ret = json2field(ait, msg, field, err);
                if (ret != 0){
                    return ret;
                }
            }
        }
        else{
            int ret = json2field(&(itr->value), msg, field, err);
            if (ret != 0){
                return ret;
            }
        }
    }
    return 0;
}

int PBJson::json2field(const rapidjson::Value* json,
                       google::protobuf::Message* msg,
                       const google::protobuf::FieldDescriptor *field,
                       std::string& err){
    const Reflection *ref = msg->GetReflection();
    const bool repeated = field->is_repeated();
    switch (field->cpp_type()){
    case FieldDescriptor::CPPTYPE_INT32:{
        if (json->GetType() != rapidjson::kNumberType){
            RETURN_ERR(ERR_INVALID_JSON, "Not a number");
        }
        if (repeated){
            ref->AddInt32(msg, field, (int32_t) json->GetInt());
        }
        else{
            ref->SetInt32(msg, field, (int32_t) json->GetInt());
        }
        break;
    }
    case FieldDescriptor::CPPTYPE_UINT32:{
        if (json->GetType() != rapidjson::kNumberType){
            RETURN_ERR(ERR_INVALID_JSON, "Not a number");
        }
        if (repeated){
            ref->AddUInt32(msg, field, json->GetUint());
        }
        else{
            ref->SetUInt32(msg, field, json->GetUint());
        }
        break;
    }
    case FieldDescriptor::CPPTYPE_INT64:{
        if (json->GetType() != rapidjson::kNumberType){
            RETURN_ERR(ERR_INVALID_JSON, "Not a number");
        }
        if (repeated){
            ref->AddInt64(msg, field, json->GetInt64());
        }
        else{
            ref->SetInt64(msg, field, json->GetInt64());
        }
        break;
    }
    case FieldDescriptor::CPPTYPE_UINT64:{
        if (json->GetType() != rapidjson::kNumberType){
            RETURN_ERR(ERR_INVALID_JSON, "Not a number");
        }
        if (repeated){
            ref->AddUInt64(msg, field, json->GetUint64());
        }
        else{
            ref->SetUInt64(msg, field, json->GetUint64());
        }
        break;
    }
    case FieldDescriptor::CPPTYPE_DOUBLE:{
        if (json->GetType() != rapidjson::kNumberType){
            RETURN_ERR(ERR_INVALID_JSON, "Not a number");
        }
        if (repeated){
            ref->AddDouble(msg, field, json->GetDouble());
        }
        else{
            ref->SetDouble(msg, field, json->GetDouble());
        }
        break;
    }
    case FieldDescriptor::CPPTYPE_FLOAT:{
        if (json->GetType() != rapidjson::kNumberType){
            RETURN_ERR(ERR_INVALID_JSON, "Not a number");
        }
        if (repeated){
            ref->AddFloat(msg, field, json->GetDouble());
        }
        else{
            ref->SetFloat(msg, field, json->GetDouble());
        }
        break;
    }
    case FieldDescriptor::CPPTYPE_BOOL:{
        if (json->GetType() != rapidjson::kTrueType &&
            json->GetType() != rapidjson::kFalseType){
            RETURN_ERR(ERR_INVALID_JSON, "Not a bool");
        }
        bool v = json->GetBool();
        if (repeated){
            ref->AddBool(msg, field, v);
        }
        else{
            ref->SetBool(msg, field, v);
        }
        break;
    }
    case FieldDescriptor::CPPTYPE_STRING:{
        if (json->GetType() != rapidjson::kStringType){
            RETURN_ERR(ERR_INVALID_JSON, "Not a string");
        }
        const char* value = json->GetString();
        uint32_t str_size = json->GetStringLength();
        std::string str_value(value, str_size);
        if (field->type() == FieldDescriptor::TYPE_BYTES){
            if (repeated){
                ref->AddString(msg, field, b64_decode(str_value));
            }
            else{
                ref->SetString(msg, field, b64_decode(str_value));
            }
        }
        else{
            if (repeated){
                ref->AddString(msg, field, str_value);
            }
            else{
                ref->SetString(msg, field, str_value);
            }
        }
        break;
    }
    case FieldDescriptor::CPPTYPE_MESSAGE:{
        Message *mf = (repeated) ? ref->AddMessage(msg, field) :
                      ref->MutableMessage(msg, field);
        return parse_json(json, mf, err);
    }
    case FieldDescriptor::CPPTYPE_ENUM:{
        const EnumDescriptor *ed = field->enum_type();
        const EnumValueDescriptor *ev = 0;
        if (json->GetType() == rapidjson::kNumberType){
            ev = ed->FindValueByNumber(json->GetInt());
        }
        else if (json->GetType() == rapidjson::kStringType){
            ev = ed->FindValueByName(json->GetString());
        }
        else
            RETURN_ERR(ERR_INVALID_JSON, "Not an integer or string");
        if (!ev)
            RETURN_ERR(ERR_INVALID_JSON, "Enum value not found");
        if (repeated){
            ref->AddEnum(msg, field, ev);
        }
        else{
            ref->SetEnum(msg, field, ev);
        }
        break;
    }
    default:
        break;
    }
    return 0;
}

void PBJson::pb2json(const Message* msg, std::string& str){
    rapidjson::Value::AllocatorType allocator;
    rapidjson::Value* json = parse_msg(msg, allocator);
    json2string(json, str);
    delete json;
}

void PBJson::json2string(const rapidjson::Value* json, std::string& str){
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    json->Accept(writer);
    str.append(buffer.GetString(), buffer.GetSize());
}

rapidjson::Value* PBJson::parse_msg(const Message *msg,
                                    rapidjson::Value::AllocatorType& allocator)
{
    const Descriptor *d = msg->GetDescriptor();
    if (!d)
        return NULL;
    size_t count = d->field_count();
    rapidjson::Value* root = new rapidjson::Value(rapidjson::kObjectType);
    if (!root)
        return NULL;
    for (size_t i = 0; i != count; ++i){
        const FieldDescriptor *field = d->field(i);
        if (!field){
            delete root;
            return NULL;
        }

        const Reflection *ref = msg->GetReflection();
        if (!ref){
            delete root;
            return NULL;
        }
        if (field->is_optional() && !ref->HasField(*msg, field)){
            //do nothing
        }
        else{
            rapidjson::Value* field_json = field2json(msg, field, allocator);
            rapidjson::Value field_name(field->name().c_str(), field->name().size());
            root->AddMember(field_name, *field_json, allocator);
            delete field_json;
        }
    }
    return root;
}

rapidjson::Value* PBJson::field2json(
        const Message *msg, const FieldDescriptor *field,
        rapidjson::Value::AllocatorType& allocator) {
    const Reflection *ref = msg->GetReflection();
    const bool repeated = field->is_repeated();

    size_t array_size = 0;
    if (repeated){
        array_size = ref->FieldSize(*msg, field);
    }
    rapidjson::Value* json = NULL;
    if (repeated){
        json = new rapidjson::Value(rapidjson::kArrayType);
    }
    switch (field->cpp_type()){
    case FieldDescriptor::CPPTYPE_DOUBLE:
        if (repeated){
            for (size_t i = 0; i != array_size; ++i){
                double value = ref->GetRepeatedDouble(*msg, field, i);
                rapidjson::Value v(value);
                json->PushBack(v, allocator);
            }
        }
        else{
            json = new rapidjson::Value(ref->GetDouble(*msg, field));
        }
        break;
    case FieldDescriptor::CPPTYPE_FLOAT:
        if (repeated){
            for (size_t i = 0; i != array_size; ++i){
                float value = ref->GetRepeatedFloat(*msg, field, i);
                rapidjson::Value v(value);
                json->PushBack(v, allocator);
            }
        }
        else{
            json = new rapidjson::Value(ref->GetFloat(*msg, field));
        }
        break;
    case FieldDescriptor::CPPTYPE_INT64:
        if (repeated){
            for (size_t i = 0; i != array_size; ++i){
                int64_t value = ref->GetRepeatedInt64(*msg, field, i);
                rapidjson::Value v(value);
                json->PushBack(v, allocator);
            }
        }
        else{
            json = new rapidjson::Value(
                    static_cast<int64_t>(ref->GetInt64(*msg, field)));
        }
        break;
    case FieldDescriptor::CPPTYPE_UINT64:
        if (repeated){
            for (size_t i = 0; i != array_size; ++i){
                uint64_t value = ref->GetRepeatedUInt64(*msg, field, i);
                rapidjson::Value v(value);
                json->PushBack(v, allocator);
            }
        }
        else{
            json = new rapidjson::Value(
                    static_cast<uint64_t>(ref->GetUInt64(*msg, field)));
        }
        break;
    case FieldDescriptor::CPPTYPE_INT32:
        if (repeated){
            for (size_t i = 0; i != array_size; ++i){
                int32_t value = ref->GetRepeatedInt32(*msg, field, i);
                rapidjson::Value v(value);
                json->PushBack(v, allocator);
            }
        }
        else{
            json = new rapidjson::Value(ref->GetInt32(*msg, field));
        }
        break;
    case FieldDescriptor::CPPTYPE_UINT32:
        if (repeated){
            for (size_t i = 0; i != array_size; ++i){
                uint32_t value = ref->GetRepeatedUInt32(*msg, field, i);
                rapidjson::Value v(value);
                json->PushBack(v, allocator);
            }
        }
        else{
            json = new rapidjson::Value(ref->GetUInt32(*msg, field));
        }
        break;
    case FieldDescriptor::CPPTYPE_BOOL:
        if (repeated){
            for (size_t i = 0; i != array_size; ++i){
                bool value = ref->GetRepeatedBool(*msg, field, i);
                rapidjson::Value v(value);
                json->PushBack(v, allocator);
            }
        }
        else{
            json = new rapidjson::Value(ref->GetBool(*msg, field));
        }
        break;
    case FieldDescriptor::CPPTYPE_STRING:{
        bool is_binary = field->type() == FieldDescriptor::TYPE_BYTES;
        if (repeated){
            for (size_t i = 0; i != array_size; ++i){
                std::string value = ref->GetRepeatedString(*msg, field, i);
                if (is_binary){
                    value = b64_encode(value);
                }
                rapidjson::Value v(value.c_str(),
                        static_cast<rapidjson::SizeType>(value.size()), allocator);
                json->PushBack(v, allocator);
            }
        }
        else{
            std::string value = ref->GetString(*msg, field);
            if (is_binary){
                value = b64_encode(value);
            }
            json = new rapidjson::Value(value.c_str(), value.size(), allocator);
        }
        break;
    }
    case FieldDescriptor::CPPTYPE_MESSAGE:
        if (repeated){
            for (size_t i = 0; i != array_size; ++i){
                const Message *value = &(ref->GetRepeatedMessage(*msg, field, i));
                rapidjson::Value* v = parse_msg(value, allocator);
                json->PushBack(*v, allocator);
                delete v;
            }
        }
        else{
            const Message *value = &(ref->GetMessage(*msg, field));
            json = parse_msg(value, allocator);
        }
        break;
    case FieldDescriptor::CPPTYPE_ENUM:
        if (repeated){
            for (size_t i = 0; i != array_size; ++i){
                const EnumValueDescriptor* value =
                    ref->GetRepeatedEnum(*msg, field, i);
                rapidjson::Value v(value->number());
                json->PushBack(v, allocator);
            }
        }
        else{
            json = new rapidjson::Value(ref->GetEnum(*msg, field)->number());
        }
        break;
    default:
        break;
    }
    return json;
}

std::string PBJson::hex2bin(const std::string &s) {
    if (s.size() % 2)
        throw std::runtime_error("Odd hex data size");
    static const char lookup[] = ""
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0x00
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0x10
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0x20
        "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x80\x80\x80\x80\x80\x80" // 0x30
        "\x80\x0a\x0b\x0c\x0d\x0e\x0f\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0x40
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0x50
        "\x80\x0a\x0b\x0c\x0d\x0e\x0f\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0x60
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0x70
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0x80
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0x90
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0xa0
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0xb0
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0xc0
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0xd0
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0xe0
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0xf0
        "";
    std::string r;
    r.reserve(s.size() / 2);
    for (size_t i = 0; i < s.size(); i += 2) {
        char hi = lookup[static_cast<unsigned char>(s[i])];
        char lo = lookup[static_cast<unsigned char>(s[i+1])];
        if (0x80 & (hi | lo))
            throw std::runtime_error("Invalid hex data: " + s.substr(i, 6));
        r.push_back((hi << 4) | lo);
    }
    return r;
}

std::string PBJson::bin2hex(const std::string &s){
    static const char lookup[] = "0123456789abcdef";
    std::string r;
    r.reserve(s.size() * 2);
    for (size_t i = 0; i < s.size(); i++) {
        unsigned char hi = s[i] >> 4;
        unsigned char lo = s[i] & 0xf;
        r.push_back(lookup[hi]);
        r.push_back(lookup[lo]);
    }
    return r;
}

std::string PBJson::b64_encode(const std::string &s){
    typedef unsigned char u1;
    static const char lookup[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    const u1 * data = (const u1 *) s.c_str();
    std::string r;
    r.reserve(s.size() * 4 / 3 + 3);
    for (size_t i = 0; i < s.size(); i += 3) {
        unsigned n = data[i] << 16;
        if (i + 1 < s.size()) n |= data[i + 1] << 8;
        if (i + 2 < s.size()) n |= data[i + 2];

        u1 n0 = (u1)(n >> 18) & 0x3f;
        u1 n1 = (u1)(n >> 12) & 0x3f;
        u1 n2 = (u1)(n >>  6) & 0x3f;
        u1 n3 = (u1)(n      ) & 0x3f;

        r.push_back(lookup[n0]);
        r.push_back(lookup[n1]);
        if (i + 1 < s.size()) r.push_back(lookup[n2]);
        if (i + 2 < s.size()) r.push_back(lookup[n3]);
    }
    for (size_t i = 0; i < (3 - s.size() % 3) % 3; i++)
        r.push_back('=');
    return r;
}

std::string PBJson::b64_decode(const std::string &s){
    typedef unsigned char u1;
    static const char lookup[] = ""
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0x00
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0x10
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x3e\x80\x80\x80\x3f" // 0x20
        "\x34\x35\x36\x37\x38\x39\x3a\x3b\x3c\x3d\x80\x80\x80\x00\x80\x80" // 0x30
        "\x80\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e" // 0x40
        "\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x80\x80\x80\x80\x80" // 0x50
        "\x80\x1a\x1b\x1c\x1d\x1e\x1f\x20\x21\x22\x23\x24\x25\x26\x27\x28" // 0x60
        "\x29\x2a\x2b\x2c\x2d\x2e\x2f\x30\x31\x32\x33\x80\x80\x80\x80\x80" // 0x70
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0x80
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0x90
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0xa0
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0xb0
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0xc0
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0xd0
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0xe0
        "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80" // 0xf0
        "";
    std::string r;
    if (!s.size()) return r;
    if (s.size() % 4)
        throw std::runtime_error("Invalid base64 data size");
    size_t pad = 0;
    if (s[s.size() - 1] == '=') pad++;
    if (s[s.size() - 2] == '=') pad++;

    r.reserve(s.size() * 3 / 4 + 3);
    for (size_t i = 0; i < s.size(); i += 4) {
        u1 n0 = lookup[(u1) s[i+0]];
        u1 n1 = lookup[(u1) s[i+1]];
        u1 n2 = lookup[(u1) s[i+2]];
        u1 n3 = lookup[(u1) s[i+3]];
        if (0x80 & (n0 | n1 | n2 | n3))
            throw std::runtime_error("Invalid hex data: " + s.substr(i, 4));
        unsigned n = (n0 << 18) | (n1 << 12) | (n2 << 6) | n3;
        r.push_back((n >> 16) & 0xff);
        if (s[i+2] != '=') r.push_back((n >> 8) & 0xff);
        if (s[i+3] != '=') r.push_back((n     ) & 0xff);
    }
    return r;
}

END_HIPPO_NAMESPACE(http_client);

