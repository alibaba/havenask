#ifndef ISEARCH_BS_DOCUMENTDEFINE_H
#define ISEARCH_BS_DOCUMENTDEFINE_H

#include <indexlib/document/raw_document/raw_document_define.h>

namespace build_service {
namespace document {

static const std::string CMD_SEPARATOR              = "\x1E\n";
static const std::string KEY_VALUE_SEPARATOR        = "\x1F\n";

static const char SUB_DOC_SEPARATOR        = '\x03';
static const char TOKEN_PAYLOAD_SYMBOL       = '\x1C';
static const char PAYLOAD_FIELD_SYMBOL       = '\x10';
static const char KEY_VALUE_EQUAL_SYMBOL     = '=';
static const size_t MAX_DOC_LENGTH           = 1024 * 1024;
static const size_t KEY_VALUE_SEPARATOR_LENGTH = KEY_VALUE_SEPARATOR.length();
static const size_t KEY_VALUE_EQUAL_SYMBOL_LENGTH = sizeof(char);

static const std::string CMD_TAG = IE_NAMESPACE(document)::CMD_TAG;
static const std::string CMD_ADD = IE_NAMESPACE(document)::CMD_ADD;
static const std::string CMD_DELETE = IE_NAMESPACE(document)::CMD_DELETE;
static const std::string CMD_DELETE_SUB = IE_NAMESPACE(document)::CMD_DELETE_SUB;
static const std::string CMD_UPDATE_FIELD = IE_NAMESPACE(document)::CMD_UPDATE_FIELD;
static const std::string CMD_SKIP = IE_NAMESPACE(document)::CMD_SKIP;

static const std::string HA_RESERVED_MODIFY_FIELDS = "ha_reserved_modify_fields";
static const std::string HA_RESERVED_SUB_MODIFY_FIELDS = "ha_reserved_sub_modify_fields";
static const std::string HA_RESERVED_MODIFY_FIELDS_SEPARATOR = ";";

static inline void __NO_USED_FUNC__() {
    (void)KEY_VALUE_SEPARATOR_LENGTH;
}

}
}

#endif //ISEARCH_BS_DOCUMENTDEFINE_H
