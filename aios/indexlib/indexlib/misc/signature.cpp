//signature.cpp
#include "indexlib/misc/signature.h"
#include <string>
namespace heavenask { namespace indexlib {
static const std::string indexlib_version_info = "signature_indexlib_version: " __INDEXLIB_VERSION;
static const std::string indexlib_git_info = "signature_indexlib_commit: " __INDEXLIB_COMMIT_INFO;
static const std::string indexlib_build_info = "signature_indexlib_build: " __INDEXLIB_BUILD_INFO;
}}
