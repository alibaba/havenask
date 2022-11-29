//config.cpp
#include "aitheta_indexer/__signature.h"
#include <string>
namespace heavenask { namespace indexlib {
static const std::string nsg_version_info = "signature_indexlib_version: " __PLUGIN_VERSION;
static const std::string nsg_git_info = "signature_indexlib_commit: " __PLUGIN_COMMIT_INFO;
static const std::string nsg_build_info = "signature_indexlib_build: " __PLUGIN_BUILD_INFO;
}}
