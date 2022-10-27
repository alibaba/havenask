load("@protobuf//:protobuf.bzl", "cc_proto_library")
load("//:build_service.bzl", 'genlex')

genlex (
    name = "lexer",
    srcs = [
        "analyzer/SingleWSScanner.ll",
    ],
    out = "SingleWSScanner.cc",
)

cc_proto_library(
    name = "build_service_cc_proto",
    srcs = glob([
        "proto/*.proto",
        "admin/*proto",
    ]),
    default_runtime = "@protobuf//:protobuf",
    cc_libs = ["@protobuf//:protobuf"],
    protoc = "@protobuf//:protoc",
    deps = [
        "@arpc_archive//:arpc_cc_proto",
        "@protobuf//:cc_wkt_protos",
        "@hippo//hippo:hippo_cc_proto",        
    ],
    visibility = ["//visibility:public"],    
)

cc_library(
    name = "build_service",
    srcs = glob([
        "util/*.cpp",
        "config/*.cpp",
        "common/*.cpp",
        "plugin/*.cpp",
        "document/*.cpp",
        "reader/*.cpp",
        "analyzer/*.cpp",
        "workflow/*.cpp",
        "processor/*.cpp",
        "builder/*.cpp",
        "proto/*.cpp",
        "task_base/*.cpp",
        "*.h",
        "util/*.h",
        "config/*.h",
        "common/*.h",
        "plugin/*.h",
        "document/*.h",
        "reader/*.h",
        "analyzer/*.h",                
        "workflow/*.h",
        "processor/*.h",
        "builder/*.h",
        "proto/*.h",
        "task_base/*.h",        
    ]) + [":lexer"],
    hdrs = glob([
        # "*.h",
        # "util/*.h",
        # "config/*.h",
        # "common/*.h",
        # "plugin/*.h",
        # "document/*.h",
        # "reader/*.h",
        # "analyzer/*.h",                
        # "workflow/*.h",
        # "processor/*.h",
        # "builder/*.h",
        # "proto/*.h",
        # "task_base/*.h",
        
        'workflow/RealtimeErrorDefine.h',
        'workflow/RealtimeBuilder.h',
        'workflow/PrepareIndex.h',
        
        'document/ClassifiedDocument.h',
        'document/RawDocument.h',
        'document/DocumentDefine.h',
        'document/TokenizeDocument.h',
        'document/ExtendDocument.h',
        'document/TokenizeField.h',
        'document/ProcessedDocument.h',
        'document/TokenizeSection.h',
        'document/RawDocumentHashMap.h',
        'document/RawDocumentHashMapManager.h',
        
        'reader/RawDocumentReader.h',
        'reader/ReaderModuleFactory.h',
        'reader/SwiftClientCreator.h',
        
        'processor/DocumentProcessorFactory.h',
        'processor/DocumentProcessor.h',
        'processor/TokenizeDocumentProcessor.h', # for plugin test
        
        'analyzer/Token.h',
        'analyzer/Tokenizer.h',
        'analyzer/SimpleTokenizer.h',
        'analyzer/TokenizerModuleFactory.h',
        'analyzer/BuildInTokenizerFactory.h',
        'analyzer/TokenizerConfig.h',
        'analyzer/TokenizerManager.h',
        'analyzer/AnalyzerFactory.h',
        'analyzer/Analyzer.h',
        'analyzer/AnalyzerInfos.h',
        'analyzer/AnalyzerInfo.h',
        'analyzer/AnalyzerDefine.h',
        'analyzer/TraditionalTables.h',
        'analyzer/Normalizer.h',
        "analyzer/NormalizeTable.h",
        'analyzer/EncodeConverter.h',
        
        'plugin/PooledObject.h',
        'plugin/ModuleInfo.h',
        'plugin/Module.h',
        'plugin/DllWrapper.h',
        'plugin/ModuleFactory.h',
        'plugin/PlugInManager.h',
        
        'config/ResourceReader.h',
        'config/GenerationMeta.h',
        
        'common/Locator.h',
        'common/End2EndLatencyReporter.h',
        
        'util/Log.h',
        'util/Monitor.h',
        'util/RangeUtil.h',
        'util/FileUtil.h',
        'util/LogSetupGuard.h',
        
        'proto/ErrorCollector.h',
        
        'common_define.h',
    ]),
    include_prefix = "build_service",
    copts = ["-std=c++11", "-Wno-sign-compare", "-fpic", "-FPIC", "-O2"],    
    deps = [
        "//build_service:build_service_cc_proto",
        "@autil_archive//autil:autil",
        "@alog_archive//:alog",
        "@arpc_archive//:arpc",
        "@amonitor//amonitor/client:amonitor_client",
        "@fslib_archive//fslib:fslib-framework",
        "@indexlib//indexlib:indexlib",
        "@swift//swift:swift",
        "@swift//swift/client:swift_client_minimal",
        "@swift//swift/protocol:swift_protocol",
        "@http_arpc//http_arpc:http_arpc",
        "@worker_framework//src:worker_framework",
    ],
    visibility = ["//visibility:public"]
)


