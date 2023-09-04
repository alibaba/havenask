load("@rules_jvm_external//:defs.bzl", "maven_install")
load("@rules_jvm_external//:specs.bzl", "maven")

SPRING_BOOT_MODULES = [
    "starter-web",
    "starter-log4j2",
    "starter-jdbc",
    "starter-tomcat",
    "starter-test",
    "loader",
]

repositories = [
    "https://maven.aliyun.com/repository/central/",
    "http://mvnonline.alibaba-inc.com/nexus/content/repositories/central",
    "http://mvnonline.alibaba-inc.com/nexus/content/repositories/releases",
    "http://mvnonline.alibaba-inc.com/nexus/content/repositories/snapshots",
    "http://mvnonline.alibaba-inc.com/nexus/content/repositories/thirdparty",
]

def iquan_init(name = "iquan_maven"):
    maven_install(
        name = name,
        artifacts = [
            "org.apache.flink:flink-table-uber-blink_2.11:iquan-0.1.24-SNAPSHOT",
            "org.apache.flink:flink-shaded-guava:18.0-12.0",
            "org.apache.flink:flink-shaded-asm-7:7.1-12.0",
            "org.scala-lang:scala-library:2.11.12",
            "org.apache.commons:commons-lang3:3.8.1",
            "org.slf4j:slf4j-api:1.7.21",
            "org.apache.logging.log4j:log4j-api:2.11.2",
            "org.apache.logging.log4j:log4j-core:2.11.2",
            "org.apache.logging.log4j:log4j-slf4j-impl:2.11.2",
            "org.mybatis.spring.boot:mybatis-spring-boot-starter:2.0.0",
            "com.lmax:disruptor:3.4.2",
            "com.google.guava:guava:23.0",
            "commons-cli:commons-cli:1.4",
            "com.google.flatbuffers:flatbuffers-java:1.9.0",
            "com.google.protobuf:protobuf-java:3.8.0-rc-1",
            "com.google.protobuf:protobuf-java-util:3.8.0-rc-1",
            "com.taobao.monitor:kmonitor-client:1.1.28",
            "com.alibaba:fastjson:1.2.77_preview_03_noneautotype",
            "org.xerial:sqlite-jdbc:3.25.2",
            "org.apache.httpcomponents:httpclient:4.5.10",
            "com.fasterxml.jackson.core:jackson-core:2.9.8",
            "com.fasterxml.jackson.core:jackson-databind:2.9.8",
            "commons-logging:commons-logging:1.2",
            "net.sf.py4j:py4j:0.10.9.1",
            "org.junit.platform:junit-platform-console:1.7.0",
            "org.junit.jupiter:junit-jupiter-params:5.7.0",
            "org.junit.jupiter:junit-jupiter-api:5.7.0",
            "org.junit.jupiter:junit-jupiter-engine:5.7.0",
            "org.opentest4j:opentest4j:1.0.0",
            "org.apache.calcite:calcite-core:1.33.0",
            "com.fasterxml.jackson.core:jackson-core:2.11.2",
            "com.fasterxml.jackson.core:jackson-databind:2.11.2",
            "com.fasterxml.jackson.core:jackson-annotations:2.11.2",
            "org.javatuples:javatuples:1.2",
            "org.immutables:value:2.9.3",
            "org.projectlombok:lombok:1.18.22",

            "org.springframework:spring-aop:5.1.5.RELEASE",
            "org.springframework:spring-aspects:5.1.5.RELEASE",
            "org.springframework:spring-beans:5.1.5.RELEASE",
            "org.springframework:spring-context:5.1.5.RELEASE",
            "org.springframework:spring-context-support:5.1.5.RELEASE",
            "org.springframework:spring-core:5.1.5.RELEASE",
            "org.springframework:spring-expression:5.1.5.RELEASE",
            "org.springframework:spring-jdbc:5.1.5.RELEASE",
            "org.springframework:spring-test:5.1.5.RELEASE",
            "org.springframework:spring-tx:5.1.5.RELEASE",
            "org.springframework:spring-web:5.1.5.RELEASE",
            "org.springframework:spring-webmvc:5.1.5.RELEASE",
            "org.springframework.boot:spring-boot:2.1.3.RELEASE",
        ] + [
            "org.springframework.boot:spring-boot-%s:2.1.3.RELEASE" % module for module in SPRING_BOOT_MODULES
        ],
        repositories = repositories,
        excluded_artifacts = [
            "org.springframework.boot:spring-boot-starter-logging",
        ],
        strict_visibility = True,
    )
