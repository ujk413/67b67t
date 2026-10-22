Signal protocol v3 interoperability uses org.whispersystems:signal-protocol-java:2.8.1.
Its original generated protobuf classes do not work with current protobuf runtimes.
The Gradle task strips those three classes (including their nested classes) and uses
the matching schemas regenerated with protoc 3.25.9 and protobuf-javalite 3.25.9.
No protocol or cryptographic primitive has been changed.

Schemas and license: https://github.com/signalapp/libsignal-protocol-java (master, version 2.8.1).
Compiler: com.google.protobuf:protoc:3.25.9.
Regenerate: protoc --proto_path=android/src/main/proto --java_out=lite:android/src/main/java android/src/main/proto/*.proto
The native Java/web interoperability fixture must pass after dependency updates.
