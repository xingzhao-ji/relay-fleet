# Third-party notices

The included Gradle wrapper JAR/scripts are Gradle components. Their existing [wrapper license](third_party/GRADLE_WRAPPER_LICENSE.txt), extracted unchanged from the JAR, and [Gradle 8.14.4 notice](third_party/GRADLE_NOTICE.txt) accompany them. The wrapper downloads a SHA-256-pinned Gradle distribution.

Generated Python protobuf/gRPC clients derive from the project's `proto/fleet.proto`; the corresponding generator/runtime dependencies are separately installed, not vendored. Java libraries and Python dependencies retain their own distribution terms. No license for this project's original source is implied or newly assigned here.
