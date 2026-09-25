#!/usr/bin/env bash
# Resolve Maven coordinates (with transitive dependencies) using spark-submit's Ivy
# integration and copy the resulting JARs into $SPARK_HOME/jars, skipping any
# artifact Spark already bundles so the classpath never holds two versions.
set -euo pipefail

packages="$1"
ivy_dir="$(mktemp -d)"
noop="$(mktemp --suffix=.py)"
maven="https://repo1.maven.org/maven2"

# Artifacts whose classes Spark already ships inside a differently named JAR.
superseded=("shims")  # RoaringBitmap >= 1.0 bundles org.roaringbitmap.*Shim

# spark-submit resolves --packages before running the application; the no-op
# script never creates a SparkContext, so no cluster is needed at build time.
"${SPARK_HOME}/bin/spark-submit" \
  --master "local[1]" \
  --conf "spark.jars.ivy=${ivy_dir}" \
  --packages "${packages}" \
  "${noop}"

jackson_version="$(basename "$(compgen -G "${SPARK_HOME}/jars/jackson-databind-*.jar" | head -1)" .jar)"
jackson_version="${jackson_version#jackson-databind-}"

added=0
for jar in "${ivy_dir}"/jars/*.jar; do
  file="$(basename "${jar}")"            # e.g. org.apache.kafka_kafka-clients-3.9.1.jar
  group="${file%%_*}"                     # org.apache.kafka
  artifact="${file#*_}"                   # kafka-clients-3.9.1.jar
  name="$(sed -E 's/-[0-9][^-]*(-[A-Za-z0-9.]+)?\.jar$//' <<<"${artifact}")"  # kafka-clients

  if [[ " ${superseded[*]} " == *" ${name} "* ]]; then
    echo "skip ${artifact} (superseded by a Spark-bundled JAR)"
    continue
  fi
  if existing="$(compgen -G "${SPARK_HOME}/jars/${name}-[0-9]*.jar")"; then
    echo "skip ${artifact} (Spark ships $(basename "$(head -1 <<<"${existing}")"))"
    continue
  fi
  if [[ "${group}" == com.fasterxml.jackson.* ]]; then
    # Jackson modules must match Spark's jackson-databind version exactly.
    target="${name}-${jackson_version}.jar"
    python3 -c "import sys, urllib.request; urllib.request.urlretrieve(sys.argv[1], sys.argv[2])" \
      "${maven}/${group//.//}/${name}/${jackson_version}/${target}" "${SPARK_HOME}/jars/${target}"
    echo "add  ${target} (aligned with Spark's Jackson ${jackson_version}; Ivy resolved ${artifact})"
  else
    cp "${jar}" "${SPARK_HOME}/jars/${artifact}"
    echo "add  ${artifact}"
  fi
  added=$((added + 1))
done

rm -rf "${ivy_dir}" "${noop}"
echo "installed ${added} connector jars into ${SPARK_HOME}/jars"
