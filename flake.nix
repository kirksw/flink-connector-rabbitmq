# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

{
  description = "Local end-to-end tests for the Flink RabbitMQ connector";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

  outputs = { nixpkgs, ... }:
    let
      forSystems = nixpkgs.lib.genAttrs [ "aarch64-darwin" "x86_64-linux" ];
      environments = forSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
          flink = version: hash: pkgs.stdenvNoCC.mkDerivation {
            pname = "apache-flink";
            inherit version;
            src = pkgs.fetchurl {
              urls = map (base: "${base}/flink-${version}/flink-${version}-bin-scala_2.12.tgz") [
                "https://dlcdn.apache.org/flink"
                "https://archive.apache.org/dist/flink"
              ];
              inherit hash;
            };
            dontUnpack = true;
            dontBuild = true;
            dontFixup = true;
            installPhase = ''
              mkdir -p "$out"
              # Extract only what the standalone runner needs, without a second copy in the build directory.
              tar -xzf "$src" -C "$out" --strip-components=1 \
                flink-${version}/bin flink-${version}/conf flink-${version}/lib
              patchShebangs --build "$out/bin"
            '';
            nativeBuildInputs = [ pkgs.bash ];
          };
          flink120 = flink "1.20.5"
            "sha512-yEbsvMShcFckgyrNutJ1OMMfF6pJTotp/1+S1yRXS+g9Hf8LNjqgVw5aRI9SkiRwXZZ9DRTfMmcUPuxQZKUqHw==";
          flink23 = flink "2.3.0"
            "sha512-5YY3Z8rqp8cuRfxi1F99+fQ1ocg67YE+pVDbOekiEZTRSOpKbDv7VgQzWXRynFedSMbEw+tDUC43MQoL+YJGKg==";
          python = pkgs.python3.withPackages (p: [ p.pika ]);
          tools = [
            pkgs.bash pkgs.coreutils pkgs.gawk pkgs.gnugrep pkgs.gnused pkgs.jdk17
            (pkgs.maven.override { jdk_headless = pkgs.jdk17; })
            pkgs.beam27Packages.erlang pkgs.rabbitmq-server python
          ];
          runner = pkgs.writeShellApplication {
            name = "rabbitmq-e2e";
            runtimeInputs = tools;
            text = ''
              export JAVA_HOME=${pkgs.jdk17.home}
              export E2E_FLINK_1_20=${flink120} E2E_FLINK_2_3=${flink23}
              export E2E_RABBITMQ=${pkgs.rabbitmq-server}/sbin/rabbitmq-server
              exec ${python}/bin/python3 ${./e2e/run.py} "$@"
            '';
          };
        in {
          app = { type = "app"; program = "${runner}/bin/rabbitmq-e2e"; };
          shell = pkgs.mkShell {
            packages = tools ++ [ runner ];
            JAVA_HOME = pkgs.jdk17.home;
          };
        });
    in {
      apps = forSystems (system: { e2e = environments.${system}.app; });
      devShells = forSystems (system: { default = environments.${system}.shell; });
    };
}
