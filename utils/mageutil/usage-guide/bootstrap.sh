#!/bin/bash
# Copyright © 2024 OpenIM open source community. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


if [[ ":$PATH:" == *":$HOME/.local/bin:"* ]]; then
    TARGET_DIR="$HOME/.local/bin"
else
    TARGET_DIR="/usr/local/bin"
    echo "Using /usr/local/bin as the installation directory. Might require sudo permissions."
fi

if ! command -v mage &> /dev/null; then
    echo "Installing Mage to $TARGET_DIR ..."
    GOBIN=$TARGET_DIR go install github.com/magefile/mage@latest
fi

if ! command -v mage &> /dev/null; then
    echo "Mage installation failed."
    echo "Please ensure that $TARGET_DIR is in your \$PATH."
    exit 1
fi

echo "Mage installed successfully."

go mod download
