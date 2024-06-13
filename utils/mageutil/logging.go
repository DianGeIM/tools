// Copyright © 2024 OpenIM open source community. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mageutil

import (
	"fmt"
	"os"
	"time"
)

const (
	ColorBlue  = "\033[0;34m"
	ColorGreen = "\033[0;32m"
	ColorRed   = "\033[0;31m"
	ColorReset = "\033[0m"
)

func PrintBlueTwoLine(message string) {
	currentTime := time.Now().Format("[2006-01-02 15:04:05 MST]")
	fmt.Println(currentTime)
	fmt.Printf("%s%s%s\n", ColorBlue, message, ColorReset)
}

func PrintBlue(message string) {
	currentTime := time.Now().Format("[2006-01-02 15:04:05 MST]")
	fmt.Printf("%s %s%s%s\n", currentTime, ColorBlue, message, ColorReset)
}

func PrintGreenTwoLine(message string) {
	currentTime := time.Now().Format("[2006-01-02 15:04:05 MST]")
	fmt.Println(currentTime)
	fmt.Printf("%s%s%s\n", ColorGreen, message, ColorReset)
}

func PrintGreen(message string) {
	currentTime := time.Now().Format("[2006-01-02 15:04:05 MST]")
	fmt.Printf("%s %s%s%s\n", currentTime, ColorGreen, message, ColorReset)
}

func PrintRed(message string) {
	currentTime := time.Now().Format("[2006-01-02 15:04:05 MST]")
	fmt.Printf("%s %s%s%s\n", currentTime, ColorRed, message, ColorReset)
}

func PrintRedNoTimeStamp(message string) {
	fmt.Printf("%s%s%s\n", ColorRed, message, ColorReset)
}

func PrintGreenNoTimeStamp(message string) {
	fmt.Printf("%s%s%s\n", ColorGreen, message, ColorReset)
}

func PrintRedToStdErr(a ...interface{}) (n int, err error) {
	return fmt.Fprint(os.Stderr, "\033[31m", fmt.Sprint(a...), "\033[0m")
}
func PrintGreenToStdOut(a ...interface{}) (n int, err error) {
	return fmt.Fprint(os.Stdout, "\033[32m", fmt.Sprint(a...), "\033[0m")
}
