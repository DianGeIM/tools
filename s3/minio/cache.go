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

package minio

import "context"

type ImageInfo struct {
	IsImg  bool   `json:"isImg"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
	Format string `json:"format"`
	Etag   string `json:"etag"`
}

type Cache interface {
	GetImageObjectKeyInfo(ctx context.Context, key string, fn func(ctx context.Context) (*ImageInfo, error)) (*ImageInfo, error)
	GetThumbnailKey(ctx context.Context, key string, format string, width int, height int, minioCache func(ctx context.Context) (string, error)) (string, error)
	DelObjectImageInfoKey(ctx context.Context, keys ...string) error
	DelImageThumbnailKey(ctx context.Context, key string, format string, width int, height int) error
}
